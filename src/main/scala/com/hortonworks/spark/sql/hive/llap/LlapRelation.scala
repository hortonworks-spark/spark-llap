/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.spark.sql.hive.llap

import java.net.URI
import java.sql.{Connection, SQLException}
import java.util.UUID

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.llap.{LlapBaseInputFormat, LlapInputSplit, LlapRowInputFormat, Schema}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.{InputSplit, JobConf}
import org.apache.hive.service.cli.HiveSQLException

import org.apache.spark.rdd.HadoopRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType

import org.slf4j.LoggerFactory

case class LlapRelation(
    @transient sc: SQLContext,
    @transient parameters: Map[String, String])
  extends BaseRelation
  with InsertableRelation
  with PrunedFilteredScan {

  private val log = LoggerFactory.getLogger(getClass)

  override def sqlContext(): SQLContext = {
    sc
  }

  @transient val tableSchema: StructType = {
    val url = parameters("url")
    val user = parameters("user.name")
    val dbcp2Configs = parameters("dbcp2.conf")
    val conn = DefaultJDBCWrapper.getConnector(None, url, user, dbcp2Configs)
    val queryKey = getQueryType()

    try {
      if (queryKey == "table") {
        val (dbName, tableName) = getDbTableNames(parameters("table"))
        DefaultJDBCWrapper.resolveTable(conn, dbName, tableName)
      } else {
        var currentDatabase: String = null
        if (parameters.isDefinedAt("currentdatabase")) {
          currentDatabase = parameters("currentdatabase")
        }
        DefaultJDBCWrapper.resolveQuery(conn, currentDatabase, parameters("query"))
      }
    } finally
    {
       conn.close()
    }
  }

  override def schema(): StructType = {
    tableSchema
  }

  def getQueryType(): String = {
    val hasTable = parameters.isDefinedAt("table")
    val hasQuery = parameters.isDefinedAt("query")
    if (hasTable && hasQuery) {
      throw new Exception(
        "LlapRelation has both table and query parameters, can only have one or the other")
    }
    if (!hasTable && !hasQuery) {
      throw new Exception("LlapRelation requires either a table or query parameter")
    }
    val queryType = if (hasTable) {
      "table"
    } else {
      "query"
    }
    queryType
  }

  // PrunedFilteredScan implementation
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val countStar = requiredColumns.isEmpty
    val queryString = getQueryString(requiredColumns, filters)

    // If this was previously called, close any resources associated with the previous invocation
    close()

    if (countStar) {
      handleCountStar(queryString)
    } else {
      @transient val inputFormatClass = classOf[LlapRowInputFormat]
      @transient val jobConf = new JobConf(sc.sparkContext.hadoopConfiguration)
      jobConf.set("hive.llap.zk.registry.user", "hive")
      // Set JDBC url/etc
      jobConf.set("llap.if.hs2.connection", parameters("url"))
      jobConf.set("llap.if.query", queryString)
      jobConf.set("llap.if.user", parameters("user.name"))
      jobConf.set("llap.if.pwd", parameters("user.password"))
      if (parameters.isDefinedAt("currentdatabase")) {
        jobConf.set("llap.if.database", parameters("currentdatabase"))
      }
      if (parameters.isDefinedAt("handleid")) {
        jobConf.set("llap.if.handleid", parameters("handleid"))
      }

      // This should be set to the number of executors
      val numPartitions = sc.sparkContext.defaultMinPartitions
      val rdd = sc.sparkContext.hadoopRDD(jobConf, inputFormatClass,
          classOf[NullWritable], classOf[org.apache.hadoop.hive.llap.Row], numPartitions)
          .asInstanceOf[HadoopRDD[NullWritable, org.apache.hadoop.hive.llap.Row]]

      // Convert from RDD into Spark Rows
      rdd.mapPartitionsWithInputSplit(LlapRelation.llapRowRddToRows, preservesPartitioning = false)
    }
  }

  // InsertableRelation implementation
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    if (getQueryType() != "table") {
      throw new Exception("Cannot insert data to a relation that is not a table")
    }

    val (dbName, tableName) = getDbTableNames(parameters("table"))

    val writer = new HiveWriter(sc)
    val conn = getConnection()
    writer.saveDataFrameToHiveTable(data, dbName, tableName, conn, overwrite)
  }

  private def getConnection(): Connection = {
    val url = parameters("url")
    val user = parameters("user.name")
    val dbcp2Configs = parameters("dbcp2.conf")
    DefaultJDBCWrapper.getConnector(None, url, user, dbcp2Configs)
  }

  private def getDbTableNames(nameStr: String): Tuple2[String, String] = {
    val nameParts = nameStr.split("\\.")
    if (nameParts.length != 2) {
      throw new IllegalArgumentException("Expected " + nameStr + " to be in the form db.table")
    }
    new Tuple2[String, String](nameParts(0), nameParts(1))
  }

  private def getQueryString(requiredColumns: Array[String], filters: Array[Filter]): String = {
    var selectCols = "count(*)"
    if (requiredColumns.length > 0) {
      selectCols = requiredColumns.mkString(",")
    }

    val baseQuery = getQueryType match {
      case "table" => "select * from " + parameters("table")
      case "query" => parameters("query")
    }
    val baseQueryAlias = "q_" + UUID.randomUUID().toString().replaceAll("[^A-Za-z0-9 ]", "")

    val whereClause = FilterPushdown.buildWhereClause(schema, filters)

    val queryString =
      s"""select $selectCols from ($baseQuery) as $baseQueryAlias $whereClause"""

    queryString
  }

  private def handleCountStar(queryString: String): RDD[Row] = {
    tryWithResource(getConnection()) { conn =>
      tryWithResource(conn.createStatement()) { stmt =>
        try {
          val rs = stmt.executeQuery(queryString)
          if (rs.next()) {
            val countStarValue = rs.getLong(1)
            sqlContext.sparkContext.parallelize(1L to countStarValue).map(_ => Row.empty)
          } else {
            throw new IllegalStateException("Failed to read count star value")
          }
        } catch {
            case e: Throwable => throw new SQLException(
              e.toString.replace("shadehive.org.apache.hive.service.cli.HiveSQLException: ", ""))
            case e: HiveSQLException => throw new HiveSQLException(e)
        }
      }
    }
  }

  private def tryWithResource[R <: AutoCloseable, T](createResource: => R)(f: R => T): T = {
    val resource = createResource
    try f.apply(resource) finally resource.close()
  }

  override def sizeInBytes(): Long = {
    if (parameters.isDefinedAt("sizeinbytes")) {
      parameters("sizeinbytes").toLong
    } else {
      super.sizeInBytes
    }
  }

  def close(): Unit = {
    if (parameters.isDefinedAt("handleid")) {
      val handleId = parameters("handleid")
      log.info("Closing handleid " + handleId)
      try {
        LlapBaseInputFormat.close(handleId)
      } catch {
        case ex: Exception => {
          log.error("Error closing " + handleId, ex)
        }
      }
    } else {
      log.info("No handleid defined - cannot close handle")
    }
  }
}

object LlapRelation {
  def llapRowRddToRows(inputSplit: InputSplit,
      iterator: Iterator[(NullWritable, org.apache.hadoop.hive.llap.Row)]): Iterator[Row] = {
    val llapInputSplit = inputSplit.asInstanceOf[LlapInputSplit]
    val schema: Schema = llapInputSplit.getSchema
    iterator.map((tuple) => {
      val row = RowConverter.llapRowToSparkRow(tuple._2, schema)
      row
    })
  }
}

class HiveWriter(sc: SQLContext) {
  def saveDataFrameToHiveTable(
      dataFrame: DataFrame,
      dbName: String,
      tabName: String,
      conn: Connection,
      overwrite: Boolean): Unit = {
    var tmpCleanupNeeded = false
    var tmpPath: String = null
    try {
      // Save data frame to HDFS. This needs to be accessible by HiveServer2
      tmpPath = createTempPathForInsert(dbName, tabName)
      dataFrame.write.orc(tmpPath)
      tmpCleanupNeeded = true

      // Run commands in HiveServer2 via JDBC:
      //   - Create temp table using the saved dataframe in HDFS
      //   - Run insert command to destination table using temp table
      var tmpTableName = "tmp_" + UUID.randomUUID().toString().replaceAll("[^A-Za-z0-9 ]", "")
      var sql =
        s"create temporary external table $tmpTableName like $dbName.$tabName " +
        s"stored as orc location '$tmpPath'"
      conn.prepareStatement(sql).execute()

      val overwriteOrInto = overwrite match {
        case true => "overwrite"
        case false => "into"
      }

      sql = s"insert $overwriteOrInto table $dbName.$tabName select * from $tmpTableName"
      conn.prepareStatement(sql).execute()
    } finally {
      if (tmpCleanupNeeded) {
        var fs = FileSystem.get(new URI(tmpPath), sc.sparkContext.hadoopConfiguration)
        fs.delete(new Path(tmpPath), true)
      }
      conn.close()
    }
  }

  def createTempPathForInsert(dbName: String, tabName: String): String = {
    val baseDir = "/tmp"
    val tmpPath = new Path(baseDir, UUID.randomUUID().toString())
    tmpPath.toString()
  }
}
