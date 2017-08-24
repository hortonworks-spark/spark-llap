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

package org.apache.spark.sql.hive.llap

import java.sql.SQLException

import com.hortonworks.spark.sql.hive.llap.DefaultJDBCWrapper
import org.apache.hadoop.conf.Configuration
import org.apache.hive.service.cli.HiveSQLException

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, NoSuchTableException}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias, View}
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.hive._
import org.apache.spark.sql.internal.SQLConf


private[sql] class LlapSessionCatalog(
    externalCatalog: LlapExternalCatalog,
    globalTempViewManager: GlobalTempViewManager,
    metastoreCatalog: HiveMetastoreCatalog,
    sparkSession: SparkSession,
    functionResourceLoader: FunctionResourceLoader,
    functionRegistry: FunctionRegistry,
    conf: SQLConf,
    parser: ParserInterface,
    hadoopConf: Configuration)
  extends HiveSessionCatalog(
      externalCatalog,
      globalTempViewManager,
      metastoreCatalog,
      functionRegistry,
      conf,
      hadoopConf,
      parser,
      functionResourceLoader) with Logging {

  override def lookupRelation(name: TableIdentifier): LogicalPlan = {
    synchronized {
      val db = formatDatabaseName(name.database.getOrElse(currentDb))
      val table = formatTableName(name.table)
      if (db == globalTempViewManager.database) {
        globalTempViewManager.get(table).map { viewDef =>
          SubqueryAlias(table, viewDef)
        }.getOrElse(throw new NoSuchTableException(db, table))
      } else if (name.database.isDefined || !tempTables.contains(table)) {
        val metadata = externalCatalog.getTable(db, table)
        val sparkSession = SparkSession.getActiveSession.get.sqlContext.sparkSession
        val sessionState = SparkSession.getActiveSession.get.sessionState
        val getConnectionUrlMethod = sessionState.getClass.
          getMethod("getConnectionUrl", classOf[SparkSession])
        val connectionUrl = getConnectionUrlMethod.invoke(sessionState, sparkSession).toString()

        val tableMeta = sessionState.catalog.getTableMetadata(TableIdentifier(table, Some(db)))
        val sizeInBytes = tableMeta.stats.map(_.sizeInBytes.toLong).getOrElse(0L)
        val logicalRelation = LogicalRelation(
          DataSource(
            sparkSession = sparkSession,
            className = "org.apache.spark.sql.hive.llap",
            options = Map(
              "table" -> (metadata.database + "." + table),
              "sizeinbytes" -> sizeInBytes.toString(),
              "url" -> connectionUrl)
          ).resolveRelation())

        SubqueryAlias(table, logicalRelation)
      } else {
        SubqueryAlias(table, tempTables(table))
      }
    }
  }


  /**
   * Retrieve the metadata of an existing permanent table/view. If no database is specified,
   * assume the table/view is in the current database. If the specified table/view is not found
   * in the database then a
   *  [[org.apache.spark.sql.catalyst.analysis.NoSuchTableException]] is thrown.
   */
  override def getTableMetadata(name: TableIdentifier): CatalogTable = {
    if (Thread.currentThread().getStackTrace()(2).toString().contains("DescribeTableCommand")) {
      val db = formatDatabaseName(name.database.getOrElse(getCurrentDatabase))
      val table = formatTableName(name.table)
      val sparkSession = SparkSession.getActiveSession.get.sqlContext.sparkSession
      val sessionState = SparkSession.getActiveSession.get.sessionState
      val getConnectionUrlMethod = sessionState.getClass.
        getMethod("getConnectionUrl", classOf[SparkSession])
      val connectionUrl = getConnectionUrlMethod.invoke(sessionState, sparkSession).toString()
      val getUserMethod = sessionState.getClass.getMethod("getUser")
      val user = getUserMethod.invoke(sessionState).toString()
      val dbcp2Configs = sparkSession.sqlContext.getConf("spark.sql.hive.llap.dbcp2", null)
      val connection = DefaultJDBCWrapper.getConnector(None, connectionUrl, user, dbcp2Configs)
      val stmt = connection.createStatement()
      try {
        stmt.executeUpdate(s"DESC `$db`.`$table`")
      } catch {
        case e: Throwable => throw new SQLException(
          e.toString.replace("shadehive.org.apache.hive.service.cli.HiveSQLException: ", ""))
        case e: HiveSQLException => throw new HiveSQLException(e)
      }
    }

    super.getTableMetadata(name)
  }
}
