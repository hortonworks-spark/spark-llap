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

import java.util.concurrent.CancellationException

import scala.util.control.NonFatal

import com.hortonworks.spark.sql.hive.llap.DefaultJDBCWrapper
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.thrift.TException

import org.apache.spark.internal.Logging
import org.apache.spark.SparkConf
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.hive.HiveExternalCatalog
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructType}


/**
 * A persistent implementation of the system catalog using Hive.
 * All public methods must be synchronized for thread-safety.
 */
private[spark] class LlapExternalCatalog(
    conf: SparkConf,
    hadoopConf: Configuration)
  extends HiveExternalCatalog(conf, hadoopConf) with Logging {

  // Exceptions thrown by the hive client that we would like to wrap
  private val clientExceptions = Set(
    "org.apache.hadoop.hive.ql.metadata.HiveException",
    "org.apache.thrift.TException")

  /**
   * Whether this is an exception thrown by the hive client that should be wrapped.
   *
   * Due to classloader isolation issues, pattern matching won't work here so we need
   * to compare the canonical names of the exceptions, which we assume to be stable.
   */
  private def isClientException(e: Throwable): Boolean = {
    var temp: Class[_] = e.getClass
    var found = false
    while (temp != null && !found) {
      found = clientExceptions.contains(temp.getCanonicalName)
      temp = temp.getSuperclass
    }
    found
  }

  /**
   * Run some code involving `client` in a [[synchronized]] block and wrap certain
   * exceptions thrown in the process in [[AnalysisException]].
   */
  private def withClient[T](body: => T): T = synchronized {
    try {
      body
    } catch {
      case NonFatal(e) if isClientException(e) =>
        throw new AnalysisException(
          e.getClass.getCanonicalName + ": " + e.getMessage, cause = Some(e))
    }
  }

  private def requireDbMatches(db: String, table: CatalogTable): Unit = {
    if (table.identifier.database != Some(db)) {
      throw new AnalysisException(
        s"Provided database '$db' does not match the one specified in the " +
        s"table definition (${table.identifier.database.getOrElse("n/a")})")
    }
  }

  override def createDatabase(
      dbDefinition: CatalogDatabase,
      ignoreIfExists: Boolean): Unit = withClient {
    val sessionState = SparkSession.getActiveSession.get.sessionState.asInstanceOf[LlapSessionState]
    val stmt = sessionState.connection.createStatement()
    val ifNotExistsString = if (ignoreIfExists) "IF NOT EXISTS" else ""
    stmt.executeUpdate(s"CREATE DATABASE $ifNotExistsString `${dbDefinition.name}`")
  }

  override def dropDatabase(
      db: String,
      ignoreIfNotExists: Boolean,
      cascade: Boolean): Unit = withClient {
    requireDbExists(db)
    val sessionState = SparkSession.getActiveSession.get.sessionState.asInstanceOf[LlapSessionState]
    val stmt = sessionState.connection.createStatement()
    val ifExistsString = if (ignoreIfNotExists) "IF EXISTS" else ""
    val cascadeString = if (cascade) "CASCADE" else ""
    stmt.executeUpdate(s"DROP DATABASE $ifExistsString `$db` $cascadeString")
  }

  override def createTable(
      tableDefinition: CatalogTable,
      ignoreIfExists: Boolean): Unit = {
    logInfo(tableDefinition.toString)

    assert(tableDefinition.identifier.database.isDefined)
    val db = tableDefinition.identifier.database.get
    requireDbExists(db)

    if (tableExists(db, tableDefinition.identifier.table)) {
      if (ignoreIfExists) {
        // No-op
      } else {
        throw new TableAlreadyExistsException(db = db, table = tableDefinition.identifier.table)
      }
    } else {
      val sessionState =
        SparkSession.getActiveSession.get.sessionState.asInstanceOf[LlapSessionState]
      val stmt = sessionState.connection.createStatement()
      // Check the privilege by creating a dummy table with the given name.
      stmt.executeUpdate(s"CREATE TABLE ${tableDefinition.identifier.quotedString} (dummy INT)")
      super.dropTable(db, tableDefinition.identifier.table, true, true)
      super.createTable(tableDefinition, ignoreIfExists)
    }
  }

  override def dropTable(
      db: String,
      table: String,
      ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = withClient {
    if (Thread.currentThread().getStackTrace()(5).toString().contains("CreateHiveTableAsSelectCommand")) {
      super.dropTable(db, table, ignoreIfNotExists, purge)
    } else {
      requireDbExists(db)
      val sessionState = SparkSession.getActiveSession.get.sessionState.asInstanceOf[LlapSessionState]
      val stmt = sessionState.connection.createStatement()
      val ifExistsString = if (ignoreIfNotExists) "IF EXISTS" else ""
      val purgeString = if (purge) "PURGE" else ""
      stmt.executeUpdate(s"DROP TABLE $ifExistsString $db.$table $purgeString")
    }
  }

  override def getTable(db: String, table: String): CatalogTable = withClient {
    val catalogTable = super.getTable(db, table)
    catalogTable.copy(tableType = CatalogTableType.EXTERNAL)
  }

  override def tableExists(db: String, table: String): Boolean = withClient {
    val sessionState = SparkSession.getActiveSession.get.sessionState.asInstanceOf[LlapSessionState]
    val dmd = sessionState.connection.getMetaData()
    val rs = dmd.getTables(null, db, table, null)
    val result = rs.next()
    rs.close()
    result
  }

  override def listTables(db: String): Seq[String] = listTables(db, "*")

  override def listTables(db: String, pattern: String): Seq[String] = withClient {
    val sessionState = SparkSession.getActiveSession.get.sessionState.asInstanceOf[LlapSessionState]
    val dmd = sessionState.connection.getMetaData()
    val rs = dmd.getTables(null, db, pattern, null)
    var tableList: List[String] = Nil
    while (rs.next()) {
      tableList = rs.getString(3) :: tableList
    }
    rs.close()
    tableList.reverse
  }

  override def renameTable(db: String, oldName: String, newName: String): Unit = {
    requireDbExists(db)
    val sessionState = SparkSession.getActiveSession.get.sessionState.asInstanceOf[LlapSessionState]
    val stmt = sessionState.connection.createStatement()
    stmt.executeUpdate(s"ALTER TABLE $db.$oldName RENAME TO $db.$newName")
  }

  override def alterTable(tableDefinition: CatalogTable): Unit = {
    val sessionState = SparkSession.getActiveSession.get.sessionState.asInstanceOf[LlapSessionState]
    val stmt = sessionState.connection.createStatement()
    val tableName = tableDefinition.identifier.quotedString
    stmt.executeUpdate(s"ALTER TABLE $tableName TOUCH")
    super.alterTable(tableDefinition)
  }

  override def createPartitions(
      db: String,
      table: String,
      parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit = {
    val sessionState = SparkSession.getActiveSession.get.sessionState.asInstanceOf[LlapSessionState]
    val stmt = sessionState.connection.createStatement()
    stmt.executeUpdate(s"ALTER TABLE `$db`.`$table` TOUCH")
    super.createPartitions(db, table, parts, ignoreIfExists)
  }

  override def dropPartitions(
      db: String,
      table: String,
      parts: Seq[CatalogTypes.TablePartitionSpec],
      ignoreIfNotExists: Boolean,
      purge: Boolean,
      retainData: Boolean): Unit = {
    // Note that `retainData` support is dropped intentionally to support SPARK-LLAP-32 instead.
    val sessionState = SparkSession.getActiveSession.get.sessionState.asInstanceOf[LlapSessionState]
    val stmt = sessionState.connection.createStatement()
    val ifExistsString = if (ignoreIfNotExists) "IF EXISTS" else ""
    val partitionString =
      parts.map(_.map{ case (k, v) => s"$k=$v" }.mkString("PARTITION (", ", ", ")")).mkString(", ")
    val purgeString = if (purge) "PURGE" else ""
    stmt.executeUpdate(
      s"ALTER TABLE `$db`.`$table` DROP $ifExistsString $partitionString $purgeString")
  }

  override def renamePartitions(
      db: String,
      table: String,
      specs: Seq[CatalogTypes.TablePartitionSpec],
      newSpecs: Seq[CatalogTypes.TablePartitionSpec]): Unit = withClient {
    val sessionState = SparkSession.getActiveSession.get.sessionState.asInstanceOf[LlapSessionState]
    val stmt = sessionState.connection.createStatement()
    stmt.executeUpdate(s"ALTER TABLE `$db`.`$table` TOUCH")
    super.renamePartitions(db, table, specs, newSpecs)
  }

  override def alterPartitions(
      db: String,
      table: String,
      newParts: Seq[CatalogTablePartition]): Unit = withClient {
    val sessionState = SparkSession.getActiveSession.get.sessionState.asInstanceOf[LlapSessionState]
    val stmt = sessionState.connection.createStatement()
    stmt.executeUpdate(s"ALTER TABLE `$db`.`$table` TOUCH")
    super.alterPartitions(db, table, newParts)
  }
}
