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

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import com.hortonworks.spark.sql.hive.llap.DefaultJDBCWrapper
import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.Logging
import org.apache.spark.SparkConf
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.hive.HiveExternalCatalog
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType


/**
 * A persistent implementation of the system catalog using Hive.
 * All public methods must be synchronized for thread-safety.
 */
private[spark] class LlapExternalCatalog(
    conf: SparkConf,
    hadoopConf: Configuration)
  extends HiveExternalCatalog(conf, hadoopConf) with Logging {

  import CatalogTypes.TablePartitionSpec

  // Exceptions thrown by the hive client that we would like to wrap
  private val clientExceptions = Set(
    "org.apache.hadoop.hive.ql.metadata.HiveException",
    "org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException",
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
    tryWithResource(sessionState.connection) { conn =>
      tryWithResource(conn.createStatement()) { stmt =>
        val ifNotExistsString = if (ignoreIfExists) "IF NOT EXISTS" else ""
        stmt.executeUpdate(s"CREATE DATABASE $ifNotExistsString `${dbDefinition.name}`")
      }
    }
  }

  override def dropDatabase(
      db: String,
      ignoreIfNotExists: Boolean,
      cascade: Boolean): Unit = withClient {
    requireDbExists(db)
    val sessionState = SparkSession.getActiveSession.get.sessionState.asInstanceOf[LlapSessionState]
    tryWithResource(sessionState.connection) { conn =>
      tryWithResource(conn.createStatement()) { stmt =>
        val ifExistsString = if (ignoreIfNotExists) "IF EXISTS" else ""
        val cascadeString = if (cascade) "CASCADE" else ""
        stmt.executeUpdate(s"DROP DATABASE $ifExistsString `$db` $cascadeString")
      }
    }
  }

  override def databaseExists(db: String): Boolean = {
    val sparkSession = SparkSession.getActiveSession
    if (sparkSession.isDefined) {
      val sessionState = sparkSession.get.sessionState.asInstanceOf[LlapSessionState]
      var isExist = false
      tryWithResource(sessionState.connection) { conn =>
        tryWithResource(conn.createStatement()) { stmt =>
          tryWithResource(stmt.executeQuery(s"SHOW DATABASES LIKE '$db'")) { rs =>
            isExist = rs.next()
          }
        }
      }
      isExist
    } else {
      // This happens only once at the initialization of SparkSession.
      // Spark checks `default` database at the beginning and creates it if not exists.
      // However, if SparkSession is not created yet, we cannot access SessionState having
      // connection string and user name. Here, we know that Spark-LLAP always access the
      // existing Hive databases. So, we returns true for that. Also, Spark checks
      // `global_temp` database and raises exceptions if it exists. For this one,
      // we simply assume that it doesn't exist.
      db.equalsIgnoreCase(SessionCatalog.DEFAULT_DATABASE)
    }
  }

  override def listDatabases(): Seq[String] = withClient {
    listDatabases("*")
  }

  override def listDatabases(pattern: String): Seq[String] = withClient {
    val sessionState = SparkSession.getActiveSession.get.sessionState.asInstanceOf[LlapSessionState]
    val databases = new ArrayBuffer[String]()
    tryWithResource(sessionState.connection) { conn =>
      tryWithResource(conn.createStatement()) { stmt =>
        tryWithResource(stmt.executeQuery(s"SHOW DATABASES LIKE '$pattern'")) { rs =>
          while (rs.next()) {
            databases += rs.getString("database_name")
          }
        }
      }
    }
    databases
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
      val location = if (tableDefinition.storage.locationUri.isDefined) {
        s"LOCATION '${tableDefinition.storage.locationUri.get}'"
      } else {
        ""
      }
      executeUpdate(s"CREATE TABLE ${tableDefinition.identifier.quotedString} (dummy INT) " +
        location)
      super.dropTable(db, tableDefinition.identifier.table, ignoreIfNotExists = true, purge = true)
      super.createTable(tableDefinition, ignoreIfExists)
    }
  }

  override def dropTable(
      db: String,
      table: String,
      ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = withClient {
    if (Thread.currentThread().getStackTrace()(5).toString
        .contains("CreateHiveTableAsSelectCommand")) {
      super.dropTable(db, table, ignoreIfNotExists, purge)
    } else {
      requireDbExists(db)
      val sessionState =
        SparkSession.getActiveSession.get.sessionState.asInstanceOf[LlapSessionState]
      tryWithResource(sessionState.connection) { conn =>
        tryWithResource(conn.createStatement()) { stmt =>
          val ifExistsString = if (ignoreIfNotExists) "IF EXISTS" else ""
          val purgeString = if (purge) "PURGE" else ""
          stmt.executeUpdate(s"DROP TABLE $ifExistsString $db.$table $purgeString")
        }
      }
    }
  }

  override def getTable(db: String, table: String): CatalogTable = withClient {
    try {
      val catalogTable = super.getTable(db, table)
      catalogTable.copy(tableType = CatalogTableType.EXTERNAL)
    } catch {
      case NonFatal(_) =>
        // Try to create a dummy table. This table cannot be used for ALTER TABLE.
        val sessionState =
          SparkSession.getActiveSession.get.sessionState.asInstanceOf[LlapSessionState]
        tryWithResource(sessionState.connection) { conn =>
          tryWithResource(conn.getMetaData.getColumns(null, db, table, null)) { rs =>
            val schema = new StructType()
            while (rs.next()) {
              val columnName = rs.getString(4)
              val dataType = rs.getInt(5)
              val typeName = rs.getString(6)
              val fieldSize = rs.getInt(7)
              val fieldScale = rs.getInt(9)
              val nullable = true // Hive cols nullable
              val isSigned = true
              val columnType =
                DefaultJDBCWrapper.getCatalystType(
                  dataType, typeName, fieldSize, fieldScale, isSigned)
              schema.add(columnName, columnType, nullable)
            }
            CatalogTable(
              identifier = TableIdentifier(table, Option(db)),
              tableType = CatalogTableType.EXTERNAL,
              schema = schema,
              storage = CatalogStorageFormat(
                locationUri = None,
                inputFormat = None,
                outputFormat = None,
                serde = None,
                compressed = false,
                properties = Map.empty))
          }
        }
    }
  }

  override def tableExists(db: String, table: String): Boolean = withClient {
    val sessionState = SparkSession.getActiveSession.get.sessionState.asInstanceOf[LlapSessionState]
    tryWithResource(sessionState.connection) { conn =>
      tryWithResource(conn.getMetaData.getTables(null, db, table, null)) { rs =>
        rs.next()
      }
    }
  }

  override def listTables(db: String): Seq[String] = listTables(db, "*")

  override def listTables(db: String, pattern: String): Seq[String] = withClient {
    val sessionState = SparkSession.getActiveSession.get.sessionState.asInstanceOf[LlapSessionState]
    var tableList: List[String] = Nil
    tryWithResource(sessionState.connection) { conn =>
      tryWithResource(conn.getMetaData.getTables(null, db, pattern, null)) { rs =>
        while (rs.next()) {
          tableList = rs.getString(3) :: tableList
        }
      }
    }
    tableList.reverse
  }

  override def loadTable(
      db: String,
      table: String,
      loadPath: String,
      isOverwrite: Boolean,
      isSrcLocal: Boolean): Unit = {
    requireDbExists(db)
    requireTableExists(db, table)

    val localString = if (isSrcLocal) "LOCAL" else ""
    val overwriteString = if (isOverwrite) "OVERWRITE" else ""

    executeUpdate(
      s"""
        |LOAD DATA $localString
        |INPATH '$loadPath'
        |$overwriteString
        |INTO TABLE $db.$table
        |""".stripMargin)
  }

  override def loadPartition(
      db: String,
      table: String,
      loadPath: String,
      partition: TablePartitionSpec,
      isOverwrite: Boolean,
      inheritTableSpecs: Boolean,
      isSrcLocal: Boolean): Unit = {
    requireDbExists(db)
    requireTableExists(db, table)

    val localString = if (isSrcLocal) "LOCAL" else ""
    val overwriteString = if (isOverwrite) "OVERWRITE" else ""
    val partitionString = partition.map { case (k, v) =>
      s"${k.toLowerCase}=$v"
    }.mkString("PARTITION (", ",", ")")

    executeUpdate(
      s"""
        |LOAD DATA $localString
        |INPATH '$loadPath'
        |$overwriteString
        |INTO TABLE $db.$table
        |$partitionString
        |""".stripMargin)
  }

  override def renameTable(db: String, oldName: String, newName: String): Unit = {
    requireDbExists(db)
    executeUpdate(s"ALTER TABLE $db.$oldName RENAME TO $db.$newName")
  }

  override def alterTable(tableDefinition: CatalogTable): Unit = {
    executeUpdate(s"ALTER TABLE ${tableDefinition.identifier.quotedString} TOUCH")
    super.alterTable(tableDefinition)
  }

  override def createPartitions(
      db: String,
      table: String,
      parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit = {
    executeUpdate(s"ALTER TABLE `$db`.`$table` TOUCH")
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
    val ifExistsString = if (ignoreIfNotExists) "IF EXISTS" else ""
    val partitionString = parts
      .map(_.map { case (k, v) => s"$k=$v" }.mkString("PARTITION (", ", ", ")"))
      .mkString(", ")
    val purgeString = if (purge) "PURGE" else ""
    executeUpdate(s"ALTER TABLE `$db`.`$table` DROP $ifExistsString $partitionString $purgeString")
  }

  override def renamePartitions(
      db: String,
      table: String,
      specs: Seq[CatalogTypes.TablePartitionSpec],
      newSpecs: Seq[CatalogTypes.TablePartitionSpec]): Unit = withClient {
    executeUpdate(s"ALTER TABLE `$db`.`$table` TOUCH")
    super.renamePartitions(db, table, specs, newSpecs)
  }

  override def alterPartitions(
      db: String,
      table: String,
      newParts: Seq[CatalogTablePartition]): Unit = withClient {
    executeUpdate(s"ALTER TABLE `$db`.`$table` TOUCH")
    super.alterPartitions(db, table, newParts)
  }

  private def executeUpdate(sql: String): Unit = {
    logDebug(sql)
    val sessionState = SparkSession.getActiveSession.get.sessionState.asInstanceOf[LlapSessionState]
    tryWithResource(sessionState.connection) { conn =>
      tryWithResource(conn.createStatement()) { stmt =>
        stmt.executeUpdate(sql)
      }
    }
  }

  private def tryWithResource[R <: AutoCloseable, T](createResource: => R)(f: R => T): T = {
    val resource = createResource
    try f.apply(resource) finally resource.close()
  }
}
