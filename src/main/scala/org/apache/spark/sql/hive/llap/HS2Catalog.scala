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

import java.sql.Connection
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.SimpleCatalystConf
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.Catalog
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Subquery
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.ResolvedDataSource

/**
 * Catalog implementation, using JDBC
 */
private [llap] class HS2Catalog(sqlContext: SQLContext, jdbcUrl: String, connection: Connection)
  extends Catalog {

  var relationSourceName = "org.apache.spark.sql.hive.llap"
  private var currentDatabase = "default"

  override val conf: CatalystConf = new SimpleCatalystConf(false)

  override def tableExists(tableIdentifier: TableIdentifier): Boolean = {

    val dmd = connection.getMetaData()
    var dbName = getDbName(tableIdentifier)

    // In the absence of a DB name, should call getTables() with "" as DB name,
    // to search with no schema. null DB name has different semantics in getTables()
    // TODO: Another option is to set the dbName to the default DB
    val rs = dmd.getTables(null, getDbName(tableIdentifier), tableIdentifier.table, null)

    val tableExists = rs.next()
    rs.close()
    tableExists
  }

  override def lookupRelation(
      tableIdentifier: TableIdentifier,
      alias: Option[String] = None): LogicalPlan = {

    if (!tableExists(tableIdentifier)) {
      throw new Exception("Table " + tableIdentifier + " does not exist")
    }
    val qualifiedName = getDbName(tableIdentifier) + "." + tableIdentifier.table

    var options = Map("table" -> qualifiedName, "url" -> jdbcUrl)
    val resolved = ResolvedDataSource(
        sqlContext,
        None,
        Array[String](),
        relationSourceName,
        options)
    val logicalRelation = LogicalRelation(resolved.relation)
    val tableWithQualifiers = Subquery(tableIdentifier.table, logicalRelation)
    alias.map(a => Subquery(a, tableWithQualifiers)).getOrElse(tableWithQualifiers)
  }

  /**
   * Returns tuples of (tableName, isTemporary) for all tables in the given database.
   * isTemporary is a Boolean value indicates if a table is a temporary or not.
   */
  override def getTables(databaseName: Option[String]): Seq[(String, Boolean)] = {
    val dmd = connection.getMetaData()
    val rs = dmd.getTables(null, databaseName.getOrElse(getCurrentDatabase()), "%", null)
    var tableList: List[(String, Boolean)] = Nil
    while (rs.next()) {
      tableList = (rs.getString(3), false) :: tableList
    }
    tableList.reverse
  }

  override def refreshTable(tableIdent: TableIdentifier): Unit = {
    // TODO
  }

  // TODO: Refactor it in the work of SPARK-10104
  override def registerTable(tableIdentifier: TableIdentifier, plan: LogicalPlan): Unit = {
    // TODO
  }

  // TODO: Refactor it in the work of SPARK-10104
  override def unregisterTable(tableIdentifier: TableIdentifier): Unit = {
    // TODO
  }

  override def unregisterAllTables(): Unit = {
    // TODO
  }

  protected def getDbName(tableIdent: TableIdentifier): String = {
    if (tableIdent.database.isEmpty) {
      getCurrentDatabase()
    } else {
      tableIdent.database.get
    }
  }

  def getCurrentDatabase(): String = {
    currentDatabase
  }

  def setCurrentDatabase(dbName: String): Unit = {
    currentDatabase = dbName
  }
}
