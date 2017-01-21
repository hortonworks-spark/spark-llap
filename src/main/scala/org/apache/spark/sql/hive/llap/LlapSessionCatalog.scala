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

import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.exec.{UDAF, UDF}
import org.apache.hadoop.hive.ql.exec.{FunctionRegistry => HiveFunctionRegistry}
import org.apache.hadoop.hive.ql.udf.generic.{AbstractGenericUDAFResolver, GenericUDF, GenericUDTF}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.hive.{HiveGenericUDF, HiveGenericUDTF, HiveSessionCatalog, HiveSimpleUDF, HiveUDAFFunction}
import org.apache.spark.sql.hive.HiveShim.HiveFunctionWrapper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DecimalType, DoubleType}


private[sql] class LlapSessionCatalog(
    externalCatalog: LlapExternalCatalog,
    globalTempViewManager: GlobalTempViewManager,
    sparkSession: SparkSession,
    functionResourceLoader: FunctionResourceLoader,
    functionRegistry: FunctionRegistry,
    conf: SQLConf,
    hadoopConf: Configuration)
  extends HiveSessionCatalog(
    externalCatalog,
    globalTempViewManager,
    sparkSession,
    functionResourceLoader,
    functionRegistry,
    conf,
    hadoopConf) with Logging {

  private val metastoreCatalog = new LlapMetastoreCatalog(sparkSession)

  override def lookupRelation(name: TableIdentifier, alias: Option[String]): LogicalPlan = {
    val table = formatTableName(name.table)
    if (name.database.isDefined || !tempTables.contains(table)) {
      val database = name.database.map(formatDatabaseName)
      val newName = name.copy(database = database, table = table)
      metastoreCatalog.lookupRelation(newName, alias)
    } else {
      val relation = tempTables(table)
      val tableWithQualifiers = SubqueryAlias(table, relation, None)
      // If an alias was specified by the lookup, wrap the plan in a subquery so that
      // attributes are properly qualified with this alias.
      alias.map(a => SubqueryAlias(a, tableWithQualifiers, None)).getOrElse(tableWithQualifiers)
    }
  }

  /**
   * Retrieve the metadata of an existing permanent table/view. If no database is specified,
   * assume the table/view is in the current database. If the specified table/view is not found
   * in the database then a [[NoSuchTableException]] is thrown.
   */
  override def getTableMetadata(name: TableIdentifier): CatalogTable = {
    if (Thread.currentThread().getStackTrace()(2).toString().contains("DescribeTableCommand")) {
      val db = formatDatabaseName(name.database.getOrElse(getCurrentDatabase))
      val table = formatTableName(name.table)
      val sessionState = sparkSession.sessionState.asInstanceOf[LlapSessionState]
      val stmt = sessionState.connection.createStatement()
      stmt.executeUpdate(s"DESC `$db`.`$table`")
    }

    super.getTableMetadata(name)
  }
}
