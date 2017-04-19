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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.hive.{HiveMetastoreCatalog, MetastoreRelation}


/**
 * HiveMetastoreCatalog is still used in Apache Spark, but in the future will be cleaned up
 * to integrate into org.apache.spark.sql.hive.HiveExternalCatalog. At that time,
 * LlapMetastoreCatalog will be removed together.
 */
class LlapMetastoreCatalog(sparkSession: SparkSession)
  extends HiveMetastoreCatalog(sparkSession) {

  override def lookupRelation(
      tableIdentifier: TableIdentifier,
      alias: Option[String] = None): LogicalPlan = {
    val qualifiedTableName = getQualifiedTableName(tableIdentifier)

    // Use metastore catalog to lookup tables
    val catalog = sparkSession.sharedState.externalCatalog.asInstanceOf[LlapExternalCatalog]
    val table = catalog.getTable(qualifiedTableName.database, qualifiedTableName.name)

    // Now convert to LlapRelation
    val sessionState = sparkSession.sessionState.asInstanceOf[LlapSessionState]
    val logicalRelation = LogicalRelation(
      DataSource(
        sparkSession = sparkSession,
        className = "org.apache.spark.sql.hive.llap",
        options = Map(
          "table" -> (qualifiedTableName.database + "." + qualifiedTableName.name),
          "url" -> sessionState.getConnectionUrl())
      ).resolveRelation())

    val tableWithQualifiers = SubqueryAlias(qualifiedTableName.name, logicalRelation, None)
    alias.map(a => SubqueryAlias(a, tableWithQualifiers, None)).getOrElse(tableWithQualifiers)
  }
}
