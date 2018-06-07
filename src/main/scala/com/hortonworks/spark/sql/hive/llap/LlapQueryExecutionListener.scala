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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution._
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.sql.execution.datasources.v2._

class LlapQueryExecutionListener extends QueryExecutionListener with Logging {

  /**
   * Closes all LlapRelations to notify Hive to clean up.
   */
  private def closeLlapRelation(queryExecution: QueryExecution): Unit = {
    queryExecution.sparkPlan.foreach {
      case r: RowDataSourceScanExec if r.relation.isInstanceOf[LlapRelation] =>
        r.relation.asInstanceOf[LlapRelation].close()
        logDebug(s"Closing Hive connection via ${classOf[LlapRelation].getName}")
      case s: DataSourceV2ScanExec if s.reader.isInstanceOf[HiveWarehouseDataSourceReader] =>
        s.reader.asInstanceOf[HiveWarehouseDataSourceReader].close()
        logDebug(s"Closing Hive connection via ${classOf[HiveWarehouseDataSourceReader].getName}")
      case _ =>
    }
  }

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    closeLlapRelation(qe)
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    closeLlapRelation(qe)
  }
}
