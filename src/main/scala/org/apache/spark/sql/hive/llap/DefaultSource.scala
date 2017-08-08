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

import com.hortonworks.spark.sql.hive.llap.{DefaultJDBCWrapper, LlapRelation}

import org.apache.spark.sql._
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.RelationProvider

class DefaultSource extends RelationProvider {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String])
      : BaseRelation = {
    val sessionState = SparkSession.getActiveSession.get.sessionState
    val getConnectionUrlMethod = sessionState.getClass.
      getMethod("getConnectionUrl", classOf[SparkSession])
    val connectionUrl = getConnectionUrlMethod.
      invoke(sessionState, sqlContext.sparkSession).toString()
    val getUserMethod = sessionState.getClass.getMethod("getUser")
    val user = getUserMethod.invoke(sessionState).toString()
    val dbcp2Config = sqlContext.getConf("spark.sql.hive.llap.dbcp2", null)
    val params = parameters +
      ("user.name" -> user) +
      ("user.password" -> "password") +
      ("dbcp2.conf" -> dbcp2Config) +
      ("url" -> connectionUrl)

    LlapRelation(
      sqlContext,
      params)
  }
}
