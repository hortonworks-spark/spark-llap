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

import java.util.UUID

import org.apache.hadoop.hive.llap.LlapBaseInputFormat

import org.apache.spark.sql.{Dataset, Row, SQLContext}

import org.slf4j.LoggerFactory

class LlapQuery(val sc: SQLContext) {

  private val log = LoggerFactory.getLogger(getClass)

  private val handleIds = new scala.collection.mutable.HashSet[String]

  private var currentDatabase: String = "default"

  def setCurrentDatabase(dbName: String): Unit = {
    currentDatabase = dbName
  }

  def sql(queryString: String): Dataset[Row] = {
    val handleId = UUID.randomUUID().toString()
    handleIds.add(handleId)
    val df = sc.read
      .format("org.apache.spark.sql.hive.llap")
      .option("query", queryString)
      .option("handleid", handleId)
      .option("currentdatabase", currentDatabase)
      .load()
    df
  }

  def close(): Unit = {
    handleIds.foreach ((handleId) => {
      try {
        LlapBaseInputFormat.close(handleId)
      } catch {
        case ex: Exception => {
          log.error("Error closing " + handleId, ex)
        }
      }
    })
    handleIds.clear
  }
}
