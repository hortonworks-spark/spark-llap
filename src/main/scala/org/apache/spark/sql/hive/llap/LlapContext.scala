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
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLConf
import org.apache.spark.sql.SQLConf.SQLConfEntry.stringConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.analysis.OverrideCatalog

class LlapContext(
  @transient override val sparkContext: SparkContext,
  val connectionUrl: String,
  @transient val connection: Connection, val userName: String)
    extends SQLContext(sparkContext) {
  @transient override lazy val catalog = getCatalog()

  def getCatalog() = {
    new HS2Catalog(this, connectionUrl, connection) with OverrideCatalog
  }

  def setCurrentDatabase(dbName: String) = {
    catalog.setCurrentDatabase(dbName)
  }

  protected[sql] override lazy val conf: SQLConf = new SQLConf {
    override def caseSensitiveAnalysis: Boolean = getConf(SQLConf.CASE_SENSITIVE, false)
  }
}

object LlapContext {
  val HIVESERVER2_URL = stringConf(
    key = "spark.sql.hive.hiveserver2.url",
    defaultValue = None,
    doc = "HiveServer2 URL.")

  def getUser(): String = {
    System.getProperty("hive_user", System.getProperty("user.name"))
  }

  def newInstance(sparkContext: SparkContext, connectionUrl: String): LlapContext = {
    val userName: String = getUser()
    val conn = DefaultJDBCWrapper.getConnector(None, url = connectionUrl, userName)
    new LlapContext(sparkContext, connectionUrl, conn, userName)
  }

  def newInstance(sparkContext: SparkContext): LlapContext = {
    LlapContext.newInstance(sparkContext, LlapContext.getConnectionUrlFromConf(sparkContext))
  }

  private def getConnectionUrlFromConf(sparkContext: SparkContext): String = {
    if (!sparkContext.conf.contains(HIVESERVER2_URL.key)) {
      throw new Exception("Spark conf does not contain config " + HIVESERVER2_URL.key)
    }
    sparkContext.conf.get(HIVESERVER2_URL.key)
  }
}

