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

import scala.reflect.runtime.{universe => ru}

import com.hortonworks.spark.sql.hive.llap.DefaultJDBCWrapper

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.{HiveContext, HiveSessionState}
import org.apache.spark.sql.internal.SQLConf.SQLConfigBuilder

/**
 * A class that holds all session-specific state in a given SparkSession backed by Llap.
 */
class LlapSessionState(sparkSession: SparkSession)
  extends HiveSessionState(sparkSession) with Logging {

  self =>

  /**
   * Internal catalog for managing table and database states.
   */
  override lazy val catalog = {
    new LlapSessionCatalog(
      sparkSession.sharedState.externalCatalog.asInstanceOf[LlapExternalCatalog],
      sparkSession.sharedState.globalTempViewManager,
      sparkSession,
      functionResourceLoader,
      functionRegistry,
      conf,
      newHadoopConf())
  }

  def connection: Connection = {
    DefaultJDBCWrapper.getConnector(None, getConnectionUrl(), getUserString())
  }

  def getConnectionUrl(): String = {
    var userString = getUserString()
    if (userString == null) {
      userString = ""
    }
    val urlString = LlapSessionState.getConnectionUrlFromConf(sparkSession)
    urlString.replace("${user}", userString)
  }

  def getUserString(): String = {
    LlapSessionState.getUserMethod match {
      case null =>
        null
      case _ =>
        val instanceMirror = ru.runtimeMirror(this.getClass.getClassLoader).reflect(this)
        val methodMirror = instanceMirror.reflectMethod(LlapSessionState.getUserMethod)
        methodMirror().asInstanceOf[String]
    }
  }
}

object LlapSessionState {
  val HIVESERVER2_URL = SQLConfigBuilder("spark.sql.hive.hiveserver2.url")
    .doc("HiveServer2 URL.")
    .stringConf
    .createWithDefault("")

  def getConnectionUrlFromConf(sparkSession: SparkSession): String = {
    if (!sparkSession.sparkContext.conf.contains(HIVESERVER2_URL.key)) {
      throw new Exception("Spark conf does not contain config " + HIVESERVER2_URL.key)
    }
    sparkSession.sparkContext.conf.get(HIVESERVER2_URL.key)
  }

  private[llap] lazy val getUserMethod = findGetUserMethod()

  private def findGetUserMethod(): ru.MethodSymbol = {
    val symbol = ru.typeOf[HiveSessionState].declaration(ru.stringToTermName("getUser"))
    val methodSymbol = symbol match {
      case ru.NoSymbol => null
      case null => null
      case _ => symbol.asMethod
    }
    methodSymbol
  }
}
