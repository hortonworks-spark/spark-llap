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
import org.apache.spark.sql.hive.HiveSessionState
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

  /**
   * Return connection.
   */
  def connection: Connection = {
    DefaultJDBCWrapper.getConnector(None, getConnectionUrl(), getUserString())
  }

  /**
   * Return connection URL (with replaced proxy user name if exists).
   */
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
  val HIVESERVER2_JDBC_URL =
    SQLConfigBuilder("spark.sql.hive.hiveserver2.jdbc.url")
      .doc("HiveServer2 JDBC URL.")
      .stringConf
      .createWithDefault("")

  val HIVESERVER2_JDBC_URL_PRINCIPAL =
    SQLConfigBuilder("spark.sql.hive.hiveserver2.jdbc.url.principal")
      .doc("HiveServer2 JDBC Principal.")
      .stringConf
      .createWithDefault("")

  val HIVESERVER2_CREDENTIAL_ENABLED =
    SQLConfigBuilder("spark.yarn.security.credentials.hiveserver2.enabled")
      .doc("When true, HiveServer2 credential provider is enabled.")
      .booleanConf
      .createWithDefault(false)

  /**
   * For the given HiveServer2 JDBC URLs, attach the postfix strings if needed.
   *
   * For kerberized clusters,
   *
   * 1. YARN cluster mode: ";auth=delegationToken"
   * 2. YARN client mode: ";principal=hive/_HOST@EXAMPLE.COM"
   *
   * Non-kerberied clusters,
   * 3. Use the given URLs.
   */
  def getConnectionUrlFromConf(sparkSession: SparkSession): String = {
    if (!sparkSession.conf.contains(HIVESERVER2_JDBC_URL.key)) {
      throw new Exception("Spark conf does not contain config " + HIVESERVER2_JDBC_URL.key)
    }

    if (sparkSession.conf.get(HIVESERVER2_CREDENTIAL_ENABLED, false)) {
      // 1. YARN Cluster mode for kerberized clusters
      s"${sparkSession.conf.get(HIVESERVER2_JDBC_URL.key)};auth=delegationToken"
    } else if (sparkSession.sparkContext.conf.contains(HIVESERVER2_JDBC_URL_PRINCIPAL.key)) {
      // 2. YARN Client mode for kerberized clusters
      s"${sparkSession.conf.get(HIVESERVER2_JDBC_URL.key)};" +
        sparkSession.conf.get(HIVESERVER2_JDBC_URL_PRINCIPAL.key)
    } else {
      // 3. For non-kerberized cluster
      sparkSession.conf.get(HIVESERVER2_JDBC_URL.key)
    }
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
