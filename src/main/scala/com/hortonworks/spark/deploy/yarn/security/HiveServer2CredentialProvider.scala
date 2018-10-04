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

package com.hortonworks.spark.deploy.yarn.security

import java.lang.reflect.UndeclaredThrowableException
import java.security.PrivilegedExceptionAction
import java.sql.{Connection, DriverManager}

import com.hortonworks.spark.sql.hive.llap.util.JobUtil

import scala.util.control.NonFatal
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.token.Token
import org.apache.spark.SparkConf
import org.apache.spark.deploy.yarn.security.ServiceCredentialProvider
import org.apache.spark.internal.Logging
import com.hortonworks.spark.sql.hive.llap.{LlapRelation, Utils}

private[security] class HiveServer2CredentialProvider extends ServiceCredentialProvider
    with Logging {

  override def serviceName: String = "hiveserver2"

  override def obtainCredentials(
      hadoopConf: Configuration,
      sparkConf: SparkConf,
      creds: Credentials): Option[Long] = {

    JobUtil.replaceSparkHiveDriver();
    var con: Connection = null
    try {

      val currentUser = UserGroupInformation.getCurrentUser()
      val userName = if (sparkConf.getBoolean(
          "spark.yarn.security.credentials.hiveserver2.useShortUserName", true)) {
        currentUser.getShortUserName
      } else {
        currentUser.getUserName
      }

      val hs2Url = sparkConf.get("spark.sql.hive.hiveserver2.jdbc.url")
      val principal = sparkConf.get("spark.sql.hive.hiveserver2.jdbc.url.principal")
      require(hs2Url != null, "spark.sql.hive.hiveserver2.jdbc.url is not configured.")
      require(principal != null,
        "spark.sql.hive.hiveserver2.jdbc.url.principal is not configured.")

      val jdbcUrl = s"$hs2Url;principal=$principal"
      logInfo(s"Getting HS2 delegation token for $userName via $jdbcUrl")

      doAsRealUser {
        con = DriverManager.getConnection(jdbcUrl)
        val method = con.getClass.getMethod("getDelegationToken", classOf[String], classOf[String])
        val tokenStr = method.invoke(con, userName, principal).asInstanceOf[String]
        val token = new Token[DelegationTokenIdentifier]()
        token.decodeFromUrlString(tokenStr)
        creds.addToken(new Text("hive.jdbc.delegation.token"), token)
        logInfo(s"Added HS2 delegation token for $userName via $jdbcUrl")
      }
    } catch {
      case NonFatal(e) => logWarning(s"Failed to get HS2 delegation token", e)
    } finally {
      if (con != null) {
        con.close()
        con = null
      }
    }

    None
  }

  /**
   * Run some code as the real logged in user (which may differ from the current user, for
   * example, when using proxying).
   */
  private def doAsRealUser[T](fn: => T): T = {
    val currentUser = UserGroupInformation.getCurrentUser()
    val realUser = Option(currentUser.getRealUser()).getOrElse(currentUser)

    // For some reason the Scala-generated anonymous class ends up causing an
    // UndeclaredThrowableException, even if you annotate the method with @throws.
    try {
      realUser.doAs(new PrivilegedExceptionAction[T]() {
        override def run(): T = fn
      })
    } catch {
      case e: UndeclaredThrowableException => throw Option(e.getCause()).getOrElse(e)
    }
  }
}
