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

import com.hortonworks.spark.sql.hive.llap.util.JobUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.security.token.Token
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.spark.SparkConf
import org.apache.spark.deploy.yarn.security.ServiceCredentialProvider
import org.apache.spark.internal.Logging

import scala.util.control.NonFatal

private[security] class HiveStreamingCredentialProvider extends ServiceCredentialProvider
  with Logging {

  override def serviceName: String = "hivestreaming"

  private val classNotFoundErrorStr = s"You are attempting to use the " +
    s"${getClass.getCanonicalName}, but your Spark distribution is not built with Hive libraries."

  private def hiveConf(hadoopConf: Configuration): Configuration = {
    try {
      new HiveConf(hadoopConf, classOf[HiveConf])
    } catch {
      case NonFatal(e) =>
        logDebug("Fail to create Hive Configuration", e)
        hadoopConf
      case e: NoClassDefFoundError =>
        logWarning(classNotFoundErrorStr)
        hadoopConf
    }
  }

  override def obtainCredentials(
                                  hadoopConf: Configuration,
                                  sparkConf: SparkConf,
                                  creds: Credentials): Option[Long] = {

    logInfo("Obtaining delegation token (secure metastore) for hive streaming..")
    try {
      val conf = hiveConf(hadoopConf)
      val principalKey = "hive.metastore.kerberos.principal"
      val principal = conf.getTrimmed(principalKey, "")
      require(principal.nonEmpty, s"Hive principal $principalKey undefined")
      val metastoreUri = conf.getTrimmed("hive.metastore.uris", "")
      require(metastoreUri.nonEmpty, "Hive metastore uri undefined")

      val currentUser = UserGroupInformation.getCurrentUser
      logInfo(s"Getting Hive delegation token for ${currentUser.getUserName} against " +
        s"$principal at $metastoreUri")

      doAsRealUser {
        val hive = Hive.get(conf, classOf[HiveConf])
        val tokenStr = hive.getDelegationToken(currentUser.getUserName, principal)
        val hive2Token = new Token[DelegationTokenIdentifier]()
        hive2Token.decodeFromUrlString(tokenStr)
        creds.addToken(hive2Token.getKind, hive2Token)
        logInfo(s"Added delegation token (secure metastore) for hive streaming: " +
          s"${hive2Token.toString} alias: ${hive2Token.getKind}")
      }

      None
    } catch {
      case NonFatal(e) =>
        logInfo(s"Failed to get token from service $serviceName", e)
        None
      case e: NoClassDefFoundError =>
        logWarning(classNotFoundErrorStr)
        None
    } finally {
      Hive.closeCurrent()
    }

    None
  }

  /**
    * Run some code as the real logged in user (which may differ from the current user, for
    * example, when using proxying).
    */
  private def doAsRealUser[T](fn: => T): T = {
    val currentUser = UserGroupInformation.getCurrentUser
    val realUser = Option(currentUser.getRealUser).getOrElse(currentUser)

    // For some reason the Scala-generated anonymous class ends up causing an
    // UndeclaredThrowableException, even if you annotate the method with @throws.
    try {
      realUser.doAs(new PrivilegedExceptionAction[T]() {
        override def run(): T = fn
      })
    } catch {
      case e: UndeclaredThrowableException => throw Option(e.getCause).getOrElse(e)
    }
  }
}
