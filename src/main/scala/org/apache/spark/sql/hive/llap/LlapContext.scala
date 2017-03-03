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

import java.io.PrintStream
import java.sql.Connection
import java.util.{Map => JMap}

import scala.reflect.runtime.{universe => ru}

import com.hortonworks.spark.sql.hive.llap.DefaultJDBCWrapper

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLConf
import org.apache.spark.sql.SQLConf.SQLConfEntry.stringConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.OverrideCatalog
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Subquery
import org.apache.spark.sql.execution.CacheManager
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.ResolvedDataSource
import org.apache.spark.sql.execution.ui.SQLListener
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.HiveMetastoreCatalog
import org.apache.spark.sql.hive.MetastoreRelation
import org.apache.spark.sql.hive.client.{ClientInterface, ClientWrapper, HiveDatabase, HiveTable}

class LlapContext(sc: SparkContext,
    cacheManager: CacheManager,
    listener: SQLListener,
    @transient private val execHive: ClientWrapper,
    @transient private val metaHive: ClientInterface,
    isRootContext: Boolean)
  extends HiveContext(sc, cacheManager, listener, execHive, metaHive, isRootContext) {
  override protected[sql] lazy val catalog =
    new LlapCatalog(metadataHive, this) with OverrideCatalog

  override def newSession(): LlapContext = {
    new LlapContext(
      sc = sc,
      cacheManager = cacheManager,
      listener = listener,
      execHive = executionHive.newSession(),
      metaHive = metadataHive.newSession(),
      isRootContext = false)
  }

  def this(sc: SparkContext) = {
    this(sc, new CacheManager, SQLContext.createListenerAndUI(sc), null, null, true)
  }

  def connection: Connection = {
    if (conn == null) {
      conn = DefaultJDBCWrapper.getConnector(None, getConnectionUrl(), getUserString())
    }
    conn
  }

  var conn: Connection = null

  def getConnectionUrl(): String = {
    var userString = getUserString()
    if (userString == null) {
      userString = ""
    }
    var urlString = LlapContext.getConnectionUrlFromConf(sc)
    urlString.replace("${user}", userString)
  }

  def getUserString(): String = {
    LlapContext.getUserMethod match {
      case null => null
      case _ =>
        val instanceMirror = ru.runtimeMirror(this.getClass.getClassLoader).reflect(this)
        val methodMirror = instanceMirror.reflectMethod(LlapContext.getUserMethod)
        methodMirror().asInstanceOf[String]
    }
  }
}

private[hive] class ClientProxy(client: ClientInterface) extends ClientInterface {
  // scalastyle:off
  def version() = client.version
  def getConf(key: String, defaultValue: String) = client.getConf(key, defaultValue)
  def runSqlHive(sql: String) = runSqlHive(sql)
  def setOut(stream: PrintStream) = client.setOut(stream)
  def setInfo(stream: PrintStream) = client.setInfo(stream)
  def setError(stream: PrintStream) = client.setError(stream)
  def listTables(dbName: String) = client.listTables(dbName)
  def currentDatabase = client.currentDatabase
  override def getDatabase(name: String) = client.getDatabase(name)
  def getDatabaseOption(name: String) = client.getDatabaseOption(name)
  override def getTable(dbName: String, tableName: String) = client.getTable(dbName, tableName)
  def getTableOption(dbName: String, tableName: String) = client.getTableOption(dbName, tableName)
  def createView(view: HiveTable) = client.createView(view)
  def alertView(view: HiveTable) = client.alertView(view)
  def createTable(table: HiveTable) = client.createTable(table)
  def alterTable(table: HiveTable) = client.alterTable(table)
  def createDatabase(database: HiveDatabase) = client.createDatabase(database)
  def getPartitionOption(hTable: HiveTable, partitionSpec: JMap[String, String]) =
    client.getPartitionOption(hTable, partitionSpec)
  def getAllPartitions(hTable: HiveTable) = client.getAllPartitions(hTable)
  def getPartitionsByFilter(hTable: HiveTable, predicates: Seq[Expression]) =
    client.getPartitionsByFilter(hTable, predicates)
  def loadPartition(
      loadPath: String,
      tableName: String,
      partSpec: java.util.LinkedHashMap[String, String], // Hive relies on LinkedHashMap ordering
      replace: Boolean,
      holdDDLTime: Boolean,
      inheritTableSpecs: Boolean,
      isSkewedStoreAsSubdir: Boolean) =
    client.loadPartition(
      loadPath, tableName, partSpec, replace, holdDDLTime, inheritTableSpecs, isSkewedStoreAsSubdir)
  def loadTable(loadPath: String, tableName: String, replace: Boolean, holdDDLTime: Boolean) =
    client.loadTable(loadPath, tableName, replace, holdDDLTime)
  def loadDynamicPartitions(
      loadPath: String,
      tableName: String,
      partSpec: java.util.LinkedHashMap[String, String], // Hive relies on LinkedHashMap ordering
      replace: Boolean,
      numDP: Int,
      holdDDLTime: Boolean,
      listBucketingEnabled: Boolean) =
    client.loadDynamicPartitions(
      loadPath, tableName, partSpec, replace, numDP, holdDDLTime, listBucketingEnabled)
  def addJar(path: String) = client.addJar(path)
  def newSession() = client.newSession()
  def withHiveState[A](f: => A): A = client.withHiveState[A](f)
  def reset() = client.reset()
  // scalastyle:on
}

class LlapCatalog(val client2: ClientInterface, hive: LlapContext)
    extends HiveMetastoreCatalog(client2, hive) {

  override val client = client2

  override def lookupRelation(
      tableIdentifier: TableIdentifier,
      alias: Option[String] = None): LogicalPlan = {
    // Use metastore catalog to lookup tables, then convert to our relations
    val relation = super.lookupRelation(tableIdentifier, alias)
    val relationSourceName = "org.apache.spark.sql.hive.llap"

    // Now convert to LlapRelation
    val logicalRelation = relation match {
      case MetastoreRelation(dbName, tabName, alias) =>
        val qualifiedName = dbName + "." + tabName
        var options = Map("table" -> qualifiedName, "url" -> hive.getConnectionUrl())
        val resolved = ResolvedDataSource(
        hive,
        None,
        Array[String](),
        relationSourceName,
        options)
        LogicalRelation(resolved.relation)
      case _ => throw new Exception("Expected MetastoreRelation")
    }

    val tableWithQualifiers = Subquery(tableIdentifier.table, logicalRelation)
    alias.map(a => Subquery(a, tableWithQualifiers)).getOrElse(tableWithQualifiers)
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

  def getConnectionUrlFromConf(sparkContext: SparkContext): String = {
    if (!sparkContext.conf.contains(HIVESERVER2_URL.key)) {
      throw new Exception("Spark conf does not contain config " + HIVESERVER2_URL.key)
    }
    sparkContext.conf.get(HIVESERVER2_URL.key)
  }

  private[llap] val getUserMethod = findGetUserMethod()

  private def findGetUserMethod(): ru.MethodSymbol = {
    val symbol = ru.typeOf[HiveContext].declaration(ru.stringToTermName("getUser"))
    val methodSymbol = symbol match {
      case ru.NoSymbol => null
      case null => null
      case _ => symbol.asMethod
    }
    methodSymbol
  }
}

