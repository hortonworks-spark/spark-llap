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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession, SQLContext}
import org.apache.spark.sql.hive.llap.DefaultSource
import org.apache.spark.sql.sources.{BaseRelation, Filter}
import org.apache.spark.sql.types.StructType

// scalastyle:off
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}
// scalastyle:on

class TestLlapQueryExecutionListener
    // scalastyle:off funsuite
    extends FunSuite with BeforeAndAfterAll with BeforeAndAfterEach {
    // scalastyle:on funsuite

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("LlapQueryExecutionListener")
      .config(
        "spark.sql.queryExecutionListeners",
        classOf[LlapQueryExecutionListener].getCanonicalName)
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try {
      spark.stop()
    } finally {
      super.afterAll()
    }
  }

  override def afterEach(): Unit = {
    try {
      CloseCalls.clear()
    } finally {
      super.afterEach()
    }
  }

  test("Closes all LlapRelations after query executions - basic") {
    val df = spark.read.format(classOf[FakeDefaultSource].getCanonicalName).load()
    assert(CloseCalls.closeCalls == 0)
    df.collect()
    assert(CloseCalls.closeCalls == 1, "Closing LlapRelation was not attempted.")
  }

  test("Closes all LlapRelations after query executions - union") {
    val df1 = spark.read.format(classOf[FakeDefaultSource].getCanonicalName).load()
    val df2 = spark.read.format(classOf[FakeDefaultSource].getCanonicalName).load()
    val df3 = spark.read.format(classOf[FakeDefaultSource].getCanonicalName).load()
    val unionDF = df1.union(df2).union(df3)
    assert(CloseCalls.closeCalls == 0)
    unionDF.collect()
    assert(
      CloseCalls.closeCalls == 3,
      s"Closing LlapRelation should be attempted 2 but got ${CloseCalls.closeCalls}.")
  }

  test("Closes all LlapRelations after query executions - different sources") {
    val df1 = spark.range(0, 10).toDF
    val df2 = spark.read.format(classOf[FakeDefaultSource].getCanonicalName).load()
    val unionDF = df1.union(df1).union(df2)
    assert(CloseCalls.closeCalls == 0)
    unionDF.show(0)
    assert(
      CloseCalls.closeCalls == 1,
      s"Closing LlapRelation should be attempted 1 but got ${CloseCalls.closeCalls}.")
  }

  test("Closes all LlapRelations after query executions - SQL") {
    spark.sql(s"""
      |CREATE TEMPORARY TABLE tableA
      |USING ${classOf[FakeDefaultSource].getCanonicalName}
    """.stripMargin.replaceAll("\n", " "))
    val df = spark.sql("SELECT * FROM tableA")
    assert(CloseCalls.closeCalls == 0)
    df.count()
    assert(
      CloseCalls.closeCalls == 1,
      s"Closing LlapRelation should be attempted 1 but got ${CloseCalls.closeCalls}.")
  }
}

/**
 * It holds a variable to count the actual 'close' call. It should manually be cleared.
 */
object CloseCalls {
  var closeCalls: Int = 0
  def clear(): Unit = {
    closeCalls = 0
  }
}

/**
 * This FakeDefaultSource is to mock LLAP query execution.
 */
class FakeDefaultSource extends DefaultSource {
  override def createRelation(
      sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    new FakeLlapRelation(sqlContext, parameters)
  }
}

class FakeLlapRelation(sc: SQLContext, parameters: Map[String, String])
  extends LlapRelation(sc, parameters) {
  // This schema is returned as the output schema.
  override lazy val tableSchema: StructType = sc.range(10).schema

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    sc.range(10).rdd
  }

  override def close(): Unit = CloseCalls.closeCalls += 1
}
