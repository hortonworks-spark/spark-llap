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

import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.apache.spark.{SparkContext, SparkException}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import java.net.InetAddress


class TestLlapContext extends FunSuite with BeforeAndAfterAll {

  private var jdbcUrl =  "jdbc:hive2://localhost:10000"
  TestUtils.sparkContext.hadoopConfiguration.set(LlapContext.HIVESERVER2_URL.key, jdbcUrl)
  private var llapContext: LlapContext = null

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    // Because LlapContext now inherits from HiveContext, LlapContext unit tests no longer work.
    // LlapContext must be used with the assembly JAR.
    /*
    llapContext = new LlapContext(TestUtils.sparkContext)
    // Assume this test is running against MiniLlapCluster.
    TestUtils.updateConfWithMiniClusterSettings(jdbcUrl, System.getProperty("user.name"))
    */
  }

  override protected def afterAll(): Unit = {
    try {
    } finally {
      super.afterAll()
    }
  }

  ignore("Catalog") {
    var foundEmployeeTable = false;
    var foundSalaryTable = false;

    var tables = llapContext.tables
    var tableRows = tables.collect
    for (row <- tableRows) {
      if (row(0).toString().toLowerCase() == "employee") {
        foundEmployeeTable = true
      } else if (row(0).toString().toLowerCase() == "salary") {
        foundSalaryTable = true
      }
    }
    assert(foundEmployeeTable)
    assert(foundSalaryTable)
  }

  ignore("simple query") {
    var df = llapContext.sql("select count(*) from employee")
    var rows = df.collect
    assert(rows(0)(0) == 1155)
  }

  ignore("filtered colums") {
    // Also test case-insensitive column names
    var df = llapContext.sql("select Last_Name, first_name from EMPLOYEE order by last_name, first_name limit 10")
    var rows = df.collect
    assert(rows(0).length == 2)
    assert(rows.length == 10)

    assert(rows(0)(0) == "Abbott")
    assert(rows(0)(1) == "Eric")

    assert(rows(9)(0) == "Adams")
    assert(rows(9)(1) == "Michelle")
  }

  ignore("filters1") {
    var df = llapContext.sql("select last_name, first_name from employee where employee_id > 1 and employee_id < 5")
    var rows = df.collect
    assert(rows(0).length == 2)
    assert(rows.length == 2)

    assert(rows(0)(0) == "Whelply")
    assert(rows(0)(1) == "Derrick")

    assert(rows(1)(0) == "Spence")
    assert(rows(1)(1) == "Michael")
  }

  ignore("filters_in1") {
    // IN filter
    var df = llapContext.sql("select last_name, first_name from employee where employee_id in (2,3,4)")
    var rows = df.collect
    assert(rows(0).length == 2)
    assert(rows.length == 2)

    assert(rows(0)(0) == "Whelply")
    assert(rows(0)(1) == "Derrick")

    assert(rows(1)(0) == "Spence")
    assert(rows(1)(1) == "Michael")
  }

  ignore("filters_in2") {
    // IN filter
    var df = llapContext.sql("select last_name, first_name from employee where first_name in ('Derrick') and last_name in ('Whelply')")
    var rows = df.collect
    assert(rows(0).length == 2)
    assert(rows.length == 1)

    assert(rows(0)(0) == "Whelply")
    assert(rows(0)(1) == "Derrick")
  }
}
