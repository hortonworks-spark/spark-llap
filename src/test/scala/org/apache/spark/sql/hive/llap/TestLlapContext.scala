package org.apache.spark.sql.hive.llap

import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.apache.spark.{SparkContext, SparkException}
import org.scalatest.{BeforeAndAfterAll, FunSuite}


class TestLlapContext extends FunSuite with BeforeAndAfterAll {

  private var jdbcUrl =  "jdbc:hive2://localhost:10000"
  private val sparkContext = new SparkContext("local", "test")
  private var llapContext = LlapContext.newInstance(sparkContext, jdbcUrl)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    try {
    } finally {
      super.afterAll()
    }
  }

  test("Catalog") {
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

  test("simple query") {
    var df = llapContext.sql("select count(*) from employee")
    var rows = df.collect
    assert(rows(0)(0) == 1155)
  }

  test("filtered colums") {
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
}
