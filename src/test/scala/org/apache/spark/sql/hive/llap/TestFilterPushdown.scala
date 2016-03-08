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

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import java.sql.Date
import java.sql.Timestamp

class TestFilterPushdown extends FunSuite with BeforeAndAfterAll {
  private var employeeSchema = StructType(Seq(
      StructField("employee_id",IntegerType,true),
      StructField("full_name",StringType,true),
      StructField("first_name",StringType,true),
      StructField("last_name",StringType,true),
      StructField("position_id",IntegerType,true),
      StructField("position_title",StringType,true),
      StructField("store_id",IntegerType,true),
      StructField("department_id",IntegerType,true),
      StructField("birth_date",DateType,true),
      StructField("hire_date",TimestampType,true),
      StructField("end_date",TimestampType,true),
      StructField("salary",DecimalType(10,4),true),
      StructField("supervisor_id",IntegerType,true),
      StructField("education_level",StringType,true),
      StructField("marital_status",StringType,true),
      StructField("gender",StringType,true),
      StructField("management_role",StringType,true)))

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    try {
    } finally {
      super.afterAll()
    }
  }

  test("where") {
    var expr = FilterPushdown.buildWhereClause(employeeSchema, Nil)
    assert("" === expr)

    expr = FilterPushdown.buildWhereClause(employeeSchema, List())
    assert("" === expr)

    expr = FilterPushdown.buildWhereClause(
        employeeSchema,
        List(new EqualTo("employee_id", 88)))
    assert("WHERE employee_id = 88" === expr)

    expr = FilterPushdown.buildWhereClause(
        employeeSchema,
        List(new EqualTo("employee_id", 88), new EqualTo("first_name", "Mike")))
    assert("WHERE employee_id = 88 AND first_name = 'Mike'" === expr)
  }

  test("String escapes") {
    checkFilter(employeeSchema,
        new EqualTo("first_name", "Mike's"),
        "first_name = 'Mike\\'s'")
  }

  test("equalTo") {
    checkFilter(employeeSchema,
        new EqualTo("employee_id", 88),
        "employee_id = 88")

    checkFilter(employeeSchema,
        new EqualTo("first_name", "Mike"),
        "first_name = 'Mike'")

    checkFilter(employeeSchema,
        new EqualTo("hire_date", Timestamp.valueOf("2001-02-03 04:05:06.123")),
        "hire_date = TIMESTAMP '2001-02-03 04:05:06.123'")

    checkFilter(employeeSchema,
        new EqualTo("birth_date", Date.valueOf("1961-08-26")),
        "birth_date = DATE '1961-08-26'")
  }

  test("gt") {
    checkFilter(employeeSchema,
        new GreaterThan("employee_id", 88),
        "employee_id > 88")
  }

  test("gte") {
    checkFilter(employeeSchema,
        new GreaterThanOrEqual("employee_id", 88),
        "employee_id >= 88")
  }

  test("lt") {
    checkFilter(employeeSchema,
        new LessThan("employee_id", 88),
        "employee_id < 88")
  }

  test("lte") {
    checkFilter(employeeSchema,
        new LessThanOrEqual("employee_id", 88),
        "employee_id <= 88")
  }

  test("in") {
    checkFilter(employeeSchema,
        new In("employee_id", Array(88, 89, 90)),
        "employee_id IN (88,89,90)")
  }

  test("string starts with") {
    checkFilter(employeeSchema,
        new StringStartsWith("management_role", "val"),
        "management_role LIKE 'val%'")
  }

  test("string ends with") {
    checkFilter(employeeSchema,
        new StringEndsWith("management_role", "val"),
        "management_role LIKE '%val'")
  }

  test("string contains") {
    checkFilter(employeeSchema,
        new StringContains("management_role", "val"),
        "management_role LIKE '%val%'")
  }

  test("is null") {
    checkFilter(employeeSchema,
        new IsNull("employee_id"),
        "employee_id IS NULL")
  }

  test("is not null") {
    checkFilter(employeeSchema,
        new IsNotNull("employee_id"),
        "employee_id IS NOT NULL")
  }

  test("not") {
    checkFilter(employeeSchema,
        new Not(new IsNotNull("employee_id")),
        "NOT (employee_id IS NOT NULL)")
  }

  test("and") {
    checkFilter(employeeSchema,
        new And(new IsNotNull("employee_id"), new LessThanOrEqual("employee_id", 88)),
        "(employee_id IS NOT NULL) AND (employee_id <= 88)")
  }

  test("or") {
    checkFilter(employeeSchema,
        new Or(new IsNotNull("employee_id"), new LessThanOrEqual("employee_id", 88)),
        "(employee_id IS NOT NULL) OR (employee_id <= 88)")
  }

  test("nested logical") {
    checkFilter(employeeSchema,
        new And(
            new Not(new In("employee_id", Array(88, 89, 90))),
            new Or(new IsNotNull("employee_id"), new LessThanOrEqual("employee_id", 88))),
        "(NOT (employee_id IN (88,89,90))) AND ((employee_id IS NOT NULL) OR (employee_id <= 88))")
  }

  def checkFilter(schema: StructType, filter: Filter, expected: String) = {
    val expr = FilterPushdown.buildFilterExpression(schema, filter)
    expected match {
      case null => assert(expr.isEmpty)
      case _ => assert(expected === expr.get)
    }
  }
}