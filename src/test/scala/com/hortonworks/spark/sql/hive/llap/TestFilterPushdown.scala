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

import java.sql.Date
import java.sql.Timestamp

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

// scalastyle:off
import org.scalatest.FunSuite
// scalastyle:on

// scalastyle:off
class TestFilterPushdown extends FunSuite {
// scalastyle:on
  private val employeeSchema = StructType(Seq(
    StructField("employee_id", IntegerType, nullable = true),
    StructField("full_name", StringType, nullable = true),
    StructField("first_name", StringType, nullable = true),
    StructField("last_name", StringType, nullable = true),
    StructField("position_id", IntegerType, nullable = true),
    StructField("position_title", StringType, nullable = true),
    StructField("store_id", IntegerType, nullable = true),
    StructField("department_id", IntegerType, nullable = true),
    StructField("birth_date", DateType, nullable = true),
    StructField("hire_date", TimestampType, nullable = true),
    StructField("end_date", TimestampType, nullable = true),
    StructField("salary", DecimalType(10, 4), nullable = true),
    StructField("supervisor_id", IntegerType, nullable = true),
    StructField("education_level", StringType, nullable = true),
    StructField("marital_status", StringType, nullable = true),
    StructField("gender", StringType, nullable = true),
    StructField("management_role", StringType, nullable = true)))

  test("where") {
    var expr = FilterPushdown.buildWhereClause(employeeSchema, Nil)
    assert("" === expr)

    expr = FilterPushdown.buildWhereClause(employeeSchema, List())
    assert("" === expr)

    expr = FilterPushdown.buildWhereClause(
      employeeSchema,
      List(EqualTo("employee_id", 88)))
    assert("WHERE employee_id = 88" === expr)

    expr = FilterPushdown.buildWhereClause(
      employeeSchema,
      List(EqualTo("employee_id", 88), new EqualTo("first_name", "Mike")))
    assert("WHERE employee_id = 88 AND first_name = 'Mike'" === expr)
  }

  test("String escapes") {
    checkFilter(employeeSchema,
      EqualTo("first_name", "Mike's"),
      "first_name = 'Mike\\'s'")
  }

  test("equalTo") {
    checkFilter(employeeSchema,
      EqualTo("employee_id", 88),
      "employee_id = 88")

    checkFilter(employeeSchema,
      EqualTo("first_name", "Mike"),
      "first_name = 'Mike'")

    checkFilter(employeeSchema,
      EqualTo("hire_date", Timestamp.valueOf("2001-02-03 04:05:06.123")),
      "hire_date = TIMESTAMP '2001-02-03 04:05:06.123'")

    checkFilter(employeeSchema,
      EqualTo("birth_date", Date.valueOf("1961-08-26")),
      "birth_date = DATE '1961-08-26'")
  }

  test("gt") {
    checkFilter(employeeSchema,
      GreaterThan("employee_id", 88),
      "employee_id > 88")
  }

  test("gte") {
    checkFilter(employeeSchema,
      GreaterThanOrEqual("employee_id", 88),
      "employee_id >= 88")
  }

  test("lt") {
    checkFilter(employeeSchema,
      LessThan("employee_id", 88),
      "employee_id < 88")
  }

  test("lte") {
    checkFilter(employeeSchema,
      LessThanOrEqual("employee_id", 88),
      "employee_id <= 88")
  }

  test("in") {
    checkFilter(employeeSchema,
      In("employee_id", Array(88, 89, 90)),
      "employee_id IN (88,89,90)")
  }

  test("string starts with") {
    checkFilter(employeeSchema,
      StringStartsWith("management_role", "val"),
      "management_role LIKE 'val%'")
  }

  test("string ends with") {
    checkFilter(employeeSchema,
      StringEndsWith("management_role", "val"),
      "management_role LIKE '%val'")
  }

  test("string contains") {
    checkFilter(employeeSchema,
      StringContains("management_role", "val"),
      "management_role LIKE '%val%'")
  }

  test("is null") {
    checkFilter(employeeSchema,
      IsNull("employee_id"),
      "employee_id IS NULL")
  }

  test("is not null") {
    checkFilter(employeeSchema,
      IsNotNull("employee_id"),
      "employee_id IS NOT NULL")
  }

  test("not") {
    checkFilter(employeeSchema,
      Not(IsNotNull("employee_id")),
      "NOT (employee_id IS NOT NULL)")
  }

  test("and") {
    checkFilter(employeeSchema,
      And(IsNotNull("employee_id"), LessThanOrEqual("employee_id", 88)),
      "(employee_id IS NOT NULL) AND (employee_id <= 88)")
  }

  test("or") {
    checkFilter(employeeSchema,
      Or(IsNotNull("employee_id"), LessThanOrEqual("employee_id", 88)),
      "(employee_id IS NOT NULL) OR (employee_id <= 88)")
  }

  test("nested logical") {
    checkFilter(employeeSchema,
      And(
        Not(In("employee_id", Array(88, 89, 90))),
        Or(IsNotNull("employee_id"), LessThanOrEqual("employee_id", 88))),
      "(NOT (employee_id IN (88,89,90))) AND ((employee_id IS NOT NULL) OR (employee_id <= 88))")
  }

  private def checkFilter(schema: StructType, filter: Filter, expected: String) = {
    val expr = FilterPushdown.buildFilterExpression(schema, filter)
    expected match {
      case null => assert(expr.isEmpty)
      case _ => assert(expected === expr.get)
    }
  }
}
