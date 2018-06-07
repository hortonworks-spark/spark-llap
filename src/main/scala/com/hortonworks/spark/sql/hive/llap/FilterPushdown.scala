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

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataType, DateType, StringType, StructType, TimestampType}


private[llap] object FilterPushdown extends Object {
  def buildWhereClause(schema: StructType, filters: Seq[Filter]): String = {
    val filterExpressions = filters.flatMap(f => buildFilterExpression(schema, f)).mkString(" AND ")
    if (filterExpressions.isEmpty) "" else "WHERE " + filterExpressions
  }

  def supportedFilter(filter: Filter) : Boolean = filter match {
    case  EqualTo(_, _) |
          EqualNullSafe(_, _) |
          LessThan (_, _) |
          GreaterThan (_, _) |
          LessThanOrEqual (_, _) |
          GreaterThanOrEqual (_, _) |
          In (_, _) |
          StringStartsWith (_, _) |
          StringEndsWith (_, _) |
          StringContains (_, _) |
          IsNull (_) |
          IsNotNull (_) |
          And (_, _) |
          Or (_, _) |
          Not (_) => true
    case _ => false
  }

  /**
   * Attempt to convert the given filter into a SQL expression. Returns None if the expression
   * could not be converted.
   */
  def buildFilterExpression(schema: StructType, filter: Filter): Option[String] = {
    def buildComparison(attr: String, value: Any, comparisonOp: String): Option[String] = {
      getTypeForAttribute(schema, attr).map { dataType =>
        val sqlEscapedValue: String = getSqlEscapedValue(dataType, value)
        s"""$attr $comparisonOp $sqlEscapedValue"""
      }
    }

    def buildInClause(attr: String, values: Array[Any]): Option[String] = {
      if (values.isEmpty) {
        // See SPARK-18436. Values in `IN` can be empty but Hive can't handle this.
        return Some(s"CASE WHEN ${attr} IS NULL THEN NULL ELSE FALSE END")
      }

      getTypeForAttribute(schema, attr).map { dataType =>
        val valuesList = values.map(value => getSqlEscapedValue(dataType, value)).mkString(",")
        s"""$attr IN ($valuesList)"""
      }
    }

    def buildNullCheck(attr: String, isNull: Boolean): Option[String] = {
      val check = if (isNull) " IS NULL" else " IS NOT NULL"
      Option(attr + check)
    }

    def buildLogicalExpr(left: Filter, right: Filter, op: String): Option[String] = {
      val leftExpr = buildFilterExpression(schema, left)
      val rightExpr = buildFilterExpression(schema, right)
      if (leftExpr.isEmpty || rightExpr.isEmpty) {
        return None
      }
      Option(s"(${leftExpr.get}) ${op} (${rightExpr.get})")
    }

    def buildNotExpr(operand: Filter): Option[String] = {
      val expr = buildFilterExpression(schema, operand)
      if (expr.isEmpty) {
        return None
      }
      Option(s"NOT (${expr.get})")
    }

    val filterExpression: Option[String] = filter match {
      case EqualTo(attr, value) => buildComparison(attr, value, "=")
      case EqualNullSafe(attr, value) => buildComparison(attr, value, "<=>")
      case LessThan(attr, value) => buildComparison(attr, value, "<")
      case GreaterThan(attr, value) => buildComparison(attr, value, ">")
      case LessThanOrEqual(attr, value) => buildComparison(attr, value, "<=")
      case GreaterThanOrEqual(attr, value) => buildComparison(attr, value, ">=")
      case In(attr, values) => buildInClause(attr, values)
      case StringStartsWith(attr, value: String) => buildComparison(attr, value + "%", "LIKE")
      case StringEndsWith(attr, value: String) => buildComparison(attr, "%" + value, "LIKE")
      case StringContains(attr, value: String) => buildComparison(attr, "%" + value + "%", "LIKE")
      case IsNull(attr) => buildNullCheck(attr, true)
      case IsNotNull(attr) => buildNullCheck(attr, false)
      case And(left, right) => buildLogicalExpr(left, right, "AND")
      case Or(left, right) => buildLogicalExpr(left, right, "OR")
      case Not(value) => buildNotExpr(value)
      case _ => None
    }

    filterExpression
  }

  private def getSqlEscapedValue(dataType: DataType, value: Any): String = {
    dataType match {
         case StringType => s"'${value.toString.replace("'", "\\'")}'"
         case TimestampType => s"TIMESTAMP '${value.toString()}'"
         case DateType => s"DATE '${value.toString()}'"
         case _ => value.toString
    }
  }

  private def getTypeForAttribute(schema: StructType, attribute: String): Option[DataType] = {
    if (schema.fieldNames.contains(attribute)) {
      Some(schema(attribute).dataType)
    } else {
      None
    }
  }
}
