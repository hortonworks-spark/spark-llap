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

import org.apache.spark.Logging
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources._


// Thanks again spark-redshift ..
private[llap] object FilterPushdown extends Object with Logging {
  def buildWhereClause(schema: StructType, filters: Seq[Filter]): String = {
    val filterExpressions = filters.flatMap(f => buildFilterExpression(schema, f)).mkString(" AND ")
    if (filterExpressions.isEmpty) "" else "WHERE " + filterExpressions
  }

  /**
   * Attempt to convert the given filter into a SQL expression. Returns None if the expression
   * could not be converted.
   */
  private def buildFilterExpression(schema: StructType, filter: Filter): Option[String] = {
    def buildComparison(attr: String, value: Any, comparisonOp: String): Option[String] = {
     getTypeForAttribute(schema, attr).map { dataType =>
       val sqlEscapedValue: String = getSqlEscapedValue(dataType, value)
       s"""$attr $comparisonOp $sqlEscapedValue"""
     }
    }

    def buildInClause(attr: String, values: Array[Any]): Option[String] = {
      getTypeForAttribute(schema, attr).map { dataType =>
        val valuesList = values.map(value => getSqlEscapedValue(dataType, value)).mkString(",")
        s"""$attr IN ($valuesList)"""
      }
    }

    filter match {
      case EqualTo(attr, value) => buildComparison(attr, value, "=")
      case LessThan(attr, value) => buildComparison(attr, value, "<")
      case GreaterThan(attr, value) => buildComparison(attr, value, ">")
      case LessThanOrEqual(attr, value) => buildComparison(attr, value, "<=")
      case GreaterThanOrEqual(attr, value) => buildComparison(attr, value, ">=")
      case In(attr, values) => buildInClause(attr, values)
      case _ => {
        logDebug("Unable to generate SQL expression for SparkSQL Filter: " + filter)
        None
      }
    }
  }

  private def getSqlEscapedValue(dataType: DataType, value: Any): String = {
    dataType match {
         case StringType => s"\\'${value.toString.replace("'", "\\'\\'")}\\'"
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