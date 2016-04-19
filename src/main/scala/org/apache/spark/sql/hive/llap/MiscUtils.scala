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

import org.apache.spark.sql.types._

import java.util.Properties
import org.apache.hadoop.io.Text
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.CatalystTypeConverters

object DataTypeUtils {

  def parse(dataTypeString: String): DataType = {
    org.apache.spark.sql.types.DataTypeParser.parse(dataTypeString)
  }
}

// Simple parser to convert delimited text to row
class TextRowParser(colNames: Seq[String], colTypes: Seq[DataType], props: Properties) {

  val fieldDelimiter = props.getProperty("field.delim", "\001")
  var firstFieldLengthWarning = false

  def parseRow(textRow: Text): Row = {
    val fields = textRow.toString().split(fieldDelimiter)
    if (fields.length != colNames.length) {
      if (!firstFieldLengthWarning) {
        println("*** Expected " + colNames.length + " fields, but row has " + fields.length)
        println("*** colNames = " + colNames)
        println("*** fields = " + fields)
        firstFieldLengthWarning = true
      }
    }

    Row.fromSeq({
      var idx = 0
      colTypes.map(colType => {
        val fieldValue = {
          if (idx >= fields.length) {
            null
          } else {
            SerdeUtils.parseValue(fields(idx), colType)
          }
        }
        idx += 1
        fieldValue
      })
    })
  }
}

object SerdeUtils {

  def parseValue(stringValue:String, colType: DataType): Any = {
    if (stringValue == null || stringValue == "\\N") {
      return null
    }
    try {
      colType match {
        case StringType => stringValue
        case IntegerType =>  stringValue.toInt
        case LongType => stringValue.toLong
        case FloatType => stringValue.toFloat
        case DoubleType => stringValue.toDouble
        case DecimalType.Fixed(precision, scale) => Decimal(Decimal(stringValue).toBigDecimal, precision, scale)
        case BooleanType => stringValue.toBoolean
        case DateType => java.sql.Date.valueOf(stringValue)
        case TimestampType => java.sql.Timestamp.valueOf(stringValue)
        case _ => null
      }
    } catch {
      case e: Exception => null
    }
  }
}
