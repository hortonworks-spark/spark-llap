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

import collection.JavaConverters._
import org.apache.hadoop.hive.llap.{Schema, TypeDesc}
import org.apache.hadoop.hive.llap.TypeDesc.Type
import org.apache.hadoop.hive.serde2.io.{ByteWritable, DateWritable, DoubleWritable, HiveDecimalWritable,
ShortWritable, TimestampWritable}
import org.apache.hadoop.io.{BooleanWritable, BytesWritable, FloatWritable, IntWritable, LongWritable, Text}

import org.apache.spark.sql.Row


object RowConverter {

  def llapRowToSparkRow(llapRow: org.apache.hadoop.hive.llap.Row, schema: Schema): Row = {

    Row.fromSeq({
      var idx = 0
      schema.getColumns.asScala.map(colDesc => {
        val sparkValue = convertValue(llapRow.getValue(idx), colDesc.getTypeDesc)
        idx += 1
        sparkValue
      })
    })
  }

  private def convertValue(value: Any, colType: TypeDesc): Any = {
    val t = colType.getType
    t match {
      // The primitives should not require conversion
      case Type.BOOLEAN   => value.asInstanceOf[Boolean]
      case Type.TINYINT   => value.asInstanceOf[Byte]
      case Type.SMALLINT  => value.asInstanceOf[Short]
      case Type.INT       => value.asInstanceOf[Integer]
      case Type.BIGINT    => value.asInstanceOf[Long]
      case Type.FLOAT     => value.asInstanceOf[Float]
      case Type.DOUBLE    => value.asInstanceOf[Double]
      case Type.STRING    => value.asInstanceOf[String]
      case Type.CHAR      => value.asInstanceOf[String]
      case Type.VARCHAR   => value.asInstanceOf[String]
      case Type.DATE      => value.asInstanceOf[java.sql.Date]
      case Type.TIMESTAMP => value.asInstanceOf[java.sql.Timestamp]
      case Type.BINARY    => value.asInstanceOf[Array[Byte]]
      case Type.DECIMAL   => value.asInstanceOf[BigDecimal]
      // Complex types require conversion
      case Type.LIST      => value.asInstanceOf[java.util.List[Any]].asScala.map(
        listElement => convertValue(listElement, colType.getListElementTypeDesc))
      case Type.MAP       => {
        // Try LinkedHashMap to preserve order of elements - is that necessary?
        var convertedMap = scala.collection.mutable.LinkedHashMap.empty[Any, Any]
        value.asInstanceOf[java.util.Map[Any, Any]].asScala.foreach((tuple) =>
          convertedMap(convertValue(tuple._1, colType.getMapKeyTypeDesc)) =
            convertValue(tuple._2, colType.getMapValueTypeDesc))
        convertedMap
      }
      case Type.STRUCT    => llapRowToSparkRow(
        value.asInstanceOf[org.apache.hadoop.hive.llap.Row],
        colType.getStructSchema)
      case _ => null
    }
  }
}
