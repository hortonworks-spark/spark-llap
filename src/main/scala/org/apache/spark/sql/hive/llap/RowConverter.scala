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

import collection.JavaConversions._

import org.apache.spark.sql.Row

import org.apache.hadoop.hive.llap.FieldDesc
import org.apache.hadoop.hive.llap.Schema
import org.apache.hadoop.hive.llap.TypeDesc
import org.apache.hadoop.hive.llap.TypeDesc.Type

// TODO: prefer hadoop writables over the hive equivalents? This needs to be fixed in the LLAP Row.
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.mapred.InputSplit

object RowConverter {

  def llapRowToSparkRow(llapRow: org.apache.hadoop.hive.llap.Row, schema: Schema): Row = {
    Row.fromSeq({
      var idx = 0
      schema.getColumns.map(colDesc => {
        val sparkValue = convertValue(llapRow.getValue(idx), colDesc.getTypeDesc)
        idx += 1
        sparkValue
      })
    })
  }

  def convertValue(value: Any, colType: TypeDesc): Any = {
    try {
      val t = colType.getType
      t match {
        case Type.BOOLEAN => value.asInstanceOf[BooleanWritable].get()
        case Type.TINYINT => value.asInstanceOf[ByteWritable].get()
        case Type.SMALLINT => value.asInstanceOf[ShortWritable].get()
        case Type.INT => value.asInstanceOf[IntWritable].get()
        case Type.BIGINT => value.asInstanceOf[LongWritable].get()
        case Type.FLOAT => value.asInstanceOf[FloatWritable].get()
        case Type.DOUBLE => value.asInstanceOf[DoubleWritable].get()
        case Type.STRING => value.asInstanceOf[Text].toString()
        case Type.CHAR => value.asInstanceOf[Text].toString()
        case Type.VARCHAR => value.asInstanceOf[Text].toString()
        case Type.DATE => value.asInstanceOf[DateWritable].get()
        case Type.TIMESTAMP => value.asInstanceOf[TimestampWritable].getTimestamp()
        case Type.BINARY => value.asInstanceOf[BytesWritable].getBytes()
        case Type.DECIMAL => value.asInstanceOf[HiveDecimalWritable].getHiveDecimal().bigDecimalValue()
        case _ => null
      }
    } catch {
      case _: Throwable => null
    }
  }
}