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

package com.hortonworks.spark.hive.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * A Hive Streaming example to ingest data from socket and push into hive table.
  *
  * Assumed HIVE table Schema:
  * create table alerts ( id int , msg string )
  *    partitioned by (continent string, country string)
  *    clustered by (id) into 5 buckets
  *    stored as orc tblproperties("transactional"="true");
  */
object TestHiveStreaming {

  def main(args: Array[String]): Unit = {
    if (args.length < 3 || args.length > 5) {
      // scalastyle:off println
      System.err.println(s"Usage: HiveStreamingExample <socket host> <socket port>" +
        s" <metastore uri> [principal] [keytab]")
      // scalastyle:on println
      System.exit(1)
    }

    val host = args(0)
    val port = args(1)
    val metastoreUri = args(2)

    val (principal, keytab) = if (args.length == 5) {
      (args(3), args(4))
    } else {
      (null, null)
    }

    val sparkConf = new SparkConf()
      .set("spark.sql.streaming.checkpointLocation", "./checkpoint")
    val sparkSession = SparkSession.builder()
      .appName("HiveStreamingExample")
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    import sparkSession.implicits._

    val socket = sparkSession.readStream
      .format("socket")
      .options(Map("host" -> host, "port" -> port))
      .load()
      .as[String]

    val writer = socket.map { s =>
      val records = s.split(",")
      assert(records.length >= 4)
      (records(0).toInt, records(1), records(2), records(3))
    }
      .selectExpr("_1 as id", "_2 as msg", "_3 as continent", "_4 as country")
      .writeStream
      .format("hive-streaming")
      .option("metastore", metastoreUri)
      .option("db", "default")
      .option("table", "alerts")

    if (principal != null && keytab != null) {
      writer.option("principal", principal)
        .option("keytab", keytab)
    }

    val query = writer.start()
    query.awaitTermination()

    query.stop()
    sparkSession.stop()
  }
}

