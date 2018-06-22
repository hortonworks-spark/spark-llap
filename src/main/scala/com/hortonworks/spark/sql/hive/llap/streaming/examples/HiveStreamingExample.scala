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

package com.hortonworks.spark.sql.hive.llap.streaming.examples

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/*
 * A Hive Streaming example to ingest data from socket and push into hive table.
 *
 * Assumed HIVE table Schema:
 * CREATE TABLE `streaming.web_sales`(
 * `ws_sold_date_sk` int,
 * `ws_sold_time_sk` int,
 * `ws_ship_date_sk` int,
 * `ws_item_sk` int,
 * `ws_bill_customer_sk` int,
 * `ws_bill_cdemo_sk` int,
 * `ws_bill_hdemo_sk` int,
 * `ws_bill_addr_sk` int,
 * `ws_ship_customer_sk` int,
 * `ws_ship_cdemo_sk` int,
 * `ws_ship_hdemo_sk` int,
 * `ws_ship_addr_sk` int,
 * `ws_web_page_sk` int,
 * `ws_web_site_sk` int,
 * `ws_ship_mode_sk` int,
 * `ws_warehouse_sk` int,
 * `ws_promo_sk` int,
 * `ws_order_number` int,
 * `ws_quantity` int,
 * `ws_wholesale_cost` float,
 * `ws_list_price` float,
 * `ws_sales_price` float,
 * `ws_ext_discount_amt` float,
 * `ws_ext_sales_price` float,
 * `ws_ext_wholesale_cost` float,
 * `ws_ext_list_price` float,
 * `ws_ext_tax` float,
 * `ws_coupon_amt` float,
 * `ws_ext_ship_cost` float,
 * `ws_net_paid` float,
 * `ws_net_paid_inc_tax` float,
 * `ws_net_paid_inc_ship` float,
 * `ws_net_paid_inc_ship_tax` float,
 * `ws_net_profit` float)
 *  PARTITIONED BY (
 *  `ws_sold_date` string)
 *  STORED AS ORC
 *  TBLPROPERTIES ('transactional'='true')
 */
case class Schema(ws_sold_date_sk: Int, ws_sold_time_sk: Int, ws_ship_date_sk: Int,
                  ws_item_sk: Int, ws_bill_customer_sk: Int, ws_bill_cdemo_sk: Int,
                  ws_bill_hdemo_sk: Int, ws_bill_addr_sk: Int, ws_ship_customer_sk: Int,
                  ws_ship_cdemo_sk: Int, ws_ship_hdemo_sk: Int, ws_ship_addr_sk: Int,
                  ws_web_page_sk: Int, ws_web_site_sk: Int, ws_ship_mode_sk: Int,
                  ws_warehouse_sk: Int, ws_promo_sk: Int, ws_order_number: Int,
                  ws_quantity: Int, ws_wholesale_cost: Float, ws_list_price: Float,
                  ws_sales_price: Float, ws_ext_discount_amt: Float, ws_ext_sales_price: Float,
                  ws_ext_wholesale_cost: Float, ws_ext_list_price: Float, ws_ext_tax: Float,
                  ws_coupon_amt: Float, ws_ext_ship_cost: Float, ws_net_paid: Float,
                  ws_net_paid_inc_tax: Float, ws_net_paid_inc_ship: Float,
                  ws_net_paid_inc_ship_tax: Float, ws_net_profit: Float, ws_sold_date: String)

object HiveStreamingExample {

  def main(args: Array[String]): Unit = {
    if (args.length < 3 || args.length > 5) {
      // scalastyle:off println
      System.err.println(s"Usage: HiveStreamingExample <socket host> <socket port>" +
        s" <metastore uri> <metastore kerberos principal>")
      // scalastyle:on println
      System.exit(1)
    }

    val host = args(0)
    val port = args(1)
    val metastoreUri = args(2)
    var metastoreKrbPrincipal: String = null
    if (args.length > 3) {
      metastoreKrbPrincipal = args(3)
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

    val writer =
      socket.map { s =>
        val x = s.split(",")
        Schema(x(0).toInt, x(1).toInt, x(2).toInt, x(3).toInt, x(4).toInt, x(5).toInt,
          x(6).toInt, x(7).toInt, x(8).toInt, x(9).toInt, x(10).toInt, x(11).toInt,
          x(12).toInt, x(13).toInt, x(14).toInt, x(15).toInt, x(16).toInt, x(17).toInt,
          x(18).toInt, x(19).toFloat, x(20).toFloat, x(21).toFloat, x(22).toFloat,
          x(23).toFloat, x(24).toFloat, x(25).toFloat, x(26).toFloat, x(27).toFloat,
          x(28).toFloat, x(29).toFloat, x(30).toFloat, x(31).toFloat, x(32).toFloat,
          x(33).toFloat, x(34))
      }
        .writeStream
        .format("com.hortonworks.spark.sql.hive.llap.streaming.HiveStreamingDataSource")
        .option("metastoreUri", metastoreUri)
        .option("database", "streaming")
        .option("table", "web_sales")

    if (metastoreKrbPrincipal != null) {
      writer.option("metastoreKrbPrincipal", metastoreKrbPrincipal)
    }

    // before this, a new terminal that runs 'nc -l <port>' has to be started and
    // csv records for web_sales table has to be pasted so that spark streaming
    // can read the rows from nc and pass it on to hive data source
    val query = writer.start()
    query.awaitTermination()

    query.stop()
    sparkSession.stop()
  }
}