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

package com.hortonworks.hwc;

import com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilder;
import org.apache.spark.sql.SparkSession;

public interface HiveWarehouseSession extends com.hortonworks.spark.sql.hive.llap.HiveWarehouseSession {
    String HIVE_WAREHOUSE_CONNECTOR = "com.hortonworks.spark.sql.hive.llap.HiveWarehouseConnector";
    String DATAFRAME_TO_STREAM = "com.hortonworks.spark.sql.hive.llap.HiveStreamingDataSource";
    String STREAM_TO_STREAM = "com.hortonworks.spark.sql.hive.llap.streaming.HiveStreamingDataSource";

    static HiveWarehouseBuilder session(SparkSession session) {
        return HiveWarehouseBuilder.session(session);
    }
}
