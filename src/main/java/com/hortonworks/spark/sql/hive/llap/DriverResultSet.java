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

package com.hortonworks.spark.sql.hive.llap;

import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;


import java.util.List;

//Holder class for data return directly to the Driver from HS2
public class DriverResultSet {

    public DriverResultSet(List<Row> data, StructType schema) {
       this.data = data;
       this.schema = schema;
    }

    public List<Row> data;
    public StructType schema;

    public Dataset<Row> asDataFrame(SparkSession session) {
      return session.createDataFrame(data, schema);
    }

    // Exposed for Python side.
    public List<Row> getData() {
        return this.data;
    }
}
