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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface HiveWarehouseSession {

    String HIVE_WAREHOUSE_CONNECTOR = "com.hortonworks.spark.sql.hive.llap.HiveWarehouseConnector";
    String SPARK_DATASOURCES_PREFIX = "spark.datasource";
    String HIVE_WAREHOUSE_POSTFIX = "hive.warehouse";
    String CONF_PREFIX = SPARK_DATASOURCES_PREFIX + "." + HIVE_WAREHOUSE_POSTFIX;

    Dataset<Row> executeQuery(String sql);
    Dataset<Row> q(String sql);

    Dataset<Row> execute(String sql);

    boolean executeUpdate(String sql);

    Dataset<Row> table(String sql);

    SparkSession session();

    void setDatabase(String name);

    Dataset<Row> showDatabases();

    Dataset<Row> showTables();

    Dataset<Row> describeTable(String table);

    void createDatabase(String database, boolean ifNotExists);

    CreateTableBuilder createTable(String tableName);

    void dropDatabase(String database, boolean ifExists, boolean cascade);

    void dropTable(String table, boolean ifExists, boolean purge);
}
