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

package com.hortonworks.spark.sql.hive.llap.util;

import java.util.UUID;

import static java.lang.String.format;

public class HiveQlUtil {

  public static String projections(String[] columns) {
    return "`" + String.join("` , `", columns) + "`";
  }

  public static String selectStar(String database, String table) {
    return format("SELECT * FROM %.%", database, table);
  }

  public static String selectStar(String table) {
    return format("SELECT * FROM %s", table);
  }

  public static String selectProjectAliasFilter(String projections, String table, String alias, String whereClause) {
    return format("select %s from (%s) as %s %s", projections, table, alias, whereClause);
  }
    public static String useDatabase(String database) {
        return format("USE %s", database);
    }

    public static String showDatabases() {
        return "SHOW DATABASES";
    }

    public static String showTables(String database) {
        return format("SHOW TABLES IN %s", database);
    }

    public static String describeTable(String database, String sql) {
        return format("DESCRIBE %s.%s", database, sql);
    }

    public static String dropDatabase(String database, boolean ifExists, boolean cascade) {
        return format("DROP %s %s %s",
                database,
                orBlank(ifExists, "IF EXISTS"),
                orBlank(cascade, "CASCADE"));
    }

    //Requires jdbc Connection attached to current database (see HiveWarehouseSessionImpl.dropTable)
    public static String dropTable(String table, boolean ifExists, boolean purge) {
        return format("DROP TABLE %s %s %s",
                orBlank(ifExists, "IF EXISTS"),
                table,
                orBlank(purge, "PURGE"));
    }

    public static String createDatabase(String database, boolean ifNotExists) {
        String x = format("CREATE DATABASE %s %s",
                orBlank(ifNotExists, "IF NOT EXISTS"),
                database);
        return x;
    }

    public static String columnSpec(String columnSpec) {
        return format(" (%s) ", columnSpec);
    }

    public static String partitionSpec(String partSpec) {
        return format(" PARTITIONED BY(%s) ", partSpec);
    }

    public static String bucketSpec(String bucketColumns, long numOfBuckets) {
        return format(" CLUSTERED BY (%s) INTO %s BUCKETS ", bucketColumns, numOfBuckets);
    }

    public static String tblProperties(String keyValuePairs) {
        return format(" TBLPROPERTIES (%s) ", keyValuePairs);
    }

    public static String createTablePrelude(String database, String table, boolean ifNotExists) {
        return format("CREATE TABLE %s %s.%s ",
                orBlank(ifNotExists, "IF NOT EXISTS"),
                database,
                table);
    }

    private static String orBlank(boolean useText, String text) {
        return (useText ? text : "");
    }

    public static String loadInto(String path, String database, String table) {
      return format("LOAD DATA INPATH '%s' INTO TABLE %s.%s", path, database, table);
    }

    public static String randomAlias() {
        return "q_" + UUID.randomUUID().toString().replaceAll("[^A-Za-z0-9 ]", "");
    }
}
