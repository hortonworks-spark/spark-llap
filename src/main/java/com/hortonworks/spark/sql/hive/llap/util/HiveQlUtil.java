package com.hortonworks.spark.sql.hive.llap.util;

import static java.lang.String.format;

public class HiveQlUtil {

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
                table,
                orBlank(ifExists, "IF EXISTS"),
                orBlank(purge, "PURGE"));
    }

    public static String createDatabase(String database, boolean ifNotExists) {
        String x = format("CREATE DATABASE %s %s",
                orBlank(ifNotExists, "IF NOT EXISTS"),
                database);
        System.out.println("TEST: " + x);
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
}
