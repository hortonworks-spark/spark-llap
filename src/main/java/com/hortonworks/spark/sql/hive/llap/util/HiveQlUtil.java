package com.hortonworks.spark.sql.hive.llap.util;

import static java.lang.String.format;

public class HiveQlUtil {

    public static String useDatabase(String database) {
        return format("USE %s", database);
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

    //Requires jdbc Connection with current database
    public static String dropTable(String table, boolean ifExists, boolean purge) {
        return format("DROP TABLE %s %s %s",
                table,
                orBlank(ifExists, "IF EXISTS"),
                orBlank(purge, "PURGE"));
    }

    public static String createDatabase(String database, boolean ifNotExists) {
        return format("CREATE DATABASE %s %s",
                orBlank(ifNotExists, "IF NOT EXISTS"),
                database);
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
