package com.hortonworks.spark.sql.hive.llap;

import java.util.Map;

public enum StatementType {
    GENERIC,
    SELECT_QUERY,
    FULL_TABLE_SCAN;

    public static StatementType fromOptions(Map<String, String> options) {
        int optionCount = 0;
        StatementType type = null;
        if(options.containsKey("stmt")) {
            type = GENERIC;
            optionCount++;
        }
        if(options.containsKey("query")) {
            type = SELECT_QUERY;
            optionCount++;
        }
        if(options.containsKey("table")) {
            type = FULL_TABLE_SCAN;
            optionCount++;
        }
        if(optionCount != 1) {
            throw new IllegalArgumentException("Use one and only one of: stmt, query, table");
        }
        return type;
    }
}
