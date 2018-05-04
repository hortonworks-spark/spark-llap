package com.hortonworks.spark.sql.hive.llap;

import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

class HiveWarehouseSessionState {
    SparkSession session;
    Map<String, Object> props = new HashMap<>();

    String getString(HWConf confValue) {
       return confValue.getString(this);
    }

    Long getLong(HWConf confValue) {
        return confValue.getLong(this);
    }
}
