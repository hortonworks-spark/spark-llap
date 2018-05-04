package com.hortonworks.spark.sql.hive.llap;

import java.util.Optional;

enum HWConf {

  USER("user.name", warehouseKey("user.name"), ""),
  PASSWORD("password", warehouseKey("password"), ""),
  HS2_URL("hs2.url", warehouseKey("hs2.url"), "jdbc:hive2://localhost:10084"),
  DBCP2_CONF("dbcp2.conf", warehouseKey("dbcp2.conf"), null),
  DEFAULT_DB("default.db", warehouseKey("default.db"), "default"),
  MAX_EXEC_RESULTS("exec.results.max", warehouseKey("exec.results.max"), 1000L);

  private HWConf(String simpleKey, String qualifiedKey, Object defaultValue) {
    this.simpleKey = simpleKey;
    this.qualifiedKey = qualifiedKey;
    this.defaultValue = defaultValue;
  }

  static String HIVE_WAREHOUSE_CONF_PREFIX = "spark.datasources.hive.warehouse";

  static String warehouseKey(String suffix) {
    return HIVE_WAREHOUSE_CONF_PREFIX + "." + suffix;
  }

  String getString(HiveWarehouseSessionState state) {
    return Optional.
      ofNullable((String) state.props.get(qualifiedKey)).
      orElse(state.session.sparkContext().getConf().get(
        qualifiedKey, (String) defaultValue)
      );
  }

  Long getLong(HiveWarehouseSessionState state) {
    return Optional.
      ofNullable((Long) state.props.get(qualifiedKey)).
      orElse(state.session.sparkContext().getConf().getLong(
        qualifiedKey, (Long) defaultValue)
      );
  }

  String simpleKey;
  String qualifiedKey;
  Object defaultValue;
}
