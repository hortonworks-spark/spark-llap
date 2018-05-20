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

import java.util.Map;
import java.util.Optional;

/**
 * See: {@link org.apache.spark.sql.sources.v2.SessionConfigSupport}
 */
enum HWConf {

  //ENUM(shortKey, qualifiedKey, default)
  USER("user.name", warehouseKey("user.name"), ""),
  PASSWORD("password", warehouseKey("password"), ""),
  HS2_URL("hs2.url", warehouseKey("hs2.url"), "jdbc:hive2://localhost:10500"),
  DBCP2_CONF("dbcp2.conf", warehouseKey("dbcp2.conf"), null),
  DEFAULT_DB("default.db", warehouseKey("default.db"), "default"),
  MAX_EXEC_RESULTS("exec.results.max", warehouseKey("exec.results.max"), 1000),
  LOAD_STAGING_DIR("load.staging.dir", warehouseKey("load.staging.dir"), "/tmp"),
  ARROW_ALLOCATOR_MAX("arrow.allocator.max", warehouseKey("arrow.allocator.max"), Long.MAX_VALUE);

  private HWConf(String simpleKey, String qualifiedKey, Object defaultValue) {
    this.simpleKey = simpleKey;
    this.qualifiedKey = qualifiedKey;
    this.defaultValue = defaultValue;
  }

  static String SPARK_DATASOURCES_PREFIX = "spark.datasource";
  static String HIVE_WAREHOUSE_POSTFIX = "hive.warehouse";
  static String CONF_PREFIX = SPARK_DATASOURCES_PREFIX + "." + HIVE_WAREHOUSE_POSTFIX;

  static String warehouseKey(String keySuffix) {
    return CONF_PREFIX + "." + keySuffix;
  }

  void setString(HiveWarehouseSessionState state, String value) {
    state.props.put(qualifiedKey, value);
    state.session.sessionState().conf().setConfString(qualifiedKey, value);
  }

  void setInt(HiveWarehouseSessionState state, Integer value) {
    state.props.put(qualifiedKey, Integer.toString(value));
    state.session.sessionState().conf().setConfString(qualifiedKey, Integer.toString(value));
  }

  //This is called from executors so it can't depend explicitly on session state
  String getFromOptionsMap(Map<String, String> options) {
    return Optional.ofNullable(options.get(simpleKey)).orElse((String) defaultValue);
  }

  String getString(HiveWarehouseSessionState state) {
    return Optional.
      ofNullable((String) state.props.get(qualifiedKey)).
      orElse(state.session.sessionState().conf().getConfString(
        qualifiedKey, (String) defaultValue)
      );
  }

  Integer getInt(HiveWarehouseSessionState state) {
    return Integer.parseInt(
      Optional.
        ofNullable(state.props.get(qualifiedKey)).
        orElse(state.session.sessionState().conf().getConfString(
        qualifiedKey, defaultValue.toString())
      )
    );
  }

  String simpleKey;
  String qualifiedKey;
  Object defaultValue;
}
