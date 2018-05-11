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

import java.util.Optional;

enum HWConf {

  USER("user.name", warehouseKey("user.name"), ""),
  PASSWORD("password", warehouseKey("password"), ""),
  HS2_URL("hs2.url", warehouseKey("hs2.url"), "jdbc:hive2://localhost:10500"),
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
      String value = (String) state.props.get(qualifiedKey);
      if (value == null) {
          // There seems no way to call spark.conf.get(key, default)?
          if (state.session.conf().getOption(qualifiedKey).nonEmpty()) {
              return state.session.conf().getOption(qualifiedKey).get();
          } else {
              // Search it from the context too just in case.
              return state.session.sparkContext().getConf().get(qualifiedKey, (String) defaultValue);
          }
      } else {
          return value;
      }
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
