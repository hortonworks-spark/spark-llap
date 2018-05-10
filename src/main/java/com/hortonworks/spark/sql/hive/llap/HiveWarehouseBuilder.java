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

import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class HiveWarehouseBuilder {

    HiveWarehouseSessionState sessionState = new HiveWarehouseSessionState();

    //Can only be instantiated through session(SparkSession session)
    private HiveWarehouseBuilder() {
    }

    public static HiveWarehouseBuilder session(SparkSession session) {
        HiveWarehouseBuilder builder = new HiveWarehouseBuilder();
        builder.sessionState.session = session;
        session.conf().getAll().foreach(new scala.runtime.AbstractFunction1<scala.Tuple2<String, String>, Object>() {
          public Object apply(Tuple2<String, String> keyValue) {
            String key = keyValue._1;
            String value = keyValue._2;
            if(key.startsWith(HWConf.HIVE_WAREHOUSE_CONF_PREFIX)) {
              session.sessionState().conf().setConfString(key, value);
            }
            return null;
          }
        });
        return builder;
    }

    public HiveWarehouseBuilder userPassword(String user, String password) {
        this.sessionState.props.put(HWConf.USER.qualifiedKey, user);
        this.sessionState.props.put(HWConf.PASSWORD.qualifiedKey, password);
        return this;
    }

    public HiveWarehouseBuilder hs2url(String hs2url) {
        this.sessionState.props.put(HWConf.HS2_URL.qualifiedKey, hs2url);
        return this;
    }

    public HiveWarehouseBuilder maxExecResults(long maxExecResults) {
        this.sessionState.props.put(HWConf.MAX_EXEC_RESULTS.qualifiedKey, maxExecResults);
        return this;
    }

    public HiveWarehouseBuilder dbcp2Conf(String dbcp2Conf) {
        this.sessionState.props.put(HWConf.DBCP2_CONF.qualifiedKey, dbcp2Conf);
        return this;
    }

    public HiveWarehouseBuilder defaultDB(String defaultDB) {
        this.sessionState.props.put(HWConf.DEFAULT_DB.qualifiedKey, defaultDB);
        return this;
    }

    //This is the only way for application to obtain a HiveWarehouseSessionImpl
    public HiveWarehouseSessionImpl build() {
        return new HiveWarehouseSessionImpl(this.sessionState);
    }

    //Revealed internally for test only (not public)
    HiveWarehouseSessionState sessionStateForTest() {
        return this.sessionState;
    }
}
