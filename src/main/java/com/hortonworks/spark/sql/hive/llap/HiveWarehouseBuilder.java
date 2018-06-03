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

import static com.hortonworks.spark.sql.hive.llap.HWConf.HIVESERVER2_CREDENTIAL_ENABLED;
import static com.hortonworks.spark.sql.hive.llap.HWConf.HIVESERVER2_JDBC_URL;
import static com.hortonworks.spark.sql.hive.llap.HWConf.HIVESERVER2_JDBC_URL_PRINCIPAL;

public class HiveWarehouseBuilder {

    HiveWarehouseSessionState sessionState = new HiveWarehouseSessionState();

    //Can only be instantiated through session(SparkSession session)
    private HiveWarehouseBuilder() {
    }

    public static HiveWarehouseBuilder session(SparkSession session) {
        HiveWarehouseBuilder builder = new HiveWarehouseBuilder();
        builder.sessionState.session = session;
        //Copy all static configuration (e.g. spark-defaults.conf)
        //with keys matching HWConf.CONF_PREFIX into
        //the SparkSQL session conf for this session
        //Otherwise these settings will not be available to
        //v2 DataSourceReader or DataSourceWriter
        //See: {@link org.apache.spark.sql.sources.v2.SessionConfigSupport}
        session.conf().getAll().foreach(new scala.runtime.AbstractFunction1<scala.Tuple2<String, String>, Object>() {
          public Object apply(Tuple2<String, String> keyValue) {
            String key = keyValue._1;
            String value = keyValue._2;
            if(key.startsWith(HiveWarehouseSession.CONF_PREFIX)) {
              session.sessionState().conf().setConfString(key, value);
            }
            return null;
          }
        });
        return builder;
    }

    public HiveWarehouseBuilder userPassword(String user, String password) {
      HWConf.USER.setString(sessionState, user);
      HWConf.PASSWORD.setString(sessionState, password);
        return this;
    }

    public HiveWarehouseBuilder hs2url(String hs2url) {
      sessionState.session.conf().set(HIVESERVER2_JDBC_URL, hs2url);
      return this;
    }

    public HiveWarehouseBuilder principal(String principal) {
      sessionState.session.conf().set(HIVESERVER2_JDBC_URL_PRINCIPAL, principal);
      return this;
    }

    public HiveWarehouseBuilder credentialsEnabled() {
      sessionState.session.conf().set(HIVESERVER2_CREDENTIAL_ENABLED, "true");
        return this;
    }

    //Hive JDBC doesn't support java.sql.Statement.setLargeMaxResults(long)
    //Need to use setMaxResults(int) instead
    public HiveWarehouseBuilder maxExecResults(int maxExecResults) {
      HWConf.MAX_EXEC_RESULTS.setInt(sessionState, maxExecResults);
        return this;
    }

    public HiveWarehouseBuilder dbcp2Conf(String dbcp2Conf) {
      HWConf.DBCP2_CONF.setString(sessionState, dbcp2Conf);
        return this;
    }

    public HiveWarehouseBuilder defaultDB(String defaultDB) {
      HWConf.DEFAULT_DB.setString(sessionState, defaultDB);
        return this;
    }

    //This is the only way for application to obtain a HiveWarehouseSessionImpl
    public HiveWarehouseSessionImpl build() {
      HWConf.RESOLVED_HS2_URL.setString(sessionState, HWConf.getConnectionUrl(sessionState));
        return new HiveWarehouseSessionImpl(this.sessionState);
    }

    // Revealed internally for test only. Exposed for Python side.
    public HiveWarehouseSessionState sessionStateForTest() {
        return this.sessionState;
    }
}
