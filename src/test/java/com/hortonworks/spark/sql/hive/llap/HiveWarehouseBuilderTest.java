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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

class HiveWarehouseBuilderTest extends SessionTestBase {

    static final String TEST_USER = "userX";
    static final String TEST_PASSWORD = "passwordX";
    static final String TEST_DBCP2_CONF = "defaultQueryTimeout=100";
    static final Integer TEST_EXEC_RESULTS_MAX = 12345;
    static final String TEST_DEFAULT_DB = "default12345";

    @Test
    void testNewEntryPoint() {
        session.sessionState().conf().setConfString(HWConf.HIVESERVER2_JDBC_URL, "test");
        com.hortonworks.hwc.HiveWarehouseSession hive =
            com.hortonworks.hwc.HiveWarehouseSession.session(session)
                .userPassword(TEST_USER, TEST_PASSWORD)
                .dbcp2Conf(TEST_DBCP2_CONF)
                .maxExecResults(TEST_EXEC_RESULTS_MAX)
                .defaultDB(TEST_DEFAULT_DB).build();
        assertEquals(hive.session(), session);
    }

    @Test
    void testAllBuilderConfig() {
        HiveWarehouseSessionState sessionState =
                HiveWarehouseBuilder
                        .session(session)
                        .userPassword(TEST_USER, TEST_PASSWORD)
                        .dbcp2Conf(TEST_DBCP2_CONF)
                        .maxExecResults(TEST_EXEC_RESULTS_MAX)
                        .defaultDB(TEST_DEFAULT_DB)
                        .sessionStateForTest();
        MockHiveWarehouseSessionImpl hive = new MockHiveWarehouseSessionImpl(sessionState);
        assertEquals(hive.session(), session);
        assertEquals(HWConf.USER.getString(sessionState), TEST_USER);
        assertEquals(HWConf.PASSWORD.getString(sessionState), TEST_PASSWORD);
        assertEquals(HWConf.DBCP2_CONF.getString(sessionState), TEST_DBCP2_CONF);
        assertEquals(HWConf.MAX_EXEC_RESULTS.getInt(sessionState), TEST_EXEC_RESULTS_MAX);
        assertEquals(HWConf.DEFAULT_DB.getString(sessionState), TEST_DEFAULT_DB);
    }

    @Test
    void testAllConfConfig() {
        session.conf().set(HWConf.USER.qualifiedKey, TEST_USER);
        session.conf().set(HWConf.PASSWORD.qualifiedKey, TEST_PASSWORD);
        session.conf().set(HWConf.DBCP2_CONF.qualifiedKey, TEST_DBCP2_CONF);
        session.conf().set(HWConf.MAX_EXEC_RESULTS.qualifiedKey, TEST_EXEC_RESULTS_MAX);
        session.conf().set(HWConf.DEFAULT_DB.qualifiedKey, TEST_DEFAULT_DB);
        HiveWarehouseSessionState sessionState =
                HiveWarehouseBuilder
                        .session(session)
                        .sessionStateForTest();
        MockHiveWarehouseSessionImpl hive = new MockHiveWarehouseSessionImpl(sessionState);
        assertEquals(hive.sessionState.session, session);
        assertEquals(HWConf.USER.getString(hive.sessionState), TEST_USER);
        assertEquals(HWConf.PASSWORD.getString(hive.sessionState), TEST_PASSWORD);
        assertEquals(HWConf.DBCP2_CONF.getString(hive.sessionState), TEST_DBCP2_CONF);
        assertEquals(HWConf.MAX_EXEC_RESULTS.getInt(hive.sessionState), TEST_EXEC_RESULTS_MAX);
        assertEquals(HWConf.DEFAULT_DB.getString(hive.sessionState), TEST_DEFAULT_DB);
    }
}
