package com.hortonworks.spark.sql.hive.llap;

import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

class HiveWarehouseBuilderTest {

    static final String TEST_USER = "userX";
    static final String TEST_PASSWORD = "passwordX";
    static final String TEST_HS2_URL = "jdbc:hive2://nohost:10084";
    static final String TEST_DBCP2_CONF = "defaultQueryTimeout=100";
    static final Long TEST_EXEC_RESULTS_MAX = Long.valueOf(12345L);
    static final String TEST_DEFAULT_DB = "default12345";

    @Test
    void testAllBuilderConfig() {
        SparkSession session = SparkSession
                .builder()
                .master("local")
                .appName("HiveWarehouseConnector test")
                .getOrCreate();
        HiveWarehouseSessionState sessionState =
                HiveWarehouseBuilder
                        .session(session)
                        .userPassword(TEST_USER, TEST_PASSWORD)
                        .hs2url(TEST_HS2_URL)
                        .dbcp2Conf(TEST_DBCP2_CONF)
                        .maxExecResults(TEST_EXEC_RESULTS_MAX)
                        .defaultDB(TEST_DEFAULT_DB)
                        .sessionStateForTest();
        MockHiveWarehouseSessionImpl hive = new MockHiveWarehouseSessionImpl(sessionState);
        assertEquals(hive.sessionState.session, session);
        assertEquals(HWConf.USER.getString(hive.sessionState), TEST_USER);
        assertEquals(HWConf.PASSWORD.getString(hive.sessionState), TEST_PASSWORD);
        assertEquals(HWConf.HS2_URL.getString(hive.sessionState), TEST_HS2_URL);
        assertEquals(HWConf.DBCP2_CONF.getString(hive.sessionState), TEST_DBCP2_CONF);
        assertEquals(HWConf.MAX_EXEC_RESULTS.getLong(hive.sessionState), TEST_EXEC_RESULTS_MAX);
        assertEquals(HWConf.DEFAULT_DB.getString(hive.sessionState), TEST_DEFAULT_DB);
    }

    @Test
    void testAllConfConfig() {
        SparkSession session = SparkSession
                .builder()
                .master("local")
                .appName("HiveWarehouseConnector test")
                .getOrCreate();
        session.conf().set(HWConf.USER.qualifiedKey, TEST_USER);
        session.conf().set(HWConf.PASSWORD.qualifiedKey, TEST_PASSWORD);
        session.conf().set(HWConf.HS2_URL.qualifiedKey, TEST_HS2_URL);
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
        assertEquals(HWConf.HS2_URL.getString(hive.sessionState), TEST_HS2_URL);
        assertEquals(HWConf.DBCP2_CONF.getString(hive.sessionState), TEST_DBCP2_CONF);
        assertEquals(HWConf.MAX_EXEC_RESULTS.getLong(hive.sessionState), TEST_EXEC_RESULTS_MAX);
        assertEquals(HWConf.DEFAULT_DB.getString(hive.sessionState), TEST_DEFAULT_DB);
    }
}
