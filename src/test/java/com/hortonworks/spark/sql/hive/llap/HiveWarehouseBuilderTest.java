package com.hortonworks.spark.sql.hive.llap;

import com.hortonworks.spark.sql.hive.llap.api.HiveWarehouseSession;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class HiveWarehouseBuilderTest {

    static final String TEST_USER = "user";
    static final String TEST_PASSWORD = "password";
    static final String TEST_HS2_URL = "jdbc:hive2://nohost:10084";
    static final String TEST_DBCP2_CONF = "defaultQueryTimeout=100";
    static final Long TEST_EXEC_RESULTS_MAX = Long.valueOf(10000L);
    static final String TEST_DEFAULT_DB = "default";

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
        assertEquals(hive.sessionState.session(), session);
        assertEquals(hive.sessionState.user(), TEST_USER);
        assertEquals(hive.sessionState.password(), TEST_PASSWORD);
        assertEquals(hive.sessionState.hs2url(), TEST_HS2_URL);
        assertEquals(hive.sessionState.dbcp2Conf(), TEST_DBCP2_CONF);
        assertEquals(hive.sessionState.maxExecResults(), TEST_EXEC_RESULTS_MAX);
        assertEquals(hive.sessionState.database(), TEST_DEFAULT_DB);
    }

    @Test
    private void testAllConfConfig() {
        SparkSession session = SparkSession
                .builder()
                .master("local")
                .appName("HiveWarehouseConnector test")
                .getOrCreate();
        session.conf().set(HiveWarehouseSession.USER_KEY, TEST_USER);
        session.conf().set(HiveWarehouseSession.PASSWORD_KEY, TEST_PASSWORD);
        session.conf().set(HiveWarehouseSession.HS2_URL_KEY, TEST_HS2_URL);
        session.conf().set(HiveWarehouseSession.DBCP2_CONF_KEY, TEST_DBCP2_CONF);
        session.conf().set(HiveWarehouseSession.EXEC_RESULTS_MAX_KEY, TEST_EXEC_RESULTS_MAX);
        session.conf().set(HiveWarehouseSession.DEFAULT_DB_KEY, TEST_DEFAULT_DB);
        HiveWarehouseSessionState sessionState =
                HiveWarehouseBuilder
                        .session(session)
                        .sessionStateForTest();
        MockHiveWarehouseSessionImpl hive = new MockHiveWarehouseSessionImpl(sessionState);
        assertEquals(hive.sessionState.session(), session);
        assertEquals(hive.sessionState.user(), TEST_USER);
        assertEquals(hive.sessionState.password(), TEST_PASSWORD);
        assertEquals(hive.sessionState.hs2url(), TEST_HS2_URL);
        assertEquals(hive.sessionState.dbcp2Conf(), TEST_DBCP2_CONF);
        assertEquals(hive.sessionState.maxExecResults(), TEST_EXEC_RESULTS_MAX);
        assertEquals(hive.sessionState.database(), TEST_DEFAULT_DB);
    }
}
