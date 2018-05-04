package com.hortonworks.spark.sql.hive.llap;

import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;

import static com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilderTest.*;
import static org.junit.Assert.assertEquals;

class HiveWarehouseSessionHiveQlTest {

    private HiveWarehouseSession hive;
    private int mockExecuteResultSize;

    @Before
    void setup() {
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
         hive = new MockHiveWarehouseSessionImpl(sessionState);
         mockExecuteResultSize =
                 MockHiveWarehouseSessionImpl.testFixture().data.size();
    }

    @Test
    void testExecuteQuery() {
        assertEquals(hive.executeQuery("SELECT * FROM t1").count(),
                MockHiveWarehouseDataReader.RESULT_SIZE);
    }

    @Test
    void testSetDatabase() {
        hive.setDatabase(TEST_DEFAULT_DB);
    }

    @Test
    void testDescribeTable() {
       assertEquals(hive.describeTable("testTable").count(),
               mockExecuteResultSize);
    }

    @Test
    void testCreateDatabase() {
        hive.createDatabase(TEST_DEFAULT_DB, false);
        hive.createDatabase(TEST_DEFAULT_DB, true);
    }

    @Test
    void testShowTable() {
        assertEquals(hive.showTables().count(), mockExecuteResultSize);
    }

    @Test
    void testCreateTable() {
        hive.createTable("TestTable")
                .ifNotExists()
                .column("id", "int")
                .column("val", "string")
                .partition("id", "int")
                .clusterBy(100, "val")
                .prop("key", "value")
                .create();
    }

}
