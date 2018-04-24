package com.hortonworks.spark.sql.hive.llap;

import com.hortonworks.spark.sql.hive.llap.api.HiveWarehouseSession;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Test;

import static com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilderTest.*;

public class HiveWarehouseSessionHiveQlTest {

    @Test
    void runFlowWithoutException() {
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
        HiveWarehouseSession hive =
               new MockHiveWarehouseSessionImpl(sessionState);
        Assert.assertEquals(hive.executeQuery("SELECT * FROM t1").count(), 10);
        hive.createDatabase(TEST_DEFAULT_DB + "2", true);
        hive.setDatabase(TEST_DEFAULT_DB + "2");
        hive.describeTable(TEST_DEFAULT_DB + "2");
        hive.showTables();
        hive.createTable("TestTable")
                .ifNotExists()
                .column("id", "int")
                .column("val", "string")
                .partition("id", "int")
                .clusterBy(100, "val")
                .create();
    }

}
