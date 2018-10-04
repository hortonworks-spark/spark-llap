package com.hortonworks.spark.sql.hive.llap;

import com.google.common.collect.Lists;
import com.hortonworks.spark.sql.hive.llap.util.SchemaUtil;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import static com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilderTest.*;
import static com.hortonworks.spark.sql.hive.llap.TestSecureHS2Url.TEST_HS2_URL;
import static org.apache.spark.sql.types.DataTypes.*;
import static org.junit.Assert.assertTrue;

public class SchemaUtilTest extends SessionTestBase {

  @Test
  public void testBuildHiveCreateTableQueryFromSparkDFSchema() {

    HiveWarehouseSessionState sessionState =
        HiveWarehouseBuilder
            .session(session)
            .userPassword(TEST_USER, TEST_PASSWORD)
            .hs2url(TEST_HS2_URL)
            .dbcp2Conf(TEST_DBCP2_CONF)
            .maxExecResults(TEST_EXEC_RESULTS_MAX)
            .defaultDB(TEST_DEFAULT_DB)
            .sessionStateForTest();
    HiveWarehouseSession hive = new MockHiveWarehouseSessionImpl(sessionState);

    HiveWarehouseSessionImpl.HIVE_WAREHOUSE_CONNECTOR_INTERNAL = "com.hortonworks.spark.sql.hive.llap.MockHiveWarehouseConnector";

    StructType schema = getSchema();
    String query = SchemaUtil.buildHiveCreateTableQueryFromSparkDFSchema(schema, "testDB", "testTable");
    System.out.println("create table query:" + query);
    assertTrue(hive.executeUpdate(query));
  }

  private StructType getSchema() {
    return (new StructType())
        .add("c1", ByteType)
        .add("c2", ShortType)
        .add("c3", IntegerType)
        .add("c4", LongType)
        .add("c5", FloatType)
        .add("c6", DoubleType)
        .add("c7", createDecimalType())
        .add("c8", StringType)
        .add("c9", StringType,
            true, new MetadataBuilder().putString("HIVE_TYPE_STRING", "char(20)").build())
        .add("c10", StringType,
            true, new MetadataBuilder().putString("HIVE_TYPE_STRING", "varchar(20)").build())
        .add("c11", BinaryType)
        .add("c12", BooleanType)
        .add("c13", TimestampType)
        .add("c14", DateType)
        .add("c15", createArrayType(StringType))
        .add("c16", createStructType(Lists.newArrayList(
            createStructField("f1", IntegerType, true),
            createStructField("f2", StringType, true))))
        .add("c17", createStructType(Lists.newArrayList(
            createStructField("f1",
                createStructType(Lists.newArrayList(
                    createStructField("f2", IntegerType, true))),
                true))));
  }
}