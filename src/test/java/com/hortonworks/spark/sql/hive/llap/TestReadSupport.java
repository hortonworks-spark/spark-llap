package com.hortonworks.spark.sql.hive.llap;

import org.apache.spark.sql.Row;
import org.junit.Test;

import static com.hortonworks.spark.sql.hive.llap.TestSecureHS2Url.TEST_HS2_URL;
import static org.junit.Assert.assertEquals;

public class TestReadSupport extends SessionTestBase {

  @Test
  public void testReadSupport() {
    HiveWarehouseSession hive = HiveWarehouseBuilder.
        session(session).
        hs2url(TEST_HS2_URL).
        build();
    HiveWarehouseSessionImpl impl = (HiveWarehouseSessionImpl) hive;
    impl.HIVE_WAREHOUSE_CONNECTOR_INTERNAL = "com.hortonworks.spark.sql.hive.llap.MockHiveWarehouseConnector";
    Row[] rows = (Row[]) hive.executeQuery("SELECT a from fake").sort("a").collect();
    for(int i = 0; i < MockHiveWarehouseConnector.testVector.length; i++) {
      assertEquals(rows[i].getInt(0), MockHiveWarehouseConnector.testVector[i]);
    }
  }

}
