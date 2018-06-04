package com.hortonworks.spark.sql.hive.llap;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.Test;

import java.util.List;

import static com.hortonworks.spark.sql.hive.llap.MockHiveWarehouseConnector.testVector;
import static com.hortonworks.spark.sql.hive.llap.TestSecureHS2Url.TEST_HS2_URL;
import static org.junit.Assert.assertEquals;

public class TestWriteSupport extends SessionTestBase {

  @Test
  public void testWriteSupport() {
    HiveWarehouseSession hive = HiveWarehouseBuilder.
        session(session).
        hs2url(TEST_HS2_URL).
        build();
    HiveWarehouseSessionImpl impl = (HiveWarehouseSessionImpl) hive;
    impl.HIVE_WAREHOUSE_CONNECTOR_INTERNAL = "com.hortonworks.spark.sql.hive.llap.MockHiveWarehouseConnector";
    
    List<InternalRow> rows = (List<InternalRow>) MockHiveWarehouseConnector.writeOutputBuffer.get("TestWriteSupport");
    for(int i = 0; i < rows.size(); i++) {
      InternalRow current = rows.get(i);
      assertEquals(current.getInt(0), testVector[i]);
    }
  }

}
