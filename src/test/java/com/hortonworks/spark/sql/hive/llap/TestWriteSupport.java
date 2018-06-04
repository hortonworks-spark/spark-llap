package com.hortonworks.spark.sql.hive.llap;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    DriverResultSet input = MockHiveWarehouseSessionImpl.testFixture();
    Dataset df = session.createDataFrame(input.data, input.schema);
    df.write().format(impl.HIVE_WAREHOUSE_CONNECTOR_INTERNAL).option("table", "fakeTable").save();
    List<InternalRow> rows = (List<InternalRow>) MockHiveWarehouseConnector.writeOutputBuffer.get("TestWriteSupport");
    Map<Integer, String> unorderedOutput = new HashMap<>();
    for(int i = 0; i < rows.size(); i++) {
      InternalRow current = rows.get(i);
      unorderedOutput.put(current.getInt(0), current.getString(1));
    }
    assertEquals(unorderedOutput.get(1), "ID 1");
    assertEquals(unorderedOutput.get(2), "ID 2");
  }

}
