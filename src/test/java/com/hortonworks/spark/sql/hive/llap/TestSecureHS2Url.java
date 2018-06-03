package com.hortonworks.spark.sql.hive.llap;

import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class TestSecureHS2Url extends SessionTestBase {

  static final String TEST_HS2_URL = "jdbc:hive2://example.com:10084";
  static final String TEST_PRINCIPAL = "testUser/_HOST@EXAMPLE.com";

  static final String KERBERIZED_CLUSTER_MODE_URL = TEST_HS2_URL + ";auth=delegationToken";
  static final String KERBERIZED_CLIENT_MODE_URL =
      TEST_HS2_URL +
          ";principal=" +
          TEST_PRINCIPAL;

  @Test
  public void kerberizedClusterMode() {
    HiveWarehouseSessionState state = HiveWarehouseBuilder
        .session(session)
        .hs2url(TEST_HS2_URL)
        .credentialsEnabled()
        .build()
        .sessionState;

    String resolvedUrl = HWConf.RESOLVED_HS2_URL.getString(state);
    assertEquals(resolvedUrl, KERBERIZED_CLUSTER_MODE_URL);
  }

  @Test
  public void kerberizedClientMode() {
    HiveWarehouseSessionState state = HiveWarehouseBuilder
        .session(session)
        .hs2url(TEST_HS2_URL)
        .principal(TEST_PRINCIPAL)
        .build()
        .sessionState;

    String resolvedUrl = HWConf.RESOLVED_HS2_URL.getString(state);
    assertEquals(resolvedUrl, KERBERIZED_CLIENT_MODE_URL);
  }

  @Test
  public void nonKerberized() {
    HiveWarehouseSessionState state = HiveWarehouseBuilder
        .session(session)
        .hs2url(TEST_HS2_URL)
        .build()
        .sessionState;

    String resolvedUrl = HWConf.RESOLVED_HS2_URL.getString(state);
    assertEquals(resolvedUrl, TEST_HS2_URL);
  }

}
