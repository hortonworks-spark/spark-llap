package com.hortonworks.spark.sql.hive.llap

import org.scalatest.FunSuite

class TestJavaProxy extends FunSuite {
  test("HiveWarehouseBuilder") {
    val builderTest = new HiveWarehouseBuilderTest();
    builderTest.testAllBuilderConfig();
  }

  test("HiveWarehouseSession") {
    val hiveWarehouseSessionTest = new HiveWarehouseSessionHiveQlTest();
    hiveWarehouseSessionTest.runFlowWithoutException();
  }
}
