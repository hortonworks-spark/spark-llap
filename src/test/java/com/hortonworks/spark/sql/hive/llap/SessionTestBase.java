package com.hortonworks.spark.sql.hive.llap;

import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;

public class SessionTestBase {
  transient SparkSession session = null;
  String name;

  public SessionTestBase() {
    this.name = getClass().getSimpleName();
  }

  @Before
  public void setUp() {
    session = SparkSession
        .builder()
        .master("local")
        .appName(name)
        .getOrCreate();
  }

  @After
  public void tearDown() {
    session.stop();
    session = null;
  }

}
