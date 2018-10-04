package com.hortonworks.spark.sql.hive.llap.util;

import com.hortonworks.spark.sql.hive.llap.HWConf;
import com.hortonworks.spark.sql.hive.llap.HiveWarehouseDataSourceReader;
import com.hortonworks.spark.sql.hive.llap.Utils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkContext;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobUtil {

  private static Logger LOG = LoggerFactory.getLogger(JobUtil.class);

  public static JobConf createJobConf(Map<String, String> options, String queryString) {
    JobConf jobConf = new JobConf(SparkContext.getOrCreate().hadoopConfiguration());
    jobConf.set("hive.llap.zk.registry.user", "hive");
    jobConf.set("llap.if.hs2.connection", HWConf.RESOLVED_HS2_URL.getFromOptionsMap(options));
    if (queryString != null) {
      jobConf.set("llap.if.query", queryString);
    }
    jobConf.set("llap.if.user", HWConf.USER.getFromOptionsMap(options));
    jobConf.set("llap.if.pwd", HWConf.PASSWORD.getFromOptionsMap(options));
    if (options.containsKey("default.db")) {
      jobConf.set("llap.if.database", HWConf.DEFAULT_DB.getFromOptionsMap(options));
    }
    if (!options.containsKey("handleid")) {
      String handleId = UUID.randomUUID().toString();
      options.put("handleid", handleId);
    }
    jobConf.set("llap.if.handleid", options.get("handleid"));
    return jobConf;
  }

  public static void replaceSparkHiveDriver() throws Exception {
    Enumeration<Driver> drivers = DriverManager.getDrivers();
    while(drivers.hasMoreElements()) {
      Driver driver = drivers.nextElement();
      String driverName = driver.getClass().getName();
      LOG.debug("Found a registered JDBC driver {}", driverName);
      if(driverName.endsWith("HiveDriver")) {
        LOG.debug("Deregistering {}", driverName);
        DriverManager.deregisterDriver(driver);
      } else {
        LOG.debug("Not deregistering the {}", driverName);
      }
    }
    DriverManager.registerDriver((Driver) Class.forName("shadehive.org.apache.hive.jdbc.HiveDriver").newInstance());
  }

}
