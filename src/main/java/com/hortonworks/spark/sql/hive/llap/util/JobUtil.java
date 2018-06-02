package com.hortonworks.spark.sql.hive.llap.util;

import com.hortonworks.spark.sql.hive.llap.HWConf;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkContext;

import java.util.Map;
import java.util.UUID;

public class JobUtil {

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

}
