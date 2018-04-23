package com.hortonworks.spark.sql.hive.llap;

import com.hortonworks.spark.sql.hive.llap.api.HiveWarehouseSession;
import org.apache.spark.sql.SparkSession;

import java.util.Optional;

import static com.hortonworks.spark.sql.hive.llap.api.HiveWarehouseSession.warehouseKey;

public class HiveWarehouseSessionState {

    public SparkSession session;
    public String user;
    public String password;
    public String hs2url;
    public Long maxExecResults;
    public String dbcp2Conf;
    public String defaultDB;

    public SparkSession session() {
        return session;
    }

    public String user() {
        return Optional.of(user).orElse(getConfStringOrNull(warehouseKey(HiveWarehouseSession.USER_KEY)));
    }

    public String password() {
        return Optional.of(password).orElse(getConfStringOrNull(warehouseKey(HiveWarehouseSession.PASSWORD_KEY)));
    }

    public String hs2url() {
        return Optional.of(hs2url).orElse(getConfStringOrNull(warehouseKey(HiveWarehouseSession.HS2_URL_KEY)));
    }

    public Long maxExecResults() {
        return Optional.of(maxExecResults)
                .orElse(getConfLongOrNull(warehouseKey(HiveWarehouseSession.EXEC_RESULTS_MAX_KEY), HiveWarehouseSession.DEFAULT_EXEC_RESULT_MAX));
    }

    public String dbcp2Conf() {
        return Optional.ofNullable(dbcp2Conf).orElse(getConfStringOrNull(warehouseKey(HiveWarehouseSession.DBCP2_CONF_KEY)));
    }

    public String database() {
        return Optional.of(defaultDB).orElse(getConfStringOrNull(warehouseKey(HiveWarehouseSession.DEFAULT_DB_KEY)));
    }

    public String getConfStringOrNull(String key) {
        if(session.sparkContext().getConf().contains(key)) {
            return session.sparkContext().getConf().get(key);
        } else {
            return null;
        }
    }

    public Long getConfLongOrNull(String key, Long orElse) {
        if(session.sparkContext().getConf().contains(key)) {
            return session.sparkContext().getConf().getLong(key, orElse);
        } else {
            return null;
        }
    }
}
