package com.hortonworks.spark.sql.hive.llap;

import org.apache.spark.sql.SparkSession;

public class HiveWarehouseBuilder {

    HiveWarehouseSessionState sessionState = new HiveWarehouseSessionState();

    //Can only be instantiated through session(SparkSession session)
    private HiveWarehouseBuilder() {
    }

    public static HiveWarehouseBuilder session(SparkSession session) {
        HiveWarehouseBuilder builder = new HiveWarehouseBuilder();
        builder.sessionState.session = session;
        return builder;
    }

    public HiveWarehouseBuilder userPassword(String user, String password) {
        this.sessionState.props.put(HWConf.USER.qualifiedKey, user);
        this.sessionState.props.put(HWConf.PASSWORD.qualifiedKey, password);
        return this;
    }

    public HiveWarehouseBuilder hs2url(String hs2url) {
        this.sessionState.props.put(HWConf.HS2_URL.qualifiedKey, hs2url);
        return this;
    }

    public HiveWarehouseBuilder maxExecResults(long maxExecResults) {
        this.sessionState.props.put(HWConf.MAX_EXEC_RESULTS.qualifiedKey, maxExecResults);
        return this;
    }

    public HiveWarehouseBuilder dbcp2Conf(String dbcp2Conf) {
        this.sessionState.props.put(HWConf.DBCP2_CONF.qualifiedKey, dbcp2Conf);
        return this;
    }

    public HiveWarehouseBuilder defaultDB(String defaultDB) {
        this.sessionState.props.put(HWConf.DEFAULT_DB.qualifiedKey, defaultDB);
        return this;
    }

    //This is the only way for application to obtain a HiveWarehouseSessionImpl
    public HiveWarehouseSessionImpl build() {
        return new HiveWarehouseSessionImpl(this.sessionState);
    }

    //Revealed internally for test only (not public)
    HiveWarehouseSessionState sessionStateForTest() {
        return this.sessionState;
    }
}
