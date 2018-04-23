package com.hortonworks.spark.sql.hive.llap;

import com.hortonworks.spark.sql.hive.llap.api.HiveWarehouseSession;
import com.hortonworks.spark.sql.hive.llap.util.HiveQlUtil;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

public class CreateTableBuilder {
    private HiveWarehouseSession hive;
    private String database;
    private String tableName;
    private boolean ifNotExists;
    private List<Pair<String, String>> cols = new ArrayList<>();
    private List<Pair<String, String>> parts = new ArrayList<>();
    private String[] clusters;
    private Long buckets;

    CreateTableBuilder(HiveWarehouseSession hive, String database, String tableName) {
        this.hive = hive;
        this.tableName = tableName;
        this.database = database;
    }

    public CreateTableBuilder ifNotExists() {
        this.ifNotExists = true;
        return this;
    }

    public CreateTableBuilder column(String name, String type) {
        cols.add(Pair.of(name, type));
        return this;
    }

    public CreateTableBuilder partition(String name, String type) {
        parts.add(Pair.of(name, type));
        return this;
    }

    public CreateTableBuilder clusterBy(long numBuckets, String ... columns) {
        this.buckets = numBuckets;
        this.clusters = columns;
        return this;
    }

    public void create() {
        hive.exec(this.toString());
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(HiveQlUtil.createTablePrelude(database, tableName, ifNotExists));
        if(cols.size() > 0) {
            List<String> colsStrings = new ArrayList<>();
            for(Pair<String, String> col : cols) {
                colsStrings.add(col.getKey() + " " + col.getValue());
            }
            builder.append("(" + String.join(",", colsStrings) + ") ");
        }
        if(parts.size() > 0) {
            List<String> partsStrings = new ArrayList<>();
            for(Pair<String, String> part : parts) {
                partsStrings.add(part.getKey() + " " + part.getValue());
            }
            builder.append("PARTITIONED BY (" + String.join(",", partsStrings) + ") ");
        }
        if(clusters != null) {
            builder.append("CLUSTERED BY (" +
                    String.join(",", clusters) +
                    ") INTO " + buckets + " BUCKETS ");
        }
        return builder.toString();
    }
}
