/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.spark.sql.hive.llap;

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

import static com.hortonworks.spark.sql.hive.llap.util.HiveQlUtil.*;
import static java.lang.String.join;
import static java.lang.String.format;

public class CreateTableBuilder implements com.hortonworks.hwc.CreateTableBuilder {
    private HiveWarehouseSession hive;
    private String database;
    private String tableName;
    private boolean ifNotExists;
    private List<Pair<String, String>> cols = new ArrayList<>();
    private List<Pair<String, String>> parts = new ArrayList<>();
    private List<Pair<String, String>> props = new ArrayList<>();
    private String[] clusters;
    private Long buckets;

    CreateTableBuilder(HiveWarehouseSession hive, String database, String tableName) {
        this.hive = hive;
        this.tableName = tableName;
        this.database = database;
    }

    @Override
    public CreateTableBuilder ifNotExists() {
        this.ifNotExists = true;
        return this;
    }

    @Override
    public CreateTableBuilder column(String name, String type) {
        cols.add(Pair.of(name, type));
        return this;
    }

    @Override
    public CreateTableBuilder partition(String name, String type) {
        parts.add(Pair.of(name, type));
        return this;
    }

    @Override
    public CreateTableBuilder prop(String key, String value) {
        props.add(Pair.of(key, value));
        return this;
    }

    @Override
    public CreateTableBuilder clusterBy(long numBuckets, String ... columns) {
        this.buckets = numBuckets;
        this.clusters = columns;
        return this;
    }

    @Override
    public void create() {
        hive.executeUpdate(this.toString());
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(createTablePrelude(database, tableName, ifNotExists));
        if(cols.size() > 0) {
            List<String> colsStrings = new ArrayList<>();
            for(Pair<String, String> col : cols) {
                colsStrings.add(col.getKey() + " " + col.getValue());
            }
            builder.append(columnSpec(join(",", colsStrings)));
        }
        if(parts.size() > 0) {
            List<String> partsStrings = new ArrayList<>();
            for(Pair<String, String> part : parts) {
                partsStrings.add(part.getKey() + " " + part.getValue());
            }
            builder.append(partitionSpec(join(",", partsStrings)));
        }
        if(clusters != null) {
            builder.append(bucketSpec(join(",", clusters), buckets));
        }
        //Currently only managed ORC tables are supported
        builder.append(" STORED AS ORC ");
        if(props.size() > 0) {
            List<String> keyValueStrings = new ArrayList<>();
            for(Pair<String, String> keyValue : props) {
                keyValueStrings.add(format("\"%s\"=\"%s\"", keyValue.getKey(), keyValue.getValue()));
            }
            builder.append(tblProperties(join(",", keyValueStrings)));
        }
        return builder.toString();
    }
}
