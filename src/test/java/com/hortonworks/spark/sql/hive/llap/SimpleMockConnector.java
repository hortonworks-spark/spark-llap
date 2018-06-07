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

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.SessionConfigSupport;
import org.apache.spark.sql.sources.v2.reader.DataReader;
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class SimpleMockConnector implements DataSourceV2, ReadSupport, SessionConfigSupport {

    @Override
    public DataSourceReader createReader(DataSourceOptions options) {
        return new SimpleMockDataSourceReader();
    }

    @Override
    public String keyPrefix() {
        return HiveWarehouseSession.CONF_PREFIX;
    }

    public static class SimpleMockDataReader implements DataReader<Row> {

        // Exposed for Python side.
        public static final int RESULT_SIZE = 10;
        private int i = 0;

        @Override
        public boolean next() throws IOException {
            return (i < 10);
        }

        @Override
        public Row get() {
            Row value = new GenericRow(new Object[] {i, "Element " + i});
            i++;
            return value;
        }

        @Override
        public void close() {

        }
    }

    public static class SimpleMockDataReaderFactory implements DataReaderFactory<Row> {

        @Override
        public DataReader<Row> createDataReader() {
            return new SimpleMockDataReader();
        }

    }

    public static class SimpleMockDataSourceReader implements DataSourceReader {

        @Override
        public StructType readSchema() {
           return (new StructType())
                   .add("col1", "int")
                   .add("col2", "string");
        }

        @Override
        public List<DataReaderFactory<Row>> createDataReaderFactories() {
            return Arrays.asList(new SimpleMockDataReaderFactory());
        }
    }
}
