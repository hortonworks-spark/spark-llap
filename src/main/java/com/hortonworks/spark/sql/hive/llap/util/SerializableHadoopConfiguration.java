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

package com.hortonworks.spark.sql.hive.llap.util;

    import java.io.Serializable;
    import java.io.IOException;
    import org.apache.hadoop.conf.Configuration;


public class SerializableHadoopConfiguration implements Serializable {
  Configuration conf;

  public SerializableHadoopConfiguration(Configuration hadoopConf) {
    this.conf = hadoopConf;

    if (this.conf == null) {
      this.conf = new Configuration();
    }
  }

  public SerializableHadoopConfiguration() {
    this.conf = new Configuration();
  }

  public Configuration get() {
    return this.conf;
  }

  private void writeObject(java.io.ObjectOutputStream out) throws IOException {
    this.conf.write(out);
  }

  private void readObject(java.io.ObjectInputStream in) throws IOException {
    this.conf = new Configuration();
    this.conf.readFields(in);
  }
}