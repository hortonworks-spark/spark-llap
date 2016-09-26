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

package org.apache.spark.sql.hive.llap

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveSharedState

/**
 * A class that holds all state shared across sessions in a given
 * [[org.apache.spark.sql.SparkSession]] backed by `HiveServer2 Interactive`.
 */
private[hive] class LlapSharedState(override val sparkContext: SparkContext)
  extends HiveSharedState(sparkContext) {

  /**
   * A catalog that interacts with the `HiveServer2 Interactive` metastore.
   */
  override lazy val externalCatalog = {
    new LlapExternalCatalog(
      sparkContext,
      metadataHive,
      sparkContext.hadoopConfiguration)
  }
}
