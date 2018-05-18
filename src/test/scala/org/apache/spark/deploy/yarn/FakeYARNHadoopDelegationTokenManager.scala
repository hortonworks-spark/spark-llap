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
package org.apache.spark.deploy.yarn

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkConf
import org.apache.spark.deploy.yarn.security.YARNHadoopDelegationTokenManager

/**
 * Fake HadoopDelegationTokenManager to mock a credential provider.
 *
 * @note [[YARNHadoopDelegationTokenManager]] is meant to be private; therefore, it should likely
 * be easy to be broken between minor versions. This class targets to test Spark 2.3
 * but if it becomes difficult to maintain, maybe we should just remove this class and
 * `TestHiveServer2CredentialProvider`.
 */
class FakeYARNHadoopDelegationTokenManager(
    sparkConf: SparkConf,
    hadoopConf: Configuration,
    fileSystems: Configuration => Set[FileSystem])
  extends YARNHadoopDelegationTokenManager(sparkConf, hadoopConf, fileSystems)
