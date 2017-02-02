#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import sys

from pyspark.sql import SparkSession


if __name__ == "__main__":
    """
        Usage: bin/spark-submit --jars spark-llap_2.11-1.0.X-2.1.jar spark_llap_dsl.py
    """
    spark = SparkSession \
        .builder \
        .appName("Spark LLAP DSL Python") \
        .master("yarn") \
        .enableHiveSupport() \
        .config("spark.sql.hive.llap", "true") \
        .getOrCreate()

    df = spark.sql("select * from db_spark.t_spark")

    df.printSchema()

    df.groupBy("a").count().show()

    df.select("a").filter(df["a"] > 0).show()

    spark.stop()
