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

from pyspark_llap.sql.session import HiveWarehouseSession

# These are aliases so that importing this module exposes those attributes below directly.
DATAFRAME_TO_STREAM = HiveWarehouseSession.DATAFRAME_TO_STREAM
HIVE_WAREHOUSE_CONNECTOR = HiveWarehouseSession.HIVE_WAREHOUSE_CONNECTOR
STREAM_TO_STREAM = HiveWarehouseSession.STREAM_TO_STREAM


__all__ = [
    'HiveWarehouseSession',
    'DATAFRAME_TO_STREAM',
    'HIVE_WAREHOUSE_CONNECTOR',
    'STREAM_TO_STREAM',
]
