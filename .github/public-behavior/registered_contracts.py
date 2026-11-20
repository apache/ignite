# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Java sources that register settings, metrics, system views, and SQL functions."""

SOURCE_ROOT = "/src/main/java/org/apache/ignite/internal/"
REGISTERED_SOURCES = {
    "core": {
        "sql/SqlKeyword",
        "cluster/DistributedBaselineConfiguration",
        "cluster/DistributedTransactionConfiguration",
        "processors/configuration/distributed/DistributedThinClientConfiguration",
        "processors/cache/CacheMetricsImpl",
        "processors/cache/CacheGroupMetricsImpl",
        "processors/cache/persistence/DataRegionMetricsImpl",
        "processors/cache/persistence/DataStorageMetricsImpl",
        "processors/cache/transactions/TransactionMetricsAdapter",
        "processors/query/QueryParserMetricsHolder",
        "managers/discovery/GridDiscoveryManager",
        "processors/cache/ClusterCachesInfo",
        "processors/cluster/GridClusterStateProcessor",
        "processors/datastructures/DataStructuresProcessor",
        "processors/query/schema/management/SchemaManager",
    },
    "calcite": {"processors/query/calcite/sql/fun/IgniteOwnSqlOperatorTable"},
    "indexing": {"processors/query/h2/DistributedIndexingConfiguration"},
}

REGISTERED_PATHS = {f"modules/{module}{SOURCE_ROOT}{name}.java"
                    for module, classes in REGISTERED_SOURCES.items() for name in classes}


def supports_registered_contracts(path):
    return path in REGISTERED_PATHS
