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

"""Locate the rows and registrations needed to compare one built-in system view."""

_CORE = "modules/core/src/main/java/org/apache/ignite/"
_VIEWS = _CORE + "spi/systemview/view/"
_CODEGEN = "modules/codegen/src/main/java/org/apache/ignite/internal/systemview/"
VIEW_RULE_SOURCES = {_CODEGEN + name + ".java" for name in
                     ("SystemViewRowAttributeWalkerGenerator", "SystemViewRowAttributeWalkerProcessor",
                      "SystemViewDescriptor", "Order")}

# These locations select companion files. The Java check verifies registration and inheritance in both revisions.
_REGISTRATIONS = {
    "IgniteKernal": "ConfigurationView",
    "managers/discovery/GridDiscoveryManager": "ClusterNodeView NodeAttributeView NodeMetricsView",
    "processors/cache/ClusterCachesInfo": "CacheGroupView CacheView",
    "processors/cache/GridCacheMvccManager": "CacheExplicitLockView CacheLockView",
    "processors/cache/GridCacheSharedContext": "ScanQueryView",
    "processors/cache/GridCacheProcessor": "CacheGroupIoView CachePagesListView PartitionStateView",
    "processors/cache/binary/CacheObjectBinaryProcessorImpl": "BinaryMetadataView",
    "processors/cache/persistence/GridCacheDatabaseSharedManager": "MetastorageView",
    "processors/cache/persistence/IgniteCacheDatabaseSharedManager": "PagesListView PagesTimestampHistogramView",
    "processors/cache/persistence/snapshot/IgniteSnapshotManager": "SnapshotView",
    "processors/cache/transactions/IgniteTxManager": "TransactionView",
    "processors/cluster/GridClusterStateProcessor": "BaselineNodeAttributeView BaselineNodeView",
    "processors/continuous/GridContinuousProcessor": "ContinuousQueryView",
    "processors/datastructures/DataStructuresProcessor": (
        "datastructures/AtomicLongView datastructures/AtomicReferenceView datastructures/AtomicSequenceView "
        "datastructures/AtomicStampedView datastructures/CountDownLatchView datastructures/QueueView "
        "datastructures/ReentrantLockView datastructures/SemaphoreView datastructures/SetView"),
    "processors/job/GridJobProcessor": "ComputeJobView",
    "processors/metastorage/persistence/DistributedMetaStorageImpl": "MetastorageView",
    "processors/metric/SqlViewMetricExporterSpi": "MetricsView",
    "processors/odbc/ClientListenerProcessor": "ClientConnectionAttributeView ClientConnectionView",
    "processors/plugin/IgnitePluginProcessor": "PluginView",
    "processors/pool/PoolProcessor": "StripedExecutorTaskView",
    "processors/query/running/RunningQueryManager": "SqlPlanHistoryView SqlQueryHistoryView SqlQueryView",
    "processors/query/schema/management/SchemaManager": (
        "sql/SqlIndexView sql/SqlSchemaView sql/SqlTableColumnView sql/SqlTableView sql/SqlViewColumnView sql/SqlViewView"),
    "processors/query/stat/IgniteGlobalStatisticsManager": "internal/processors/query/stat/view/StatisticsColumnGlobalDataView",
    "processors/query/stat/IgniteStatisticsConfigurationManager": (
        "internal/processors/query/stat/view/StatisticsColumnConfigurationView"),
    "processors/query/stat/IgniteStatisticsRepository": (
        "internal/processors/query/stat/view/StatisticsColumnLocalDataView "
        "internal/processors/query/stat/view/StatisticsColumnPartitionDataView"),
    "processors/service/IgniteServiceProcessor": "ServiceView",
    "processors/task/GridTaskProcessor": "ComputeTaskView",
}

_PARENTS = {
    "CachePagesListView": "PagesListView",
    **{"datastructures/" + name + "View": "datastructures/AbstractDataStructureView" for name in
       ("AtomicLong", "AtomicReference", "AtomicSequence", "AtomicStamped", "CountDownLatch", "ReentrantLock", "Semaphore")},
}


def _row(name):
    return (_CORE if name.startswith("internal/") else _VIEWS) + name + ".java"


_REGISTRY_PATHS = {_CORE + "internal/" + name + ".java" for name in _REGISTRATIONS}
_ROWS = {_row(name) for names in _REGISTRATIONS.values() for name in names.split()} | {_row(p) for p in _PARENTS.values()}


_SCAN_WRAPPER = _CORE + "internal/managers/systemview/ScanQuerySystemView.java"
_SCAN_BASE = _CORE + "internal/managers/systemview/AbstractSystemView.java"


def supports_view_source(path):
    return path in _ROWS or path in _REGISTRY_PATHS or path in VIEW_RULE_SOURCES or path in {_SCAN_WRAPPER, _SCAN_BASE}


def view_companions(changed_paths):
    if changed_paths & VIEW_RULE_SOURCES:
        return set()
    affected = set()
    for registry, names in _REGISTRATIONS.items():
        for name in names.split():
            row = _row(name)
            family = {row}
            if name in _PARENTS:
                family.add(_row(_PARENTS[name]))
            if family & changed_paths:
                affected.update(family)
                affected.add(_CORE + "internal/" + registry + ".java")
    if _row("ScanQueryView") in affected:
        # The wrapper inherits name() and walker() from this base. A base change needs review.
        affected.add(_SCAN_BASE if _SCAN_BASE in changed_paths else _SCAN_WRAPPER)
    return affected
