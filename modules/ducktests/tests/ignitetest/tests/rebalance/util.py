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

"""
Utils for rebalanced tests.
"""

import sys
from enum import IntEnum
from typing import NamedTuple

# pylint: disable=W0622
from ducktape.errors import TimeoutError

# pylint: disable=too-many-arguments
from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.utils.enum import constructible
from ignitetest.utils.version import IgniteVersion

NUM_NODES = 4
DEFAULT_DATA_REGION_SZ = 1 << 30


@constructible
class TriggerEvent(IntEnum):
    """
    Rebalance trigger event.
    """
    NODE_JOIN = 0
    NODE_LEFT = 1


# pylint: disable=R0914
def preload_data(
        context, config, preloaders, backups, cache_count, entry_count, entry_size,
        preload_class_name="org.apache.ignite.internal.ducktest.tests.rebalance.DataGenerationApplicationStreamer",
        timeout=3600
):
    """
    Puts entry_count of key-value pairs of entry_size bytes to cache_count caches.
    :param context: Test context.
    :param config: Ignite configuration.
    :param preloaders: Preload client nodes count.
    :param backups: Cache backups count.
    :param cache_count: Cache count.
    :param entry_count: Cache entry count.
    :param entry_size: Entry size in bytes.
    :param preload_class_name: Preload java class name
    :param timeout: Timeout in seconds for application finished.
    :return: Time taken for data preloading.
    """
    assert preloaders > 0
    assert cache_count > 0
    assert entry_count > 0
    assert entry_size > 0

    apps = []

    def start_app(from_, to_):
        app0 = IgniteApplicationService(
            context,
            config=config,
            java_class_name=preload_class_name,
            params={
                "backups": backups,
                "cacheCount": cache_count,
                "entrySize": entry_size,
                "from": from_,
                "to": to_
            },
            shutdown_timeout_sec=timeout)
        app0.start_async()

        apps.append(app0)

    count = int(entry_count / preloaders)
    _from = 0
    _to = 0

    for _ in range(preloaders - 1):
        _from = _to
        _to += count
        start_app(_from, _to)

    start_app(_to, entry_count)

    for app1 in apps:
        app1.await_stopped()

    return (max(map(lambda app: app.get_finish_time(), apps)) -
            min(map(lambda app: app.get_init_time(), apps))).total_seconds()


def await_rebalance_start(service: IgniteService, timeout: int = 30):
    """
    Awaits rebalance starting on any test-cache on any node.
    :param service: IgniteService in which rebalance start will be awaited.
    :param timeout: Rebalance start await timeout.
    :return: dictionary of two keypairs with keys "node" and "time",
    where "node" contains the first node on which rebalance start was detected
    and "time" contains the time when rebalance was started.
    """
    for node in service.alive_nodes:
        try:
            rebalance_start_time = service.get_event_time_on_node(
                node,
                "Starting rebalance routine",
                timeout=timeout)
        except TimeoutError:
            continue
        else:
            return rebalance_start_time

    raise RuntimeError("Rebalance start was not detected on any node")


def get_rebalance_metrics(node, cache_group):
    """
    Gets rebalance metrics for specified node and cache group.
    :param node: Ignite node.
    :param cache_group: Cache group.
    :return: RebalanceMetrics instance.
    """
    mbean = node.jmx_client().find_mbean('.*group=cacheGroups.*name="%s"' % cache_group)
    start_time = int(next(mbean.RebalancingStartTime))
    end_time = int(next(mbean.RebalancingEndTime))

    return RebalanceMetrics(
        received_bytes=int(next(mbean.RebalancingReceivedBytes)),
        start_time=start_time,
        end_time=end_time,
        duration=(end_time - start_time) if start_time != -1 and end_time != -1 else 0,
        node=node.name)


class RebalanceMetrics(NamedTuple):
    """
    Rebalance metrics
    """
    received_bytes: int = 0
    start_time: int = 0
    end_time: int = 0
    duration: int = 0
    node: str = None


def aggregate_rebalance_stats(nodes, cache_count):
    """
    Aggregates rebalance stats for specified nodes and cache count.
    :param nodes: Nodes list.
    :param cache_count: Cache count.
    :return: Aggregated rebalance stats dictionary.
    """
    def __stats(cache_idx):
        cache_name = "test-cache-%d" % (cache_idx + 1)

        stats = {
            "cache": cache_name,
            "start_time": {},
            "end_time": {},
            "duration": {},
            "received_bytes": {}
        }

        metrics = list(map(lambda node: get_rebalance_metrics(node, cache_name), nodes))

        def __key(tup):
            return tup[1]

        stats["start_time"]["min"] = min(map(lambda item: (item.node, item.start_time), metrics), key=__key)
        stats["start_time"]["max"] = max(map(lambda item: (item.node, item.start_time), metrics), key=__key)
        stats["end_time"]["min"] = min(map(lambda item: (item.node, item.end_time), metrics), key=__key)
        stats["end_time"]["max"] = max(map(lambda item: (item.node, item.end_time), metrics), key=__key)
        stats["duration"]["min"] = min(map(lambda item: (item.node, item.duration), metrics), key=__key)
        stats["duration"]["max"] = max(map(lambda item: (item.node, item.duration), metrics), key=__key)
        stats["duration"]["sum"] = sum(map(lambda item: item.duration, metrics))
        stats["received_bytes"]["min"] = min(map(lambda item: (item.node, item.received_bytes), metrics), key=__key)
        stats["received_bytes"]["max"] = max(map(lambda item: (item.node, item.received_bytes), metrics), key=__key)
        stats["received_bytes"]["sum"] = sum(map(lambda item: item.received_bytes, metrics))

        return stats

    return list(map(__stats, range(cache_count)))


# pylint: disable=too-many-arguments, too-many-locals
def start_ignite(test_context, ignite_version, trigger_event, backups, cache_count, entry_count, entry_size, preloaders,
                 thread_pool_size, batch_size, batches_prefetch_count, throttle, persistent: bool = False):

    """
    Start IgniteService:

    :param test_context: Test context.
    :param ignite_version: Ignite version.
    :param trigger_event: Trigger event.
    :param backups: Backup count.
    :param cache_count: Cache count.
    :param entry_count: Cache entry count.
    :param entry_size: Cache entry size.
    :param preloaders: Preload application nodes count.
    :param thread_pool_size: rebalanceThreadPoolSize config property.
    :param batch_size: rebalanceBatchSize config property.
    :param batches_prefetch_count: rebalanceBatchesPrefetchCount config property.
    :param throttle: rebalanceThrottle config property.
    :param persistent: Persistent enabled.
    :return: IgniteService.
    """
    node_count = len(test_context.cluster) - preloaders

    if persistent:
        data_storage = DataStorageConfiguration(
            max_wal_archive_size=max(cache_count * entry_count * entry_size * (backups + 1), DEFAULT_DATA_REGION_SZ),
            default=DataRegionConfiguration(
                persistent=True,
                max_size=max(cache_count * entry_count * entry_size * (backups + 1), DEFAULT_DATA_REGION_SZ),
            )
        )
    else:
        data_storage = DataStorageConfiguration(
            default=DataRegionConfiguration(
                max_size=max(cache_count * entry_count * entry_size * (backups + 1), DEFAULT_DATA_REGION_SZ)))

    node_config = IgniteConfiguration(
        version=IgniteVersion(ignite_version),
        data_storage=data_storage,
        metric_exporter="org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi",
        rebalance_thread_pool_size=thread_pool_size,
        rebalance_batch_size=batch_size,
        rebalance_batches_prefetch_count=batches_prefetch_count,
        rebalance_throttle=throttle)

    ignites = IgniteService(test_context, config=node_config,
                            num_nodes=node_count if trigger_event else node_count - 1)
    ignites.start()

    return ignites


# pylint: disable=too-many-arguments, too-many-locals
def get_result(rebalance_nodes: list, preload_time: int, cache_count: int, entry_count: int, entry_size: int) -> dict:
    """

    :param rebalance_nodes: Ignite nodes in which rebalance will be awaited.
    :param preload_time: Preload time.
    :param cache_count: Cache count.
    :param entry_count: Cache entry count.
    :param entry_size: Cache entry size.
    :return: Rebalance result with aggregated rebalance stats dictionary
    """

    stats = aggregate_rebalance_stats(rebalance_nodes, cache_count)

    return {
        "rebalance_nodes": len(rebalance_nodes),
        "rebalance_stats": stats,
        "preload_time": int(preload_time * 1000),
        "preloaded_bytes": cache_count * entry_count * entry_size
    }


def check_type_of_rebalancing(rebalance_nodes: list, is_full: bool = True):
    """
    Check the type of rebalancing on node.

    :param rebalance_nodes: Ignite nodes in which rebalance will be awaited.
    :param is_full: Expected type of rebalancing.
    """

    for node in rebalance_nodes:
        output = node.account.ssh_output(f'grep "Starting rebalance routine" {node.log_file}', allow_fail=False,
                                         combine_stderr=False) \
            .decode(sys.getdefaultencoding()) \
            .splitlines()

        msg = 'histPartitions=[]' if is_full else 'fullPartitions=[]'

        for i in output:
            assert msg in i, i

        return output
