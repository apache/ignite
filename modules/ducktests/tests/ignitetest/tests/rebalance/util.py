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
from datetime import datetime
from enum import IntEnum
from itertools import chain, product
from typing import NamedTuple

from ducktape.errors import TimeoutError

from ignitetest.services.ignite import IgniteService
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.tests.util import DataGenerationParams
from ignitetest.utils.enum import constructible
from ignitetest.utils.version import IgniteVersion

NUM_NODES = 4


@constructible
class TriggerEvent(IntEnum):
    """
    Rebalance trigger event.
    """
    NODE_JOIN = 0
    NODE_LEFT = 1


class RebalanceParams(DataGenerationParams):
    """
    Rebalance parameters
    """
    trigger_event: TriggerEvent = TriggerEvent.NODE_JOIN
    thread_pool_size: int = None
    batch_size: int = None
    batches_prefetch_count: int = None
    throttle: int = None
    persistent: bool = False
    jvm_opts: list = None


class RebalanceMetrics(NamedTuple):
    """
    Rebalance metrics
    """
    received_bytes: int = 0
    start_time: int = 0
    end_time: int = 0
    duration: int = 0
    node: str = None


def start_ignite(test_context, ignite_version: str, rebalance_params: RebalanceParams) -> IgniteService:
    """
    Start IgniteService:

    :param test_context: Test context.
    :param ignite_version: Ignite version.
    :param rebalance_params: Rebalance parameters.
    :return: IgniteService.
    """
    node_count = test_context.available_cluster_size - rebalance_params.preloaders

    if rebalance_params.persistent:
        data_storage = DataStorageConfiguration(
            max_wal_archive_size=2 * rebalance_params.data_region_max_size,
            default=DataRegionConfiguration(
                persistence_enabled=True,
                max_size=rebalance_params.data_region_max_size
            )
        )
    else:
        data_storage = DataStorageConfiguration(
            default=DataRegionConfiguration(max_size=rebalance_params.data_region_max_size)
        )

    node_config = IgniteConfiguration(
        version=IgniteVersion(ignite_version),
        data_storage=data_storage,
        metric_exporters={"org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi"},
        rebalance_thread_pool_size=rebalance_params.thread_pool_size,
        rebalance_batch_size=rebalance_params.batch_size,
        rebalance_batches_prefetch_count=rebalance_params.batches_prefetch_count,
        rebalance_throttle=rebalance_params.throttle)

    ignites = IgniteService(test_context, config=node_config,
                            num_nodes=node_count if rebalance_params.trigger_event else node_count - 1,
                            jvm_opts=rebalance_params.jvm_opts)
    ignites.start()

    return ignites


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

        for prop, func in chain(product(['start_time', 'end_time'], [min, max]),
                                product(['duration', 'received_bytes'], [min, max, sum])):
            if func.__name__ == 'sum':
                val = func(map(lambda item: getattr(item, prop), metrics))
            else:
                val = func(map(lambda item: [item.node, getattr(item, prop)], metrics), key=lambda tup: tup[1])

            if prop in ['start_time', 'end_time']:
                val[1] = to_time_format(val[1])

            if prop == 'duration':
                if func.__name__ == 'sum':
                    val = f'{round(val / 1000, 3)} s.'
                else:
                    val[1] = f'{round(val[1] / 1000, 3)} s.'

            stats[prop][func.__name__] = val

        return stats

    return list(map(__stats, range(cache_count)))


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


def to_time_format(timestamp: int, fmt: str = '%Y-%m-%d %H:%M:%S'):
    """
    Convert timestamp to string using format.

    :param timestamp: Timestamp in ms
    :param fmt: Format.
    :return:
    """
    return datetime.fromtimestamp(int(timestamp) // 1000).strftime(fmt)


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
        "preload_time_sec": int(preload_time),
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
