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
This package contains rebalance tests.
"""

from typing import NamedTuple

# pylint: disable=W0622
from ducktape.errors import TimeoutError

from ignitetest.services.ignite_app import IgniteApplicationService


# pylint: disable=too-many-arguments
def preload_data(context, config, preloaders, backups, cache_count, entry_count, entry_size, timeout=3600):
    """
    Puts entry_count of key-value pairs of entry_size bytes to cache_count caches.
    :param context: Test context.
    :param config: Ignite configuration.
    :param preloaders: Preload client nodes count.
    :param backups: Cache backups count.
    :param cache_count: Cache count.
    :param entry_count: Cache entry count.
    :param entry_size: Entry size in bytes.
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
            java_class_name="org.apache.ignite.internal.ducktest.tests.rebalance.DataGenerationApplication",
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


def await_rebalance_start(ignite, timeout=1):
    """
    Awaits rebalance starting on any test-cache on any node.
    :param ignite: IgniteService instance.
    :param timeout: Rebalance start await timeout.
    :return: dictionary of two keypairs with keys "node" and "time",
    where "node" contains the first node on which rebalance start was detected
    and "time" contains the time when rebalance was started.
    """
    for node in ignite.nodes:
        try:
            rebalance_start_time = ignite.get_event_time_on_node(
                node, "Starting rebalance routine \\[test-cache-",
                timeout=timeout)
        except TimeoutError:
            continue
        else:
            return node, rebalance_start_time

    raise RuntimeError("Rebalance start was not detected on any node")


def await_rebalance_complete(ignite, node=None, cache_count=1, timeout=300):
    """
    Awaits rebalance complete on each test-cache.
    :param ignite: IgniteService instance.
    :param node: Ignite node in which rebalance will be awaited. If None, the first node in ignite will be used.
    :param cache_count: The count of test caches to wait for rebalance completion.
    :param timeout: Rebalance completion timeout.
    :return: The time of rebalance completion.
    """
    rebalance_complete_times = []

    for cache_idx in range(cache_count):
        rebalance_complete_times.append(ignite.get_event_time_on_node(
            node if node else ignite.nodes[0],
            "Completed rebalance future: RebalanceFuture \\[%s \\[grp=test-cache-%d" %
            ("state=STARTED, grp=CacheGroupContext", cache_idx + 1),
            timeout=timeout))

    return max(rebalance_complete_times)


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
