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

from datetime import datetime
from typing import NamedTuple

# pylint: disable=W0622
from ducktape.errors import TimeoutError

from ignitetest.services.ignite import get_event_time
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

    for start_key, range_size in __ranges__(preloaders, entry_count):
        app0 = IgniteApplicationService(
            context,
            config=config,
            java_class_name="org.apache.ignite.internal.ducktest.tests.rebalance.DataGenerationApplication",
            params={
                "backups": backups,
                "cacheCount": cache_count,
                "entryCount": range_size,
                "entrySize": entry_size,
                "startKey": start_key
            },
            shutdown_timeout_sec=timeout)
        app0.start_async()

        apps.append(app0)

    for app1 in apps:
        app1.await_stopped()

    return (max(map(lambda app: app.get_finish_time(), apps)) -
            min(map(lambda app: app.get_init_time(), apps))).total_seconds()


def __ranges__(preloaders, entry_count):
    range_size = int(entry_count / preloaders)
    extra_size = __extra__(entry_count % preloaders)
    start_key = 0

    while start_key < entry_count:
        range_bound = start_key + range_size + next(extra_size)
        yield start_key, range_bound - start_key
        start_key = range_bound


def __extra__(extra):
    while True:
        yield 1 if extra else 0

        if extra:
            extra -= 1


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
            rebalance_start_time = get_event_time(
                ignite, node,
                "Starting rebalance routine \\[test-cache-",
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
        rebalance_complete_times.append(get_event_time(
            ignite,
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
    start_time = to_datetime(int(next(mbean.RebalancingStartTime)))
    end_time = to_datetime(int(next(mbean.RebalancingEndTime)))

    return RebalanceMetrics(
        received_bytes=int(next(mbean.RebalancingReceivedBytes)),
        start_time=start_time,
        end_time=end_time,
        duration=(end_time - start_time).total_seconds() if start_time and end_time else 0)


def to_datetime(timestamp):
    """
    Converts timestamp in millicesonds to datetime.
    :param timestamp: Timestamp in milliseconds.
    :return: datetime constructed from timestamp or None if ts == -1.
    """
    return None if timestamp == -1 else datetime.fromtimestamp(timestamp / 1000.0)


class RebalanceMetrics(NamedTuple):
    """
    Rebalance metrics
    """
    received_bytes: int = 0
    start_time: datetime = None
    end_time: datetime = None
    duration: float = 0


def aggregate_rebalance_stats(nodes, cache_count):
    """
    Aggregates rebalance stats for specified nodes and cache count:
    received_bytes -> sum(all of received_bytes)
    start_time -> min(all of start_time)
    end_time -> max(all of end_time)
    duration -> sum(all of duration)
    :param nodes: Nodes list.
    :param cache_count: Cache count.
    :return: RebalanceMetrics instance with aggregated values.
    """
    received_bytes = 0
    start_time = None
    end_time = None
    duration = 0

    for node in nodes:
        for cache_idx in range(cache_count):
            metrics = get_rebalance_metrics(node, "test-cache-%d" % (cache_idx + 1))
            received_bytes += metrics.received_bytes
            if metrics.start_time is not None:
                start_time = min(t for t in [start_time, metrics.start_time] if t is not None)
            if metrics.end_time is not None:
                end_time = max(t for t in [end_time, metrics.end_time] if t is not None)
            duration += metrics.duration

    return RebalanceMetrics(
        received_bytes=received_bytes,
        start_time=start_time,
        end_time=end_time,
        duration=duration)
