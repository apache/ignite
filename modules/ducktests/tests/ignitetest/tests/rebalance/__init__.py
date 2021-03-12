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

# pylint: disable=W0622
from ducktape.errors import TimeoutError

from ignitetest.services.ignite import get_event_time
from ignitetest.services.ignite_app import IgniteApplicationService


# pylint: disable=too-many-arguments
def preload_data(context, config, backups, cache_count, entry_count, entry_size, timeout=3600):
    """
    Puts entry_count of key-value pairs of entry_size bytes to cache_count caches.
    :param context: Test context.
    :param config: Ignite configuration.
    :param backups: Cache backups count.
    :param cache_count: Cache count.
    :param entry_count: Cache entry count.
    :param entry_size: Entry size in bytes.
    :param timeout: Timeout in seconds for application finished.
    :return: Time taken for data preloading.
    """
    app = IgniteApplicationService(
        context,
        config=config,
        java_class_name="org.apache.ignite.internal.ducktest.tests.rebalance.DataGenerationApplication",
        params={"backups": backups, "cacheCount": cache_count, "entryCount": entry_count, "entrySize": entry_size},
        startup_timeout_sec=timeout)
    app.run()

    return (get_event_time(
        app, app.nodes[0], "Data generation finished") - get_event_time(
        app, app.nodes[0], "Data generation started")).total_seconds()


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
            return {"node": node, "time": rebalance_start_time}

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
