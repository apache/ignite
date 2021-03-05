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

from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.utils.ignite_test import IgniteTest


# pylint: disable=W0223
class RebalanceTest(IgniteTest):
    """
    Contains common rebalance test definitions and methods.
    """
    PRELOAD_TIMEOUT = 5000
    REBALANCE_START_TIMEOUT = 1
    REBALANCE_COMPLETE_TIMEOUT = 300

    def preload_data(self, config, cache_count, entry_count, entry_size):
        """
        Puts entry_count of key-value pairs of entry_size bytes to cache_count caches.
        """
        start = self.monotonic()

        IgniteApplicationService(
            self.test_context,
            config=config,
            java_class_name="org.apache.ignite.internal.ducktest.tests.rebalance.DataGenerationApplication",
            params={"cacheCount": cache_count, "entryCount": entry_count, "entrySize": entry_size},
            startup_timeout_sec=self.PRELOAD_TIMEOUT
        ).run()

        return self.monotonic() - start

    def await_rebalance_start(self, ignite):
        """
        Awaits rebalance starting on some test-cache on any node.
        Returns first node on which rebalance start was detected.
        """
        for node in ignite.nodes:
            try:
                ignite.await_event_on_node(
                    "Starting rebalance routine \\[test-cache-",
                    node,
                    timeout_sec=self.REBALANCE_START_TIMEOUT,
                    from_the_beginning=True,
                    backoff_sec=1)
            except TimeoutError:
                continue
            else:
                return node

        raise RuntimeError("Rebalance start was not detected on any node")

    def await_rebalance_complete(self, ignite, node=None, cache_count=1):
        """
        Awaits rebalance complete on some test-cache
        """
        for cache_idx in range(cache_count):
            ignite.await_event_on_node(
                "Completed rebalance future: RebalanceFuture \\[%s \\[grp=test-cache-%d" %
                ("state=STARTED, grp=CacheGroupContext", cache_idx + 1),
                node if node else ignite.nodes[0],
                timeout_sec=self.REBALANCE_COMPLETE_TIMEOUT,
                from_the_beginning=True,
                backoff_sec=1)
