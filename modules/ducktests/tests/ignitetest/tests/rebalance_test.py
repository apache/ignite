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
Module contains rebalance tests.
"""

from enum import IntEnum

from ducktape.mark import matrix, defaults

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.enum import constructible
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import IgniteVersion, DEV_BRANCH, LATEST


@constructible
class Mode(IntEnum):
    """
    Rebalance mode.
    """
    IN_MEMORY = 0
    PERSISTENT = 1


@constructible
class TriggerEvent(IntEnum):
    """
    Rebalance trigger event.
    """
    NODE_ENTER = 0
    NODE_LEAVE = 1


# pylint: disable=W0223
class RebalanceTest(IgniteTest):
    """
    Tests rebalance scenarios.
    """
    NUM_NODES = 10
    PRELOAD_TIMEOUT = 60
    REBALANCE_TIMEOUT = 60

    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @defaults(initial_node_count=[2, 4, 8],
              trigger_event=[TriggerEvent.NODE_ENTER, TriggerEvent.NODE_LEAVE],
              mode=[Mode.IN_MEMORY], cache_count=[1], entry_count=[10000], entry_size=[1000])
    def test_rebalance(self, ignite_version, initial_node_count, trigger_event,
                       mode, cache_count, entry_count, entry_size):
        """
        Test performs rebalance test which consists of following steps:
            * Start cluster.
            * Put data to it via IgniteClientApp.
            * Triggering a rebalance event (node enter or leave) and awaits for rebalance to finish.
        """
        node_config = IgniteConfiguration(version=IgniteVersion(ignite_version))

        node_count = initial_node_count
        if trigger_event is TriggerEvent.NODE_LEAVE:
            node_count -= 1

        ignites = IgniteService(self.test_context, config=node_config, num_nodes=node_count)
        ignites.start()

        ignite = IgniteService(self.test_context, node_config._replace(discovery_spi=from_ignite_cluster(ignites)),
                               num_nodes=1)
        if trigger_event is TriggerEvent.NODE_LEAVE:
            ignite.start()

        # This client just put some data to the cache.
        app_config = node_config._replace(client_mode=True, discovery_spi=from_ignite_cluster(ignites))
        IgniteApplicationService(
            self.test_context,
            config=app_config,
            java_class_name="org.apache.ignite.internal.ducktest.tests.DataModelGenerationApplication",
            params={"cacheCount": cache_count, "entryCount": entry_count, "entrySize": entry_size},
            startup_timeout_sec=self.PRELOAD_TIMEOUT
        ).run()

        if trigger_event is TriggerEvent.NODE_LEAVE:
            ignite.stop()
        else:
            ignite.start()

        start = self.monotonic()

        ignites.await_event("rebalanced=true, wasRebalanced=false",
                            timeout_sec=RebalanceTest.REBALANCE_TIMEOUT,
                            from_the_beginning=True,
                            backoff_sec=1)

        data = {"Rebalanced in (sec)": self.monotonic() - start}

        return data
