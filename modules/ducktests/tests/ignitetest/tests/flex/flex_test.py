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
Module contains discovery tests.
"""

import os
import random
from ducktape.mark import matrix
from enum import IntEnum
from time import monotonic
from typing import NamedTuple

from ignitetest.services.ignite import IgniteService
from ignitetest.services.utils.ignite_aware import node_failed_event_pattern
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.services.utils.ignite_configuration.cache import CacheConfiguration
from ignitetest.utils import cluster
from ignitetest.utils.bean import Bean
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import LATEST_2_17, IgniteVersion


class FlexTest(IgniteTest):
    SERVERS = 3
    SERVER_IDX_TO_DROP = 1
    IGNITE_VERSION = LATEST_2_17
    FAILURE_DETECTION_TIMEOUT_SEC = 7

    @cluster(num_nodes=SERVERS + 1)
    def flex_test(self):
        cacheAffinity = Bean("org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction", partitions=512)

        ignite_config = IgniteConfiguration(
            version=self.IGNITE_VERSION,
            metrics_log_frequency = 0,
            failure_detection_timeout=self.FAILURE_DETECTION_TIMEOUT_SEC * 1000,
            caches=[CacheConfiguration(
                name='TBG_SCS_DM_DOCUMENTS',
                atomicity_mode='TRANSACTIONAL',
                affinity = cacheAffinity,
                cache_mode = 'REPLICATED'
            )]
        )

        servers, start_servers_sec = start_servers(self.test_context, self.SERVERS, ignite_config)

        servers.await_event(f"servers={self.SERVERS}, clients=0, state=ACTIVE, CPUs=", 30, from_the_beginning=True,
                            nodes=servers.nodes)

        failedNode = servers.nodes[self.SERVER_IDX_TO_DROP]
        failedNodeId = servers.node_id(failedNode)

        self.logger.info(f"'kill -9' node {self.SERVER_IDX_TO_DROP} with id {failedNodeId} ...")

        servers.stop_node(failedNode, force_stop=True)

        self.logger.info("Awaiting for the node-failed-event ...")
        servers.await_event(node_failed_event_pattern(failedNodeId), self.FAILURE_DETECTION_TIMEOUT_SEC * 3,
                            from_the_beginning=True, nodes=[servers.nodes[0], servers.nodes[2]])

        self.logger.info("Awaiting for the new cluster state ...")
        servers.await_event(f"servers={self.SERVERS - 1}, clients=0, state=ACTIVE, CPUs=", self.FAILURE_DETECTION_TIMEOUT_SEC * 3,
                            from_the_beginning=True, nodes=[servers.nodes[0], servers.nodes[2]])

        self.logger.info("The cluster has detected the node failure.")

        servers.stop()

def start_servers(test_context, num_nodes, ignite_config, modules=None):
    """
    Start ignite servers.
    """
    servers = IgniteService(test_context, config=ignite_config, num_nodes=num_nodes, modules=modules,
                            # mute spam in log.
                            jvm_opts=["-DIGNITE_DUMP_THREADS_ON_FAILURE=false"])

    start = monotonic()
    servers.start_async()
    return servers, round(monotonic() - start, 1)