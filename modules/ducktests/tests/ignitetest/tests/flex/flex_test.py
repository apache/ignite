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
Module contains Flex tests.
"""

import os
import random
import time
from ducktape.mark import matrix
from enum import IntEnum
from typing import NamedTuple

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_aware import node_failed_event_pattern
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration, \
    IgniteThinJdbcConfiguration
from ignitetest.services.utils.ignite_configuration.cache import CacheConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.services.utils.ssl.client_connector_configuration import ClientConnectorConfiguration
from ignitetest.utils import cluster
from ignitetest.utils.bean import Bean
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import LATEST_2_17

class FlexTest(IgniteTest):
    SERVERS = 3
    SERVER_IDX_TO_DROP = 1
    PRELOAD_SECONDS = 120
    LOAD_SECONDS = 40
    LOAD_THREADS = 8
    IGNITE_VERSION = LATEST_2_17
    CACHE_NAME = "TBG_SCS_DM_DOCUMENTS"
    TABLE_NAME = "SCS_DM_DOCUMENTS"

    @cluster(num_nodes=SERVERS + 1)
    def flex_test(self):
        servers, ignite_config = self.launch_cluster()

        control_utility = ControlUtility(servers)
        control_utility.activate()

        load_app = self.start_load_app(servers, ignite_config.client_connector_configuration.port)

        self.logger.info(f"TEST | Loading the cluster for {self.LOAD_SECONDS} seconds...")

        time.sleep(self.LOAD_SECONDS)

        self.kill_node(servers)

        self.logger.info("TEST | Stopping the load application ...")

        load_app.stop()
        load_app.await_stopped()

        self.logger.info("TEST | The load application has stopped.")

        output = control_utility.idle_verify(self.CACHE_NAME)

        self.logger.info(f"TEST | Idle verify finished: {output}")

        servers.stop()

    def kill_node(self, servers):
        failedNode = servers.nodes[self.SERVER_IDX_TO_DROP]
        failedNodeId = servers.node_id(failedNode)

        self.logger.info(f"TEST | 'kill -9' node {self.SERVER_IDX_TO_DROP} with id {failedNodeId} ...")

        servers.stop_node(failedNode, force_stop=True)

        self.logger.info("TEST | Awaiting for the node-failed-event...")
        servers.await_event(node_failed_event_pattern(failedNodeId), 30, from_the_beginning=True,
                            nodes=[servers.nodes[0], servers.nodes[2]])

        self.logger.info("TEST | Awaiting for the new cluster state...")
        servers.await_event(f"servers={self.SERVERS - 1}, clients=0, state=ACTIVE, CPUs=", 30, from_the_beginning=True,
                            nodes=[servers.nodes[0], servers.nodes[2]])

        self.logger.info("TEST | The cluster has detected the node failure.")

    def start_load_app(self, servers, jdbcPort):
        jdbcPort = str(jdbcPort)

        addrs = [servers.nodes[0].account.hostname + ":" + jdbcPort, servers.nodes[1].account.hostname + ":" + jdbcPort,
                 servers.nodes[2].account.hostname + ":" + jdbcPort]

        app = IgniteApplicationService(
            self.test_context,
            IgniteThinJdbcConfiguration(version=self.IGNITE_VERSION, addresses=addrs),
            java_class_name="org.apache.ignite.internal.ducktest.tests.flex.FlexLoadApplication",
            num_nodes=1,
            params={"preloadDurSec": self.PRELOAD_SECONDS, "threads": self.LOAD_THREADS, "cacheName": self.CACHE_NAME,
                    "tableName" : self.TABLE_NAME},
            startup_timeout_sec=self.PRELOAD_SECONDS + 10
        )

        self.logger.info("TEST | Starting the loading application...")

        app.start()

        self.logger.info("TEST | Waiting for the load application initialization...")

        app.await_started()

        self.logger.info("TEST | The load application has initialized.")

        return app

    def launch_cluster(self):
        cacheAffinity = Bean("org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction", partitions=512)

        ignite_config = IgniteConfiguration(
            version=self.IGNITE_VERSION,
            metrics_log_frequency = 0,
            caches=[CacheConfiguration(
                name=self.CACHE_NAME,
                atomicity_mode='TRANSACTIONAL',
                affinity = cacheAffinity,
                cache_mode = 'REPLICATED'
            )],
            data_storage=DataStorageConfiguration(
                default=DataRegionConfiguration(
                    initial_size = 256 * 1024 * 1024,
                    max_size = 1024 * 1024 * 1024,
                    #persistence_enabled = True
                )
            ),
            client_connector_configuration=ClientConnectorConfiguration()
        )

        servers, start_servers_sec = start_servers(self.test_context, self.SERVERS, ignite_config)

        servers.await_event(f"servers={self.SERVERS}, clients=0, state=ACTIVE, CPUs=", 30, from_the_beginning=True,
                            nodes=servers.nodes)

        return servers, ignite_config

def start_servers(test_context, num_nodes, ignite_config, modules=None):
    """
    Start ignite servers.
    """
    servers = IgniteService(test_context, config=ignite_config, num_nodes=num_nodes, modules=modules,
                            # mute spam in log.
                            jvm_opts=["-DIGNITE_DUMP_THREADS_ON_FAILURE=false"])

    start = time.monotonic()
    servers.start_async()
    return servers, round(time.monotonic() - start, 1)