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
    IgniteThinJdbcConfiguration, TransactionConfiguration
from ignitetest.services.utils.ignite_configuration.cache import CacheConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.services.utils.ssl.client_connector_configuration import ClientConnectorConfiguration
from ignitetest.utils import cluster
from ignitetest.utils.bean import Bean
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import LATEST_2_17, DEV_BRANCH


class MexTest(IgniteTest):
    PRELOAD_SECONDS = 50
    LOAD_SECONDS = PRELOAD_SECONDS / 3
    LOAD_THREADS = 8
    SERVERS = 3
    SERVER_IDX_TO_DROP = 1
    IGNITE_VERSION = DEV_BRANCH
    CACHE_NAME = "TEST_CACHE"
    TABLE_NAME = "TEST_TABLE"

    @cluster(num_nodes=SERVERS * 2)
    def mex_test(self):
        # Start the servers.
        servers, control_utility, ignite_config = self.launch_cluster()

        # Start the loading app. ant wait for some records preloaded.
        app = self.start_load_app(servers, ignite_config.client_connector_configuration.port)

        # The loading is on. Now, kill a server node.
        self.kill_node(servers)

        # Keep the loading a bit longer.
        self.logger.info(f"TEST | Loading the cluster for additional {self.LOAD_SECONDS} seconds...")
        time.sleep(self.LOAD_SECONDS)

        # Stop the loading.
        self.logger.debug("TEST | Stopping the load application ...")
        app.stop()
        app.await_stopped()

        records_cnt = set()

        for node in self.alive_servers(servers.nodes):
            app = self.start_cnt_app(node, ignite_config.client_connector_configuration.port)
            app.await_started()

            cnt = app.extract_result("tableRowsCnt")
            records_cnt.add(cnt)
            self.logger.info(f"TEST | Partitions cnt on node {node}: {cnt}")

            app.stop()
            app.await_stopped()

        # Run the idle verify.
        self.logger.info("TEST | The load application has stopped.")
        output = control_utility.idle_verify(self.CACHE_NAME)
        self.logger.info(f"TEST | Idle verify finished: {output}")

        # Compare the rows cnt results.
        assert len(records_cnt) == 1;

        # Finish the test.
        servers.stop()

    def kill_node(self, servers):
        failedNode = servers.nodes[self.SERVER_IDX_TO_DROP]
        failedNodeId = servers.node_id(failedNode)
        alive_servers = self.alive_servers(servers.nodes)

        self.logger.info(f"TEST | 'kill -9' node {self.SERVER_IDX_TO_DROP} with id {failedNodeId} ...")

        servers.stop_node(failedNode, force_stop=True)

        self.logger.debug("TEST | Awaiting for the node-failed-event...")
        servers.await_event(node_failed_event_pattern(failedNodeId), 30, from_the_beginning=True, nodes = alive_servers)

        self.logger.debug("TEST | Awaiting for the new cluster state...")
        servers.await_event(f"servers={self.SERVERS - 1}, clients=0, state=ACTIVE, CPUs=", 30, from_the_beginning=True,
                            nodes = alive_servers)

        self.logger.info("TEST | The cluster has detected the node failure.")

    def alive_servers(self, nodes):
        return [item for index, item in enumerate(nodes) if index != self.SERVER_IDX_TO_DROP]

    def alive_servers_addrs(self, nodes, jdbcPort):
        nodes = self.alive_servers(nodes)

        jdbcPort = str(jdbcPort)

        return [n.account.hostname + ":" + jdbcPort for n in nodes]

    def start_load_app(self, servers, jdbcPort):
        addrs = self.alive_servers_addrs(servers.nodes, jdbcPort)

        app = IgniteApplicationService(
            self.test_context,
            IgniteThinJdbcConfiguration(version=self.IGNITE_VERSION, addresses=addrs),
            java_class_name="org.apache.ignite.internal.ducktest.tests.mex.MexLoadApplication",
            num_nodes=1,
            params={"preloadDurSec": self.PRELOAD_SECONDS, "threads": self.LOAD_THREADS, "cacheName": self.CACHE_NAME,
                    "tableName" : self.TABLE_NAME},
            startup_timeout_sec=self.PRELOAD_SECONDS + 10,
            jvm_opts="-Xms4G -Xmx4G"
        )

        self.logger.debug("TEST | Starting the loading application...")
        app.start()

        self.logger.debug("TEST | Waiting for the load application initialization...")
        app.await_started()

        self.logger.info("TEST | The load application has initialized.")

        return app

    def start_cnt_app(self, node, jdbcPort):
        jdbcPort = str(jdbcPort)

        addrs = [node.account.hostname + ":" + jdbcPort]

        app = IgniteApplicationService(
            self.test_context,
            IgniteThinJdbcConfiguration(version=self.IGNITE_VERSION, addresses=addrs),
            java_class_name="org.apache.ignite.internal.ducktest.tests.mex.MexCntApplication",
            num_nodes=1,
            params={"tableName" : self.TABLE_NAME},
            startup_timeout_sec=10,
            jvm_opts="-Xms4G -Xmx4G"
        )

        self.logger.debug(f"TEST | Starting the counter application to server node {node}...")
        app.start()

        self.logger.debug(f"TEST | Waiting for the counter application initialization to server node {node}...")
        app.await_started()

        self.logger.info(f"TEST | The counter application has initialized to server node {node}.")

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
                    persistence_enabled = False,

                    # initial_size = 128 * 1024 * 1024,
                    # max_size = 256 * 1024 * 1024,

                    initial_size = 4096 * 1024 * 1024,
                    max_size = 4096 * 1024 * 1024,

                    # initial_size = 2147483648,
                    # max_size = 17179869184,
                )
            ),
            cluster_state = 'ACTIVE',
            client_connector_configuration=ClientConnectorConfiguration(),
            transaction_configuration=TransactionConfiguration(
                default_tx_timeout=300000,
                default_tx_isolation="READ_COMMITTED",
                tx_timeout_on_partition_map_exchange=120000),
        )

        servers, start_servers_sec = start_servers(self.test_context, self.SERVERS, ignite_config)

        servers.await_event(f"Topology snapshot \\[ver={self.SERVERS}", 15, from_the_beginning=True, nodes=servers.nodes)

        control_utility = ControlUtility(servers)
        control_utility.activate()

        return servers, control_utility, ignite_config

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