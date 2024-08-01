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
Module contains DNS service failure test.
"""
import socket

from ducktape.mark import defaults
from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_aware import IgniteAwareService
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.utils import ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import IgniteVersion, DEV_BRANCH
from ignitetest.utils import cluster


class DnsFailureTest(IgniteTest):
    """
    Test DNS service failure.
    """

    @cluster(num_nodes=4)
    @ignite_versions(str(DEV_BRANCH))
    @defaults(fail=[True, False])
    def dns_failure_test(self, ignite_version, fail):
        """
        DNS failure test.
        """
        # Replace hosts with IP addresses.
        for node in self.test_context.cluster.nodes:
            node.account.externally_routable_ip = socket.gethostbyname(node.account.externally_routable_ip)

        version = IgniteVersion(ignite_version)

        ignite_config = IgniteConfiguration(
            version=version,
            data_storage=DataStorageConfiguration(
                default=DataRegionConfiguration(
                    persistence_enabled=True
                )
            )
        )

        ignites = self.__prepare_service(ignite_config, 3)

        self.__unblock_dns(ignites)

        # Start nodes one-by-one to reproduce the problem.
        ignites.start_node(ignites.nodes[0])
        ignites.await_started([ignites.nodes[0]])
        ignites.start_node(ignites.nodes[1])
        ignites.await_started([ignites.nodes[0], ignites.nodes[1]])
        ignites.start_node(ignites.nodes[2])
        ignites.await_started()

        control_utility = ControlUtility(ignites)
        control_utility.activate()

        self.__block_dns(ignites, 20000, fail)

        ignites.stop_node(ignites.nodes[1])

        ignites.await_event("Node left topology", 60, from_the_beginning=True,
                            nodes=[ignites.nodes[0], ignites.nodes[2]])

        assert ignites.alive(ignites.nodes[0]), 'Node 0 should be alive'
        assert ignites.alive(ignites.nodes[2]), 'Node 2 should be alive'

        self.__unblock_dns(ignites, [ignites.nodes[1]])

        ignites.start_node(ignites.nodes[1])
        ignites.await_started()

        assert len(ignites.alive_nodes) == 3, 'All nodes should be alive'

        # Smoke test on full topology.
        app = IgniteApplicationService(
            self.test_context,
            ignite_config._replace(client_mode=True, discovery_spi=from_ignite_cluster(ignites)),
            startup_timeout_sec=120,
            java_class_name="org.apache.ignite.internal.ducktest.tests.smoke_test.SimpleApplication")

        app.start()
        app.stop()

        ignites.stop()

    def __prepare_service(self, ignite_config, num_nodes=1):
        ignite = IgniteService(
            self.test_context,
            ignite_config,
            startup_timeout_sec=120,
            num_nodes=num_nodes,
            main_java_class="org.apache.ignite.internal.ducktest.tests.dns_failure_test.BlockingNameService")

        return ignite

    @staticmethod
    def __block_dns(ignite, timeout, fail, nodes=None):
        if nodes is None:
            nodes = ignite.nodes

        for node in nodes:
            _, err = IgniteAwareService.exec_command_ex(node, f"echo {timeout} {fail} > /tmp/block_dns")

    @staticmethod
    def __unblock_dns(ignite, nodes=None):
        if nodes is None:
            nodes = ignite.nodes

        for node in nodes:
            _, err = IgniteAwareService.exec_command_ex(node, "rm /tmp/block_dns")
