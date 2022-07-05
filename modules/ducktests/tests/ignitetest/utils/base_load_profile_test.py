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
This module contains basic load profile test template.
"""
from ignitetest.services.utils.control_utility import ControlUtility

from ignitetest.services.gatling.gatling import GatlingService
from ignitetest.services.ignite import IgniteService
from ignitetest.services.utils import IgniteServiceType
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, IgniteThinClientConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.services.utils.ssl.client_connector_configuration import ClientConnectorConfiguration
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import IgniteVersion


class BaseLoadProfileTest(IgniteTest):
    """
    Basic test for load profile execution.
    """

    def get_server_config(self, ignite_version):
        """
        Creates the configuration of server nodes. Assumed to be overridden in subclasses.

        :param ignite_version: Ignite version
        :return: Ignite configuration instance
        """
        return IgniteConfiguration(version=IgniteVersion(ignite_version),
                                   client_connector_configuration=ClientConnectorConfiguration())

    @property
    def get_server_jvm_opts(self):
        """
        :return: JVM options for server nodes.
        """
        return []

    def get_client_config(self, ignite, client_type):
        """
        Creates the configuration of client nodes. Assumed to be overridden in subclasses.

        :param ignite: Ignite service instance
        :param client_type: Type of the client (thin or thick)
        :return: Client configuration instance
        """
        if client_type == IgniteServiceType.THIN_CLIENT:
            addresses = ignite.nodes[0].account.hostname + ":" + str(ignite.config.client_connector_configuration.port)
            return IgniteThinClientConfiguration(addresses=addresses)
        else:
            return ignite.config._replace(client_mode=True, discovery_spi=from_ignite_cluster(ignite))

    @property
    def get_client_jvm_opts(self):
        """
        :return: JVM options for server nodes.
        """
        return []

    def execute_load_profile(self, profile, ignite_version, client_type, rps, duration, client_nodes, timeout_sec=600):
        """
        Executes the load profile.

        :param profile: Scala class name of the profile
        :param ignite_version: Ignite version
        :param client_type: Type of Ignite client to run gatling simulation (thin or thick)
        :param rps: Base requests-per-second parameter
        :param duration: Duration of simulation in seconds
        :param client_nodes: Number of client nodes to be used in simulation. All other available nodes in cluster
                             will be used for server instances.
        :param timeout_sec: Time to wait for the completion.
        """
        server_config = self.get_server_config(ignite_version)

        ignite = IgniteService(self.test_context, server_config, self.available_cluster_size - client_nodes,
                               jvm_opts=self.get_server_jvm_opts)

        client_config = self.get_client_config(ignite, client_type)

        gatling = GatlingService(
            self.test_context,
            client_config,
            params={
                "simulation": "org.apache.ignite.internal.ducktest.gatling.simulation.ProfileAwareSimulation",
                "options": {"profile": profile}
            },
            num_nodes=client_nodes,
            jvm_opts=self.get_client_jvm_opts)

        ignite.start()

        control_utility = ControlUtility(ignite)
        control_utility.activate()

        gatling.params["options"]["step"] = "ddl"
        gatling.run_with_timeout(timeout_sec)

        gatling.params["options"]["step"] = "data"
        gatling.run_with_timeout(timeout_sec)

        gatling.params["options"]["step"] = "test"
        gatling.params["options"]["rps"] = rps
        gatling.params["options"]["duration"] = duration
        gatling.run_with_timeout(timeout_sec)

        gatling.generate_report()

        ignite.stop()
