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
This module contains client tests.
"""
import time

from jinja2 import Template
from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, IgniteThinClientConfiguration, \
    DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.services.utils.ssl.client_connector_configuration import ClientConnectorConfiguration
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion


class ThinClient4857Test(IgniteTest):
    """
    cluster - cluster size.
    JAVA_CLIENT_CLASS_NAME - running classname.
    to use with ssl enabled:
    export GLOBALS='{"ssl":{"enabled":true}}' .
    """

    JAVA_CLIENT_CLASS_NAME = "org.apache.ignite.internal.ducktest.tests.thin_client_test.ThinClientPutWhileNotTerminated"

    CACHE_NAME = "TEST_CACHE"

    CONFIG_TEMPLATE = """
            <property name="cacheConfiguration">
            <bean class="org.apache.ignite.configuration.CacheConfiguration">
                <property name="name" value="{{ cache_name }}"/>
                <property name="expiryPolicyFactory">
                    <bean class="org.apache.ignite.internal.processors.platform.cache.expiry.PlatformExpiryPolicyFactory">
                        <constructor-arg value="#{30*1000}"/>
                        <constructor-arg value="#{30*1000}"/>
                        <constructor-arg value="#{30*1000}"/>
                    </bean>
                </property>
            </bean>
        </property>
        """  # noqa: E501

    @cluster(num_nodes=4)
    @ignite_versions(str(DEV_BRANCH))
    def test_thin_client_compatibility(self, ignite_version):
        """
        Thin SBTSUPPORT-4857 test.
        """

        server_config = IgniteConfiguration(version=IgniteVersion(ignite_version),
                                            client_connector_configuration=ClientConnectorConfiguration(),
                                            data_storage=DataStorageConfiguration(
                                                default=DataRegionConfiguration(persistent=True)),
                                            properties=Template(ThinClient4857Test.CONFIG_TEMPLATE)
                                            .render(cache_name=ThinClient4857Test.CACHE_NAME)
                                            )

        ignite = IgniteService(self.test_context, server_config, 2)

        addresses = ignite.nodes[0].account.hostname + ":" + str(server_config.client_connector_configuration.port)

        thin_clients = IgniteApplicationService(self.test_context,
                                                IgniteThinClientConfiguration(addresses=addresses,
                                                                              version=IgniteVersion(ignite_version)),
                                                java_class_name=self.JAVA_CLIENT_CLASS_NAME,
                                                num_nodes=2,
                                                params={"cacheName": ThinClient4857Test.CACHE_NAME})

        ignite.start()

        control_utility = ControlUtility(ignite)
        control_utility.activate()
        self.test_context.logger.warn(">>> activate")

        thin_clients.start()

        time.sleep(30)

        control_utility.deactivate()
        self.test_context.logger.warn(">>> deactivate")

        time.sleep(60)

        control_utility.activate()
        self.test_context.logger.warn(">>> activate")

        thin_clients.stop()
        ignite.stop()

