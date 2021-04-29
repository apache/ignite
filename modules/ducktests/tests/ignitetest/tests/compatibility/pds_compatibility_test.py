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
This module contains test that checks that PDS "from_version" compatible with "to_version"
"""
from ducktape.mark import parametrize

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.utils import cluster
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, LATEST, IgniteVersion


# pylint: disable=W0223
# pylint: disable=no-member
class PdsCompatibilityTest(IgniteTest):
    """
    A simple test to check PDS compatibility of different Ignite versions.

    Start Ignite cluster version "from_version" with PDS enabled.
    Start a client application that puts prepared data.
    Stop cluster and client.
    Start Ignite cluster version "to_version" without PDS clearing.
    Start client that reads data and checks that it can be read and have not changed.

    """
    APP_CLASS = "org.apache.ignite.internal.ducktest.tests.compatibility.PdsCompatiblityApplication"
    LOAD_OPERATION = "load"
    CHECK_OPERATION = "check"

    @cluster(num_nodes=2)
    @parametrize(version_from=str(LATEST), version_to=str(DEV_BRANCH))
    def test_pds_compatibility(self, version_from, version_to):
        """
        Saves data using one version of ignite and then load with another.
        """

        server_configuration_from = IgniteConfiguration(version=IgniteVersion(version_from),
                                                        data_storage=DataStorageConfiguration(
                                                            default=DataRegionConfiguration(persistent=True)))
        ignite_from = IgniteService(self.test_context, server_configuration_from, num_nodes=1)

        ignite_from.start()

        ControlUtility(ignite_from).activate()

        loader = IgniteApplicationService(
            self.test_context,
            config=ignite_from.config._replace(client_mode=True, discovery_spi=from_ignite_cluster(ignite_from)),
            java_class_name=self.APP_CLASS,
            params={"operation": self.LOAD_OPERATION})

        app_nodes = loader.nodes.copy()
        loader.run()
        loader.free()

        ignite_from.stop()
        nodes = ignite_from.nodes.copy()
        ignite_from.free()

        ignite_to = IgniteService(
            self.test_context,
            config=server_configuration_from._replace(version=IgniteVersion(version_to)),
            num_nodes=1)

        ignite_to.nodes = nodes

        ignite_to.start(clean=False)

        ControlUtility(ignite_to).activate()

        checker = IgniteApplicationService(
            self.test_context,
            config=ignite_to.config._replace(client_mode=True, discovery_spi=from_ignite_cluster(ignite_to)),
            java_class_name=self.APP_CLASS,
            params={"operation": self.CHECK_OPERATION})

        checker.nodes = app_nodes
        checker.start(clean=False)
        checker.await_stopped()
