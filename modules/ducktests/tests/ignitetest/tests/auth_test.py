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
This module contains password based authentication tests
"""

from ignitetest.services.ignite import IgniteService
from ignitetest.services.utils.control_utility import ControlUtility, ControlUtilityError
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.utils import ignite_versions, cluster, ignore_if
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, LATEST, IgniteVersion


# pylint: disable=W0223
class AuthenticationTests(IgniteTest):
    """
    Tests Ignite Authentication
    """
    NUM_NODES = 3

    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    def test_activate_correct_password(self, ignite_version):
        """
        Test activate cluster.
        Authentication enabled
        Positive case
        """

        config = IgniteConfiguration(
            cluster_state="INACTIVE",
            auth=True,
            version=IgniteVersion(ignite_version),
            data_storage=DataStorageConfiguration(
                default=DataRegionConfiguration(name='persistent', persistent=True),
            )
        )

        servers = IgniteService(self.test_context, config=config, num_nodes=self.NUM_NODES)

        servers.start()

        control_utility = ControlUtility(cluster=servers, login="ignite", password="ignite")
        control_utility.activate()

    @cluster(num_nodes=NUM_NODES)
    @ignore_if(lambda version, globals_dict: globals_dict.get("use_auth", False))  # Globals overrides test params
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    def test_activate_wrong_password(self, ignite_version):
        """
        Test activate cluster.
        Authentication enabled
        Negative case
        """

        config = IgniteConfiguration(
            cluster_state="INACTIVE",
            auth=True,
            version=IgniteVersion(ignite_version),
            data_storage=DataStorageConfiguration(
                default=DataRegionConfiguration(name='persistent', persistent=True),
            )
        )

        servers = IgniteService(self.test_context, config=config, num_nodes=self.NUM_NODES)

        servers.start()

        control_utility = ControlUtility(cluster=servers, login="ignite", password="wrong_password")

        try:
            control_utility.activate()
            raise Exception("User successfully execute command with wrong password")
        except ControlUtilityError:
            pass
