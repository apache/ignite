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
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.auth import DEFAULT_AUTH_PASSWORD, DEFAULT_AUTH_USERNAME
from ignitetest.services.utils.control_utility import ControlUtility, ControlUtilityError
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.utils import ignite_versions, cluster
from ignitetest.services.utils.ignite_configuration import IgniteThinClientConfiguration
from ignitetest.services.utils.ssl.client_connector_configuration import ClientConnectorConfiguration
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, LATEST, IgniteVersion

WRONG_PASSWORD = "wrong_password"
TEST_USERNAME = "admin"
TEST_PASSWORD = "qwe123"
TEST_PASSWORD2 = "123qwe"

ADD_USER = 'adduser'
UPDATE_USER = 'updateuser'
REMOVE_USER = 'removeuser'


class AuthenticationTests(IgniteTest):
    """
    Tests Ignite Authentication
    https://ignite.apache.org/docs/latest/security/authentication
    """
    NUM_NODES = 2

    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    def test_change_users(self, ignite_version):
        """
        Test add, update and remove user
        """
        config = IgniteConfiguration(
            cluster_state="INACTIVE",
            auth_enabled=True,
            version=IgniteVersion(ignite_version),
            data_storage=DataStorageConfiguration(
                default=DataRegionConfiguration(persistence_enabled=True)),
            client_connector_configuration=ClientConnectorConfiguration()
        )

        servers = IgniteService(self.test_context, config=config, num_nodes=self.NUM_NODES - 1)

        servers.start()

        ControlUtility(cluster=servers, username=DEFAULT_AUTH_USERNAME, password=DEFAULT_AUTH_PASSWORD).activate()

        client_cfg = IgniteThinClientConfiguration(
            addresses=servers.nodes[0].account.hostname + ":" + str(config.client_connector_configuration.port),
            version=IgniteVersion(ignite_version),
            username=DEFAULT_AUTH_USERNAME,
            password=DEFAULT_AUTH_PASSWORD)

        # Add new user
        check_authenticate(servers, TEST_USERNAME, TEST_PASSWORD, True)
        self.run_with_creds(client_cfg, ADD_USER, TEST_USERNAME, TEST_PASSWORD)
        check_authenticate(servers, TEST_USERNAME, TEST_PASSWORD)

        # Update user password
        check_authenticate(servers, TEST_USERNAME, TEST_PASSWORD2, True)
        self.run_with_creds(client_cfg, UPDATE_USER, TEST_USERNAME, TEST_PASSWORD2)
        check_authenticate(servers, TEST_USERNAME, TEST_PASSWORD, True)
        check_authenticate(servers, TEST_USERNAME, TEST_PASSWORD2)

        # Remove user
        self.run_with_creds(client_cfg, REMOVE_USER, TEST_USERNAME, free=False)
        check_authenticate(servers, TEST_USERNAME, TEST_PASSWORD2, True)

    def run_with_creds(self, client_configuration, rest_key: str, name: str, password: str = None, clean=False,
                       free=True):
        """
        Run user modifying command
        """
        app = IgniteApplicationService(
            self.test_context,
            client_configuration,
            java_class_name="org.apache.ignite.internal.ducktest.tests.authentication.UserModifyingApplication",
            params={"rest_key": rest_key,
                    "username": name,
                    "password": password}
        )
        app.start(clean=clean)
        app.stop()
        if free:
            app.free()


def check_authenticate(servers, username: str, password: str, exception_expected: bool = False):
    """
    Check if user can run control.sh command
    """
    control_utility = ControlUtility(cluster=servers, username=username, password=password)
    if exception_expected:
        try:
            control_utility.cluster_state()
            raise Exception("Something went wrong.")
        except ControlUtilityError:
            pass
    else:
        control_utility.cluster_state()
