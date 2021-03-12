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

from enum import IntEnum

from ducktape.mark import matrix

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.auth import DEFAULT_AUTH_PASSWORD, DEFAULT_AUTH_USERNAME
from ignitetest.services.utils.control_utility import ControlUtility, ControlUtilityError
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.utils import ignite_versions, cluster
from ignitetest.utils.enum import constructible
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, LATEST, IgniteVersion


@constructible
class OperationType(IntEnum):
    """
    Load type.
    """
    ADD_USER = 0
    UPDATE_USER = 1
    REMOVE_USER = 2


WRONG_PASSWORD = "wrong_password"
TEST_USERNAME = "admin"
TEST_PASSWORD = "qwe123"


# pylint: disable=W0223
class AuthenticationTests(IgniteTest):
    """
    Tests Ignite Control Utility Activation command with enabled Authentication
    """
    NUM_NODES = 3

    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @matrix(password_is_correct=[True, False])
    def test_activate_with_authentication(self, ignite_version, password_is_correct):
        """
        Test activate cluster.
        Authentication enabled
        """

        config = IgniteConfiguration(
            cluster_state="INACTIVE",
            auth_enabled=True,
            version=IgniteVersion(ignite_version),
            data_storage=DataStorageConfiguration(
                default=DataRegionConfiguration(name='persistent', persistent=True),
            )
        )

        servers = IgniteService(self.test_context, config=config, num_nodes=self.NUM_NODES - 2)

        servers.start()

        control_utility = ControlUtility(cluster=servers,
                                         username=DEFAULT_AUTH_USERNAME,
                                         password=DEFAULT_AUTH_PASSWORD if password_is_correct else WRONG_PASSWORD)
        if password_is_correct:
            control_utility.activate()
        else:
            try:
                control_utility.activate()
                raise Exception("User successfully execute command with wrong password")
            except ControlUtilityError:
                pass

    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(DEV_BRANCH))
    @matrix(operation=[OperationType.ADD_USER, OperationType.UPDATE_USER, OperationType.REMOVE_USER])
    def test_change_users(self, ignite_version, operation):
        """
        Test add, update and remove user
        """

        config = IgniteConfiguration(
            cluster_state="INACTIVE",
            auth_enabled=True,
            version=IgniteVersion(ignite_version),
            data_storage=DataStorageConfiguration(
                default=DataRegionConfiguration(name='persistent', persistent=True),
            )
        )

        servers = IgniteService(self.test_context, config=config, num_nodes=self.NUM_NODES - 2)

        servers.start()

        ControlUtility(cluster=servers, username=DEFAULT_AUTH_USERNAME, password=DEFAULT_AUTH_PASSWORD).activate()

        client_configuration = config._replace(client_mode=True,
                                               discovery_spi=from_ignite_cluster(servers))

        # Add secound user because Default user cannot be removed
        if operation is OperationType.REMOVE_USER:
            app = IgniteApplicationService(
                self.test_context,
                client_configuration,
                java_class_name="org.apache.ignite.internal.ducktest.tests.authentication.UserModifyingApplication",
                params={"operation": operation,
                        "auth_username": DEFAULT_AUTH_USERNAME,
                        "auth_password": DEFAULT_AUTH_PASSWORD,
                        "username": TEST_USERNAME,
                        "password": TEST_PASSWORD})

            app.start()
            app.stop()

        if operation is OperationType.ADD_USER:
            appointed_user = TEST_USERNAME
            appointed_password = TEST_PASSWORD
        elif operation is OperationType.UPDATE_USER:
            appointed_user = DEFAULT_AUTH_USERNAME
            appointed_password = TEST_PASSWORD
        elif operation is OperationType.REMOVE_USER:
            appointed_user = TEST_USERNAME
            appointed_password = TEST_PASSWORD

        app = IgniteApplicationService(
            self.test_context,
            client_configuration,
            java_class_name="org.apache.ignite.internal.ducktest.tests.authentication.UserModifyingApplication",
            params={"operation": operation,
                    "auth_username": DEFAULT_AUTH_USERNAME,
                    "auth_password": DEFAULT_AUTH_PASSWORD,
                    "username": appointed_user,
                    "password": appointed_password})

        app.start()
        app.stop()

        control_utility = ControlUtility(cluster=servers,
                                         username=appointed_user,
                                         password=appointed_password)

        if operation is not OperationType.REMOVE_USER:
            control_utility.cluster_state()
        else:
            try:
                control_utility.cluster_state()
                raise Exception("User successfully execute command from removed user")
            except ControlUtilityError:
                pass

        servers.stop()
