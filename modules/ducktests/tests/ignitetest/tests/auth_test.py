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
from ignitetest.services.utils.control_utility import ControlUtility, ControlUtilityError
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.utils import ignite_versions, cluster
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, LATEST, IgniteVersion
from ignitetest.services.utils.auth import DEFAULT_AUTH_PASSWORD, DEFAULT_AUTH_USERNAME
from ignitetest.utils.enum import constructible

WRONG_PASSWORD = "wrong_password"


@constructible
class PasswordType(IntEnum):
    """
    Password type.
    """
    CORRECT = 0
    WRONG = 1


# pylint: disable=W0223
class AuthenticationTests(IgniteTest):
    """
    Tests Ignite Control Utility Activation command with enabled Authentication
    """
    NUM_NODES = 3

    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @matrix(password_type=[PasswordType.CORRECT, PasswordType.WRONG])
    def test_activate_with_authentication(self, ignite_version, password_type):
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

        control_utility = ControlUtility(cluster=servers,
                                         username=DEFAULT_AUTH_USERNAME,
                                         password=WRONG_PASSWORD if password_type is PasswordType.WRONG
                                         else DEFAULT_AUTH_PASSWORD
                                         )

        if password_type is PasswordType.WRONG:
            try:
                control_utility.activate()
                raise Exception("User successfully execute command with wrong password")
            except ControlUtilityError:
                pass
        else:
            control_utility.activate()
