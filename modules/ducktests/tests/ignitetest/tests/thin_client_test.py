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
This module contains client tests
"""
import time

from ducktape.mark import parametrize

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.utils import ignite_versions, cluster
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, LATEST, IgniteVersion

class ThinClientTest(IgniteTest):
    """
    cluster - cluster size
    JAVA_CLIENT_CLASS_NAME - running classname.
    """

    JAVA_CLIENT_CLASS_NAME = "org.apache.ignite.internal.ducktest.tests.thin_client_test.ThinClientSelfTest"

    """
    Tests services implementations
    """

    @cluster(num_nodes=2)
    @ignite_versions(str(DEV_BRANCH))
    def test_thin_client_selftest(self, ignite_version):
        """
        Thin client self test
        """

        server_configuration = IgniteConfiguration(version=IgniteVersion(ignite_version), caches=[])

        ignite = IgniteService(self.test_context, server_configuration, 1, startup_timeout_sec=180)

        thin_client_connection = ignite.nodes[0].account.hostname + ":10800"

        static_clients = IgniteApplicationService(self.test_context, server_configuration,
                                           java_class_name=self.JAVA_CLIENT_CLASS_NAME,
                                           num_nodes=1,
                                           params={"thin_client_connection": thin_client_connection},
                                           start_ignite = False)

        ignite.start()
        static_clients.start()

        static_clients.stop()
        ignite.stop()
