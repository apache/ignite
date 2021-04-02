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
    CACHE_NAME - name of the cache to create for the test.
    JAVA_CLIENT_CLASS_NAME - running classname.
    static_clients - the number of permanently employed clients.
    """

    JAVA_CLIENT_CLASS_NAME = "org.apache.ignite.internal.ducktest.tests.thin_client_test.ThinClientSelfTest"

    """
    Tests services implementations
    """

    @cluster(num_nodes=2)
    @ignite_versions(str(DEV_BRANCH))
    @parametrize(num_nodes=2, clients_num=1)
    def test_replicated_atomic_cache(self, ignite_version, num_nodes, clients_num):
        """
        Test that thin client can connect, create, configure  and use cache
        """
        servers_count = num_nodes - clients_num

        server_configuration = IgniteConfiguration(version=IgniteVersion(ignite_version), caches=[])

        ignite = IgniteService(self.test_context, server_configuration, servers_count, startup_timeout_sec=180)

        thin_client_connection = ignite.nodes[0].account.hostname + ":10800"

        static_clients = IgniteApplicationService(self.test_context, server_configuration,
                                           java_class_name=self.JAVA_CLIENT_CLASS_NAME,
                                           num_nodes=clients_num,
                                           params={"thin_client_connection": thin_client_connection},
                                           start_ignite = False,
                                           startup_timeout_sec=180)

        ignite.start()
        static_clients.start()

        static_clients.stop()
        ignite.stop()
