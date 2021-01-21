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
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.services.utils.ignite_configuration.cache import CacheConfiguration
from ignitetest.utils import ignite_versions, cluster
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, LATEST, IgniteVersion

from ignitetest.services.thin_client_app import ThinClientService

class ThinClientTest(IgniteTest):
    """
    cluster - cluster size
    CACHE_NAME - name of the cache to create for the test.
    JAVA_CLIENT_CLASS_NAME - running classname.
    static_clients - the number of permanently employed clients.
    """

    CACHE_NAME = "think_client_test_cache"
    ENTRY_NUM = 10
    JAVA_CLIENT_CLASS_NAME = "org.apache.ignite.internal.ducktest.tests.thin_client_test.ThinClientCachePut"

    """
    Tests services implementations
    """

    @cluster(num_nodes=3)
    @ignite_versions(str(DEV_BRANCH))
    @parametrize(num_nodes=3, static_clients_num=1)
    def test_replicated_atomic_cache(self, ignite_version, num_nodes, static_clients_num):
        backups = 2
        self.start_fill_cache_stop(ignite_version, num_nodes, static_clients_num,
                                   "REPLICATED", "ATOMIC", backups)

    @cluster(num_nodes=3)
    @ignite_versions(str(DEV_BRANCH))
    @parametrize(num_nodes=3, static_clients_num=1)
    def test_replicated_transactional_cache(self, ignite_version, num_nodes, static_clients_num):
        backups = 2
        self.start_fill_cache_stop(ignite_version, num_nodes, static_clients_num,
                                   "REPLICATED", "TRANSACTIONAL", backups)

    @cluster(num_nodes=3)
    @ignite_versions(str(DEV_BRANCH))
    @parametrize(num_nodes=3, static_clients_num=1)
    def test_partitioned_atomic_cache(self, ignite_version, num_nodes, static_clients_num):
        backups = 2
        self.start_fill_cache_stop(ignite_version, num_nodes, static_clients_num,
                                   "PARTITIONED", "ATOMIC", backups)

    @cluster(num_nodes=3)
    @ignite_versions(str(DEV_BRANCH))
    @parametrize(num_nodes=3, static_clients_num=1)
    def test_partitioned_transactional_cache(self, ignite_version, num_nodes, static_clients_num):
        backups = 2
        self.start_fill_cache_stop(ignite_version, num_nodes, static_clients_num,
                                   "PARTITIONED", "TRANSACTIONAL", backups)

    def start_fill_cache_stop(self, ignite_version, num_nodes, static_clients_num, cache_mode, cache_atomicity_mode,
                              backups):
        """
        Test that thin client can connect, create, configure  and use cache
        """

        servers_count = num_nodes - static_clients_num

        server_configuration = IgniteConfiguration(version=IgniteVersion(ignite_version), caches=[])

        ignite = IgniteService(self.test_context, server_configuration, servers_count, startup_timeout_sec=180)

        server_address = ignite.nodes[0].account.hostname
        server_port = 10800

        static_clients = ThinClientService(self.test_context, server_configuration,
                                           java_class_name=self.JAVA_CLIENT_CLASS_NAME,
                                           num_nodes=static_clients_num,
                                           params={"cache_name": self.CACHE_NAME, "entry_num": self.ENTRY_NUM,
                                                   "server_address": server_address, "port": server_port,
                                                   "cache_mode": cache_mode, "cache_atomicity_mode": cache_atomicity_mode,
                                                   "backups": backups},
                                           startup_timeout_sec=180)

        ignite.start()
        static_clients.start()

        static_clients.stop()
        ignite.stop()
