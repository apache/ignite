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
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.services.utils.ignite_configuration.cache import CacheConfiguration
from ignitetest.utils import ignite_versions, cluster
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, LATEST, IgniteVersion


class ClientTest(IgniteTest):
    """
    cluster - cluster size
    CACHE_NAME - name of the cache to create for the test.
    PACING - the frequency of the operation on clients (ms).
    JAVA_CLIENT_CLASS_NAME - running classname.
    client_work_time - clients working time (s).
    iteration_count - the number of iterations of starting and stopping client nodes (s).
    static_clients - the number of permanently employed clients.
    temp_client - number of clients who come log in and out.
    """

    CACHE_NAME = "simple-tx-cache"
    PACING = 10
    JAVA_CLIENT_CLASS_NAME = "org.apache.ignite.internal.ducktest.tests.client_test.IgniteCachePutClient"

    @cluster(num_nodes=7)
    @ignite_versions(str(LATEST), str(DEV_BRANCH))
    @parametrize(num_nodes=7, static_clients=2, temp_client=3, iteration_count=3, client_work_time=30)
    def test_ignite_start_stop_nodes(self, ignite_version, num_nodes, static_clients, temp_client, iteration_count,
                                     client_work_time):
        """
        Start and stop clients node test without kill java process.
        Check topology.
        """
        self.ignite_start_stop(ignite_version, True, num_nodes, static_clients,
                               temp_client, iteration_count, client_work_time)

    @cluster(num_nodes=7)
    @ignite_versions(str(LATEST), str(DEV_BRANCH))
    @parametrize(num_nodes=7, static_clients=2, temp_client=3, iteration_count=3, client_work_time=30)
    def test_ignite_kill_start_nodes(self, ignite_version, num_nodes, static_clients, temp_client, iteration_count,
                                     client_work_time):
        """
        Start and kill client nodes, Check topology
        """
        self.ignite_start_stop(ignite_version, False, num_nodes, static_clients,
                               temp_client, iteration_count, client_work_time)

    def ignite_start_stop(self, ignite_version, graceful_shutdown, nodes_num, static_clients_num, temp_client,
                          iteration_count, client_work_time):
        """
        Test for starting and stopping fat clients.
        """

        servers_count = nodes_num - static_clients_num - temp_client
        current_top_v = servers_count

        # Topology version after test.
        fin_top_ver = servers_count + (2 * static_clients_num) + (2 * iteration_count * temp_client)

        server_cfg = IgniteConfiguration(version=IgniteVersion(ignite_version), caches=[
            CacheConfiguration(name=self.CACHE_NAME, backups=1, atomicity_mode='TRANSACTIONAL')])

        ignite = IgniteService(self.test_context, server_cfg, num_nodes=servers_count)

        control_utility = ControlUtility(ignite)

        client_cfg = server_cfg._replace(client_mode=True)

        static_clients = IgniteApplicationService(self.test_context, client_cfg,
                                                  java_class_name=self.JAVA_CLIENT_CLASS_NAME,
                                                  num_nodes=static_clients_num,
                                                  params={"cacheName": self.CACHE_NAME, "pacing": self.PACING})

        temp_clients = IgniteApplicationService(self.test_context, client_cfg,
                                                java_class_name=self.JAVA_CLIENT_CLASS_NAME, num_nodes=temp_client,
                                                params={"cacheName": self.CACHE_NAME, "pacing": self.PACING})

        ignite.start()

        static_clients.start()

        current_top_v += static_clients_num

        check_topology(control_utility, current_top_v)

        # Start / stop temp_clients node. Check cluster.
        for i in range(iteration_count):
            self.logger.info(f'Starting iteration: {i}.')

            temp_clients.start()

            current_top_v += temp_client

            await_event(static_clients, f'ver={current_top_v}, locNode=')

            check_topology(control_utility, current_top_v)

            await_event(temp_clients, f'clients={static_clients_num + temp_client}')

            time.sleep(client_work_time)

            if graceful_shutdown:
                temp_clients.stop()
            else:
                temp_clients.kill()

            current_top_v += temp_client

        await_event(static_clients, f'ver={current_top_v}, locNode=')

        static_clients.stop()

        check_topology(control_utility, fin_top_ver)


def await_event(service: IgniteApplicationService, message):
    """
    :param service: target service for wait
    :param message: message
    """
    service.await_event(message, timeout_sec=80, from_the_beginning=True)


def check_topology(control_utility: ControlUtility, fin_top_ver: int):
    """
    Check current topology version.
    """
    top_ver = control_utility.cluster_state().topology_version
    assert top_ver == fin_top_ver, f'Cluster current topology version={top_ver}, ' \
                                   f'expected topology version={fin_top_ver}.'
