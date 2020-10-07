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
from ducktape.mark.resource import cluster
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.ignite import IgniteService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.utils import ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, V_2_8_1, IgniteVersion


# pylint: disable=W0223

class ClientTest(IgniteTest):
    """
    CACHE_NAME - name of the cache to create for the test.
    REPORT_NAME - the name of the tests.
    PACING - the frequency of the operation on clients (ms).
    JAVA_CLIENT_CLASS_NAME - running classname.
    CLIENTS_WORK_TIME_S - clients working time (s).
    ITERATION_COUNT - the number of iterations of starting and stopping client nodes (s).
    CLUSTER_NODES - cluster size.
    STATIC_CLIENTS_NUM - the number of permanently employed clients.
    TEMP_CLIENTS_NUM - number of clients who come log in and out.
    """

    CACHE_NAME = "simple-tx-cache"
    PACING = 10
    JAVA_CLIENT_CLASS_NAME = "org.apache.ignite.internal.ducktest.tests.start_stop_client.SingleClientNode"

    CLIENTS_WORK_TIME_S = 30
    ITERATION_COUNT = 1
    CLUSTER_NODES = 12
    STATIC_CLIENTS_NUM = 2
    TEMP_CLIENTS_NUM = 7

    @cluster(num_nodes=CLUSTER_NODES)
    @ignite_versions(str(DEV_BRANCH), str(V_2_8_1))
    def test_ignite_start_stop(self, ignite_version):
        """
        test scenario
        """
        # prepare servers
        servers_count = self.CLUSTER_NODES - self.STATIC_CLIENTS_NUM - self.TEMP_CLIENTS_NUM
        # topology version after test
        current_top_v = servers_count
        fin_top_ver = servers_count + 2 * self.STATIC_CLIENTS_NUM + 2 * self.ITERATION_COUNT * self.TEMP_CLIENTS_NUM
        server_cfg = IgniteConfiguration(version=IgniteVersion(ignite_version))
        ignite = IgniteService(self.test_context, server_cfg, num_nodes=servers_count)
        control_utility = ControlUtility(ignite, self.test_context)

        # build client config
        client_cfg = server_cfg._replace(client_mode=True, discovery_spi=from_ignite_cluster(ignite))
        # prepare client services
        static_clients = multi_nodes_builder(
            self.JAVA_CLIENT_CLASS_NAME,
            self.CACHE_NAME,
            self.PACING,
            self.test_context,
            client_cfg,
            self.STATIC_CLIENTS_NUM
        )

        temp_clients = multi_nodes_builder(
            self.JAVA_CLIENT_CLASS_NAME,
            self.CACHE_NAME,
            self.PACING,
            self.test_context,
            client_cfg,
            self.TEMP_CLIENTS_NUM
        )
        # start servers and check cluster
        ignite.start()
        ignite.await_event(f'servers={servers_count}',
                           timeout_sec=60,
                           from_the_beginning=True,
                           backoff_sec=1)

        # start static clients
        start(static_clients)
        static_clients[len(static_clients) - 1].await_event(f'servers={servers_count}',
                                                            timeout_sec=60,
                                                            from_the_beginning=True,
                                                            backoff_sec=1)

        current_top_v += self.STATIC_CLIENTS_NUM
        check_topology(control_utility, current_top_v)

        # check client counter
        ignite.await_event(f'clients={self.STATIC_CLIENTS_NUM}',
                           timeout_sec=60,
                           from_the_beginning=True,
                           backoff_sec=1)

        # start stop temp_clients node. Check cluster.
        i = 0
        for i in range(self.ITERATION_COUNT):
            start(temp_clients)

            temp_clients[len(temp_clients) - 1].await_event(
                f'clients={self.STATIC_CLIENTS_NUM + self.TEMP_CLIENTS_NUM}',
                timeout_sec=80,
                from_the_beginning=True,
                backoff_sec=1)

            current_top_v += self.TEMP_CLIENTS_NUM
            check_topology(control_utility, current_top_v)

            time.sleep(self.CLIENTS_WORK_TIME_S)
            stop(temp_clients)

            current_top_v += self.TEMP_CLIENTS_NUM
            assert current_top_v == control_utility.cluster_state().topology_version

        stop(static_clients)
        check_topology(control_utility, fin_top_ver)


def check_topology(control_utility, fin_top_ver):
    """
    :param control_utility: control.sh
    :param fin_top_ver: expected topology version
    :return:
    """
    top_ver = control_utility.cluster_state().topology_version
    print("cluster topology version: " + str(top_ver))
    print("expected topology version: " + str(fin_top_ver))
    assert top_ver == fin_top_ver


# pylint: disable=R0913
def multi_nodes_builder(java_client_class_name, cache, pacing, context, client_cfg, num_nodes):
    """
    :param java_client_class_name:
    :param cache: cache name
    :param pacing: pause bentween operation
    :param context: test contex
    :param client_cfg: cfg
    :param num_nodes: node count
    :return: void
    """
    clients = []
    for i in range(num_nodes):
        clients.append(IgniteApplicationService(
            context,
            client_cfg,
            java_class_name=java_client_class_name,
            params={"cacheName": cache,
                    "pacing": pacing}))
    return clients


def start(clients):
    """
    :param clients: array with serveices
    :return: void
    run all nodes
    """
    for client in clients:
        client.start()


def stop(clients):
    """
   :param clients: array with serveices
   :return: void
   stop all nodes
   """
    for client in clients:
        client.stop()
