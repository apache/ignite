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
import os
import time
from ducktape.mark.resource import cluster
from ducktape.tests.test import TestContext
from ducktape.utils.local_filesystem_utils import mkdir_p

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.services.utils.ignite_persistence import PersistenceAware
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
    STATIC_CLIENT_WORK_TIME_S - static client work time (s)
    ITERATION_COUNT - the number of iterations of starting and stopping client nodes (s).
    CLUSTER_NODES - cluster size.
    STATIC_CLIENTS_NUM - the number of permanently employed clients.
    TEMP_CLIENTS_NUM - number of clients who come log in and out.
    """

    CACHE_NAME = "simple-tx-cache"
    PACING = 10
    ACTION = "put-tx"
    JAVA_CLIENT_CLASS_NAME = "org.apache.ignite.internal.ducktest.tests.start_stop_client.SimpleClient"

    CLIENTS_WORK_TIME_S = 30
    STATIC_CLIENT_WORK_TIME_S = 30
    ITERATION_COUNT = 3
    CLUSTER_NODES = 8
    STATIC_CLIENTS_NUM = 2
    TEMP_CLIENTS_NUM = 4

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
        static_clients = IgniteApplicationService(
            self.test_context,
            client_cfg,
            java_class_name=self.JAVA_CLIENT_CLASS_NAME,
            num_nodes=self.STATIC_CLIENTS_NUM,
            params={"cacheName": self.CACHE_NAME,
                    "pacing": self.PACING})

        temp_clients = IgniteApplicationService(
            self.test_context,
            client_cfg,
            java_class_name=self.JAVA_CLIENT_CLASS_NAME,
            num_nodes=self.TEMP_CLIENTS_NUM,
            params={"cacheName": self.CACHE_NAME,
                    "pacing": self.PACING})

        # start servers and check cluster
        ignite.start()
        ignite.await_event(f'servers={servers_count}',
                           timeout_sec=60,
                           from_the_beginning=True,
                           backoff_sec=1)

        # start static clients
        static_clients.start()
        current_top_v += self.STATIC_CLIENTS_NUM
        check_topology(control_utility, current_top_v)

        # check client counter
        ignite.await_event(f'clients={self.STATIC_CLIENTS_NUM}',
                           timeout_sec=60,
                           from_the_beginning=True,
                           backoff_sec=1)

        # start stop temp_clients node. Check cluster.

        time.sleep(self.STATIC_CLIENT_WORK_TIME_S)

        for i in range(self.ITERATION_COUNT):
            time.sleep(self.CLIENTS_WORK_TIME_S)
            temp_clients.start()
            print("Starting iteration: " + str(i))
            temp_clients.await_event(f'clients={self.STATIC_CLIENTS_NUM + self.TEMP_CLIENTS_NUM}',
                                     timeout_sec=80,
                                     from_the_beginning=True,
                                     backoff_sec=1)

            current_top_v += self.TEMP_CLIENTS_NUM
            check_topology(control_utility, current_top_v)

            time.sleep(self.CLIENTS_WORK_TIME_S)
            temp_clients.stop()
            time.sleep(30)

            current_top_v += self.TEMP_CLIENTS_NUM
            check_topology(control_utility, current_top_v)

        static_clients.stop()
        time.sleep(15)

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
