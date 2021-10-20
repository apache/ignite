
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
This module contains transactions manipulation test through control utility.
"""

import random

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.services.utils.ignite_configuration.cache import CacheConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.utils import ignite_versions, cluster
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, LATEST, IgniteVersion


class TransactionsTests(IgniteTest):
    """
    Tests control.sh transaction management command.
    """
    NUM_NODES = 4
    CACHE_NAME = "TEST"

    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    def test_tx_info(self, ignite_version):
        """
        Tests verbose tx info for specific xid.
        """
        servers = self.__start_ignite_nodes(ignite_version, self.NUM_NODES - 2)

        long_tx = self.__start_tx_app(ignite_version, servers, cache_name=self.CACHE_NAME, tx_count=2, tx_size=2,
                                      key_prefix='TX_1_KEY_')

        wait_for_key_locked(long_tx)

        control_utility = ControlUtility(servers)

        transactions = control_utility.tx()

        pick_tx = random.choice(transactions)

        res = control_utility.tx_info(pick_tx.xid)

        assert res.xid == pick_tx.xid
        assert res.timeout == pick_tx.timeout
        assert res.top_ver == pick_tx.top_ver
        assert res.label == pick_tx.label

    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    def test_kill_tx(self, ignite_version):
        """
        Test kill transactions by xid and filter.
        """
        servers = self.__start_ignite_nodes(ignite_version, self.NUM_NODES - 2)

        tx_count = 3

        long_tx_1 = self.__start_tx_app(ignite_version, servers, cache_name=self.CACHE_NAME, tx_count=tx_count,
                                        tx_size=2, key_prefix='TX_1_KEY_', label='TX_1', wait_for_topology_version=4)

        long_tx_2 = self.__start_tx_app(ignite_version, servers, cache_name=self.CACHE_NAME, tx_count=tx_count,
                                        tx_size=2, key_prefix='TX_2_KEY_', label='TX_2', wait_for_topology_version=4)

        wait_for_key_locked(long_tx_1, long_tx_2)

        control_utility = ControlUtility(servers)

        # check kill with specific xid.
        transactions = control_utility.tx(label_pattern='TX_1')
        res = control_utility.tx_kill(xid=random.choice(transactions).xid)
        assert res and len(res) == 1 and res[0].xid == long_tx_1.extract_result("TX_ID")

        # check kill with filter.
        res = control_utility.tx_kill(label_pattern='TX_2')
        assert res and len(res) == tx_count and set(map(lambda x: x.xid, res))\
            .issubset(set(long_tx_2.extract_results("TX_ID")))

    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    def test_tx_filter(self, ignite_version):
        """
        Test filtering transactions list.
        """
        servers = self.__start_ignite_nodes(ignite_version, self.NUM_NODES - 2)

        client_tx_count, client_tx_size = 5, 4
        server_tx_count, server_tx_size = 3, 2

        servers = self.__start_tx_app(ignite_version, servers, client_mode=False, cache_name=self.CACHE_NAME,
                                      tx_count=server_tx_count, tx_size=server_tx_size, key_prefix='TX_1_KEY_',
                                      label='LBL_SERVER', wait_for_topology_version=4)

        clients = self.__start_tx_app(ignite_version, servers, cache_name=self.CACHE_NAME, tx_count=client_tx_count,
                                      tx_size=client_tx_size, key_prefix='TX_2_KEY_', label='LBL_CLIENT',
                                      wait_for_topology_version=4)

        wait_for_key_locked(clients, servers)
        control_utility = ControlUtility(servers)

        start_check = self.monotonic()
        assert len(control_utility.tx(clients=True, label_pattern='LBL_.*')) == client_tx_count
        assert len(control_utility.tx(servers=True, label_pattern='LBL_.*')) == server_tx_count

        # limit to 2 transactions on each node, therefore 4 total.
        assert len(control_utility.tx(limit=2, label_pattern='LBL_.*')) == 4

        assert len(control_utility.tx(label_pattern='LBL_.*')) == client_tx_count + server_tx_count

        # filter transactions with keys size greater or equal to min_size.
        assert len(control_utility.tx(min_size=client_tx_size, label_pattern='LBL_.*')) == client_tx_count

        server_nodes = [node.consistent_id for node in servers.nodes]
        assert len(control_utility.tx(label_pattern='LBL_.*', nodes=server_nodes)) == server_tx_count

        # test ordering.
        for order_type in ['DURATION', 'SIZE', 'START_TIME']:
            transactions = control_utility.tx(label_pattern='LBL_.*', order=order_type)
            assert is_sorted(transactions, key=lambda x, attr=order_type: getattr(x, attr.lower()), reverse=True)

        # test min_duration filtering.
        min_duration = int(self.monotonic() - start_check)
        transactions = control_utility.tx(min_duration=min_duration, label_pattern='LBL_.*')
        assert len(transactions) == server_tx_count + client_tx_count
        for tx in transactions:
            assert tx.duration >= min_duration

    def __start_tx_app(self, version, servers, *, client_mode=True, **kwargs):
        app_params = {
            'config': IgniteConfiguration(version=IgniteVersion(version),
                                          client_mode=client_mode,
                                          discovery_spi=from_ignite_cluster(servers)),
            'java_class_name': 'org.apache.ignite.internal.ducktest.tests.control_utility'
                               '.LongRunningTransactionsGenerator',
            'params': kwargs
        }

        app = IgniteApplicationService(self.test_context, **app_params)
        app.start()

        return app

    def __start_ignite_nodes(self, version, num_nodes, timeout_sec=60):
        config = IgniteConfiguration(
            cluster_state="ACTIVE",
            version=IgniteVersion(version),
            caches=[CacheConfiguration(name=self.CACHE_NAME, atomicity_mode='TRANSACTIONAL')]
        )

        servers = IgniteService(self.test_context, config=config, num_nodes=num_nodes, startup_timeout_sec=timeout_sec)

        servers.start()

        return servers


def wait_for_key_locked(*clusters):
    """
    Wait for APPLICATION_KEYS_LOCKED on tx_app nodes.
    """
    for cluster_ in clusters:
        cluster_.await_event("APPLICATION_KEYS_LOCKED", timeout_sec=60, from_the_beginning=True)


def is_sorted(lst, key=lambda x: x, reverse=False):
    """
    Check if list is sorted.
    """
    for i, elem in enumerate(lst[1:]):
        return key(elem) <= key(lst[i]) if not reverse else key(elem) >= key(lst[i])

    return True
