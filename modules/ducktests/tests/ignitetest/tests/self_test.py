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
This module contains smoke tests that checks that ducktape works as expected
"""
import os

from ducktape.mark import matrix

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.ignite_execution_exception import IgniteExecutionException
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import DataStorageConfiguration, TransactionConfiguration, \
    BinaryConfiguration, \
    TcpCommunicationSpi
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, IgniteClientConfiguration
from ignitetest.services.utils.ignite_configuration.cache import CacheConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.services.utils.ssl.client_connector_configuration import ClientConnectorConfiguration, \
    ThinClientConfiguration
from ignitetest.services.utils.ssl.connector_configuration import ConnectorConfiguration
from ignitetest.utils import ignite_versions, cluster
from ignitetest.utils.bean import Bean
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion


class SelfTest(IgniteTest):
    """
    Self test
    """

    @cluster(num_nodes=1)
    @ignite_versions(str(DEV_BRANCH))
    def test_assertion_convertion(self, ignite_version):
        """
        Test to make sure Java assertions are converted to python exceptions
        """
        server_configuration = IgniteConfiguration(version=IgniteVersion(ignite_version))

        app = IgniteApplicationService(
            self.test_context,
            server_configuration,
            java_class_name="org.apache.ignite.internal.ducktest.tests.smoke_test.AssertionApplication")

        try:
            app.start()
        except IgniteExecutionException as ex:
            assert str(ex) == "Java application execution failed. java.lang.AssertionError"
        else:
            app.stop()
            assert False

    @cluster(num_nodes=4)
    @ignite_versions(str(DEV_BRANCH))
    def test_simple_services_start_stop(self, ignite_version):
        """
        Tests plain services start and stop (termitation vs self-terination).
        """
        ignites = IgniteService(self.test_context, IgniteConfiguration(version=IgniteVersion(ignite_version)),
                                num_nodes=1)

        ignites.start()

        client = IgniteService(self.test_context, IgniteClientConfiguration(version=IgniteVersion(ignite_version)),
                               num_nodes=1)

        client.start()

        node1 = IgniteApplicationService(
            self.test_context,
            IgniteClientConfiguration(version=IgniteVersion(ignite_version),
                                      discovery_spi=from_ignite_cluster(ignites)),
            java_class_name="org.apache.ignite.internal.ducktest.tests.self_test.TestKillableApplication",
            startup_timeout_sec=180)

        node2 = IgniteApplicationService(
            self.test_context,
            IgniteClientConfiguration(version=IgniteVersion(ignite_version),
                                      discovery_spi=from_ignite_cluster(ignites)),
            java_class_name="org.apache.ignite.internal.ducktest.tests.self_test.TestSelfKillableApplication",
            startup_timeout_sec=180)

        node1.start()

        node2.run()

        node1.stop()

        client.stop()

        ignites.stop()

    @cluster(num_nodes=1)
    @ignite_versions(str(DEV_BRANCH))
    def test_logs_rotation(self, ignite_version):
        """
        Test logs rotation after ignite service restart.
        """
        def get_log_lines_count(service, filename):
            node = service.nodes[0]
            log_file = os.path.join(service.log_dir, filename)
            log_cnt = list(node.account.ssh_capture(f'cat {log_file} | wc -l', callback=int))[0]
            return log_cnt

        def get_logs_count(service):
            node = service.nodes[0]
            return list(node.account.ssh_capture(f'ls {service.log_dir}/ignite.log* | wc -l', callback=int))[0]

        ignites = IgniteService(self.test_context, IgniteConfiguration(version=IgniteVersion(ignite_version)),
                                num_nodes=1)

        ignites.start()

        num_restarts = 6
        for i in range(num_restarts - 1):
            ignites.stop()

            old_cnt = get_log_lines_count(ignites, "ignite.log")
            assert old_cnt > 0

            ignites.start(clean=False)

            new_cnt = get_log_lines_count(ignites, "ignite.log")
            assert new_cnt > 0

            # check that there is no new entry in rotated file
            assert old_cnt == get_log_lines_count(ignites, f"ignite.log.{i + 1}")

        assert get_logs_count(ignites) == num_restarts

    @cluster(num_nodes=1)
    @ignite_versions(str(DEV_BRANCH))
    @matrix(is_ignite_service=[True, False])
    def test_config_add_to_result(self, ignite_version, is_ignite_service):
        """
        Test that the config file is in config directory
        and Service.logs contains the config directory to add to the result.
        """
        ignite_cfg = IgniteConfiguration(version=IgniteVersion(ignite_version))

        if is_ignite_service:
            ignite = IgniteService(self.test_context, ignite_cfg, num_nodes=1)
        else:
            ignite = IgniteApplicationService(
                self.test_context, ignite_cfg,
                java_class_name="org.apache.ignite.internal.ducktest.tests.self_test.TestKillableApplication")

        ignite.start()

        assert ignite.logs.get('config').get('path') == ignite.config_dir

        assert ignite.config_file.startswith(ignite.config_dir)

        ignite.nodes[0].account.ssh(f'ls {ignite.config_dir} | grep {os.path.basename(ignite.config_file)}')
        ignite.nodes[0].account.ssh(f'ls {ignite.config_dir} | grep {os.path.basename(ignite.log_config_file)}')

        ignite.stop()

    @cluster(num_nodes=1)
    @ignite_versions(str(DEV_BRANCH))
    def test_server_config_options(self, ignite_version):
        """
        Test to make sure non-default non-trivial ignite node configuration XML file is generated correctly.
        """
        ignite = IgniteService(self.test_context, get_server_config(ignite_version), 1, jvm_opts="-DCELL=1")
        ignite.start()

        control_utility = ControlUtility(ignite)
        control_utility.activate()

        ignite.stop()


def get_server_config(ignite_version):
    affinity = Bean("org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction",
                    partitions=16384,
                    affinityBackupFilter=Bean(
                        "org.apache.ignite.cache.affinity.rendezvous.ClusterNodeAttributeColocatedBackupFilter",
                        constructor_args=["CELL"]))

    cache_templates = [
        CacheConfiguration(name="PARTITIONED*", cache_mode="PARTITIONED", atomicity_mode="TRANSACTIONAL",
                           statistics_enabled=True, affinity=affinity),
        CacheConfiguration(name="AffinityTemplate*", cache_mode="PARTITIONED",
                           atomicity_mode="TRANSACTIONAL", statistics_enabled=True, affinity=affinity,
                           affinity_mapper=Bean(
                               "org.apache.ignite.internal.ducktest.tests.self_test.TestAffinityMapper")),
    ]
    return IgniteConfiguration(version=IgniteVersion(ignite_version),
                               data_storage=DataStorageConfiguration(
                                   checkpoint_frequency=10000,
                                   wal_history_size=2147483647,
                                   wal_segment_size=1024 * 1024 * 1024,
                                   wal_mode="LOG_ONLY",
                                   metrics_enabled=True,
                                   metrics_rate_time_interval=60000,
                                   wal_buffer_size=5242880,
                                   wal_compaction_enabled=True,
                                   default=DataRegionConfiguration(
                                       persistence_enabled=True,
                                       max_size=1024 * 1024 * 1024,
                                       metrics_enabled=True,
                                       metrics_rate_time_interval=1000
                                   )),
                               client_connector_configuration=ClientConnectorConfiguration(
                                   thread_pool_size=10,
                                   thin_client_configuration=ThinClientConfiguration(
                                       max_active_compute_tasks_per_connection=100)),
                               transaction_configuration=TransactionConfiguration(
                                   default_tx_timeout=300000,
                                   default_tx_isolation="READ_COMMITTED",
                                   tx_timeout_on_partition_map_exchange=120000),
                               sql_schemas=["schema1", "schema2"],
                               caches=cache_templates,
                               metrics_log_frequency=30000,
                               failure_detection_timeout=120000,
                               rebalance_thread_pool_size=8,
                               peer_class_loading_enabled=True,
                               auto_activation_enabled=False,
                               binary_configuration=BinaryConfiguration(compact_footer=True),
                               communication_spi=TcpCommunicationSpi(
                                   idle_connection_timeout=600000,
                                   socket_write_timeout=30000,
                                   selectors_count=18,
                                   connections_per_node=4,
                                   use_paired_connections=True,
                                   message_queue_limit=0),
                               connector_configuration=ConnectorConfiguration(idle_timeout=180000)
                               )
