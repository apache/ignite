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
# limitations under the License

import json
from time import sleep

from ducktape.mark import defaults

from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.cdc.cdc_configurer import CdcParams
from ignitetest.services.utils.cdc.ignite_to_ignite_cdc_configurer import CdcIgniteToIgniteConfigurer
from ignitetest.services.utils.cdc.kafka.kafka_cdc_configurer import CdcIgniteToKafkaToIgniteConfigurer, \
    CdcIgniteToKafkaToIgniteClientConfigurer
from ignitetest.services.utils.cdc.thin.ignite_to_ignite_client_cdc_configurer import CdcIgniteToIgniteClientConfigurer
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.cache import CacheConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.services.utils.ssl.client_connector_configuration import ClientConnectorConfiguration
from ignitetest.tests.cdc.cdc_ext_base_test import CdcExtBaseTest
from ignitetest.tests.client_test import check_topology
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.version import DEV_BRANCH, LATEST, IgniteVersion


CACHE_NAME = "cdc-test-cache"
JAVA_CLIENT_CLASS_NAME = "org.apache.ignite.internal.ducktest.tests.client_test.IgniteCachePutClient"

class CdcExtTest(CdcExtBaseTest):
    """
    CDC extensions tests.
    """
    @cluster(num_nodes=5)
    # @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @ignite_versions(str(DEV_BRANCH))
    @defaults(wal_force_archive_timeout=[100], duration_sec=[10], pds=[True, False])
    def cdc_ignite_to_ignite_test(self, ignite_version, wal_force_archive_timeout, duration_sec, pds):
        cdc_configurer = CdcIgniteToIgniteConfigurer()

        return self.run(ignite_version, wal_force_archive_timeout, duration_sec, pds, cdc_configurer)

    @cluster(num_nodes=5)
    # @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @ignite_versions(str(DEV_BRANCH))
    @defaults(wal_force_archive_timeout=[100], duration_sec=[10], pds=[True, False])
    def cdc_ignite_to_ignite_client_test(self, ignite_version, wal_force_archive_timeout, duration_sec, pds):
        cdc_configurer = CdcIgniteToIgniteClientConfigurer()

        return self.run(ignite_version, wal_force_archive_timeout, duration_sec, pds, cdc_configurer)

    @cluster(num_nodes=12)
    # @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @ignite_versions(str(DEV_BRANCH))
    @defaults(wal_force_archive_timeout=[100], duration_sec=[10], pds=[True, False])
    def cdc_ignite_to_kafka_to_ignite_test(self, ignite_version, wal_force_archive_timeout, duration_sec, pds):
        cdc_configurer = CdcIgniteToKafkaToIgniteConfigurer()

        return self.run(ignite_version, wal_force_archive_timeout, duration_sec, pds, cdc_configurer)

    @cluster(num_nodes=12)
    # @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @ignite_versions(str(DEV_BRANCH))
    @defaults(wal_force_archive_timeout=[100], duration_sec=[10], pds=[True, False])
    def cdc_ignite_to_kafka_to_ignite_client_test(self, ignite_version, wal_force_archive_timeout, duration_sec, pds):
        cdc_configurer = CdcIgniteToKafkaToIgniteClientConfigurer()

        return self.run(ignite_version, wal_force_archive_timeout, duration_sec, pds, cdc_configurer)

    def run(self, ignite_version, wal_force_archive_timeout, duration_sec, pds, cdc_configurer):
        config = IgniteConfiguration(
            version=IgniteVersion(ignite_version),
            data_storage=DataStorageConfiguration(wal_force_archive_timeout=wal_force_archive_timeout),
            caches=[CacheConfiguration(name=CACHE_NAME)],
            client_connector_configuration=ClientConnectorConfiguration()
        )

        if pds:
            config = config._replace(
                data_storage=config.data_storage._replace(
                    default=config.data_storage.default._replace(
                        persistence_enabled=True
                    )
                )
            )

        target_cluster = self.get_target_cluster(self.test_context, config, 2,
            cdc_configurer, CdcParams(cdc_caches=[CACHE_NAME]))

        source_cluster = self.get_source_cluster(self.test_context, config, 2,
            cdc_configurer, CdcParams(cdc_caches=[CACHE_NAME]), target_cluster)

        cdc_configurer.start_ignite_cdc(source_cluster)

        client_cfg = source_cluster.config._replace(
            client_mode=True, data_storage=None, plugins=[], ext_beans=[],
            discovery_spi=from_ignite_cluster(source_cluster)
        )

        client = IgniteApplicationService(self.test_context, client_cfg,
            java_class_name=JAVA_CLIENT_CLASS_NAME, num_nodes=1,
            params={"cacheName": CACHE_NAME})

        client.start()

        check_topology(ControlUtility(source_cluster), 3)

        sleep(duration_sec)
        client.stop()

        cdc_configurer.wait_cdc(no_new_events_period_secs=10, timeout_sec=300)

        cdc_streamer_metrics = cdc_configurer.stop_ignite_cdc(source_cluster, timeout_sec=300)

        partitions_are_same = self.check_partitions_are_same(source_cluster, target_cluster)

        self.logger.info(f"Results:\n{json.dumps(cdc_streamer_metrics, indent=4)}")

        if not partitions_are_same:
            raise AssertionError("Partitions are different in source and target clusters")

        source_cluster.stop()

        target_cluster.stop()

        return cdc_streamer_metrics
