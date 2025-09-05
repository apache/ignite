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
from copy import deepcopy
from time import sleep

from ducktape.mark import defaults

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.cdc.cdc_configurer import CdcParams
from ignitetest.services.utils.cdc.ignite_to_ignite_cdc_configurer import CdcIgniteToIgniteConfigurer
from ignitetest.services.utils.cdc.kafka.kafka_cdc_configurer import CdcIgniteToKafkaToIgniteConfigurer, \
    CdcIgniteToKafkaToIgniteClientConfigurer, KafkaCdcParams
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

WAL_FORCE_ARCHIVE_TIMEOUT_MS = 100
TEST_DURATION_SEC = 5

class CdcExtTest(CdcExtBaseTest):
    """
    CDC extensions tests.
    """
    @cluster(num_nodes=6)
    @ignite_versions(str(DEV_BRANCH))
    @defaults(pds=[True])
    def cdc_ignite_to_ignite_test(self, ignite_version, pds):
        return self.run(ignite_version, pds,
                        CdcIgniteToIgniteConfigurer(),
                        CdcParams(caches=[CACHE_NAME]))

    @cluster(num_nodes=6)
    @ignite_versions(str(DEV_BRANCH))
    @defaults(pds=[True])
    def cdc_ignite_to_ignite_client_test(self, ignite_version, pds):
        return self.run(ignite_version, pds,
                        CdcIgniteToIgniteClientConfigurer(),
                        CdcParams(caches=[CACHE_NAME]))

    @cluster(num_nodes=12)
    @ignite_versions(str(DEV_BRANCH))
    @defaults(pds=[True])
    def cdc_ignite_to_kafka_to_ignite_test(self, ignite_version, pds):
        return self.run(ignite_version, pds,
                        CdcIgniteToKafkaToIgniteConfigurer(),
                        KafkaCdcParams(caches=[CACHE_NAME]))

    @cluster(num_nodes=12)
    @ignite_versions(str(DEV_BRANCH))
    @defaults(pds=[True])
    def cdc_ignite_to_kafka_to_ignite_client_test(self, ignite_version, pds):
        return self.run(ignite_version, pds,
                        CdcIgniteToKafkaToIgniteClientConfigurer(),
                        KafkaCdcParams(caches=[CACHE_NAME]))

    def run(self, ignite_version, pds, cdc_configurer, cdc_params):
        config = IgniteConfiguration(
            version=IgniteVersion(ignite_version),
            data_storage=DataStorageConfiguration(
                wal_force_archive_timeout=WAL_FORCE_ARCHIVE_TIMEOUT_MS
            ),
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

        source_cluster = IgniteService(self.test_context, source_cluster_config(config),2, modules=["cdc-ext"])
        target_cluster = IgniteService(self.test_context, target_cluster_config(config),2, modules=["cdc-ext"])

        cdc_configurer.configure_target_cluster(target_cluster, cdc_params)
        target_cluster.start()
        ControlUtility(target_cluster).activate()

        cdc_configurer.configure_source_cluster(source_cluster, target_cluster, cdc_params)
        source_cluster.start()
        ControlUtility(source_cluster).activate()

        cdc_configurer.start_ignite_cdc(source_cluster)

        client = IgniteApplicationService(
            self.test_context,
            client_cluster_config(source_cluster),
            java_class_name=JAVA_CLIENT_CLASS_NAME,
            num_nodes=2,
            params={"cacheName": CACHE_NAME, "pacing": 10})

        client.start()

        check_topology(ControlUtility(source_cluster), 4)

        sleep(TEST_DURATION_SEC)
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


def source_cluster_config(config):
    cfg = deepcopy(config)

    return cfg._replace(
        ignite_instance_name="source",
        data_storage=cfg.data_storage._replace(
            default=cfg.data_storage.default._replace(
                cdc_enabled=True
            )
        )
    )

def target_cluster_config(config):
    cfg = deepcopy(config)

    return cfg._replace(ignite_instance_name="target")

def client_cluster_config(ignite):
    return ignite.config._replace(
        client_mode=True,
        data_storage=None,
        plugins=[],
        ext_beans=[],
        discovery_spi=from_ignite_cluster(ignite)
    )
