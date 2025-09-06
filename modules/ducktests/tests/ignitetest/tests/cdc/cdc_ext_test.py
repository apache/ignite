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
from ignitetest.services.kafka.kafka import KafkaSettings, KafkaService
from ignitetest.services.utils import IgniteServiceType
from ignitetest.services.utils.cdc.cdc_configurer import CdcParams
from ignitetest.services.utils.cdc.ignite_to_ignite_cdc_configurer import IgniteToIgniteCdcConfigurer
from ignitetest.services.utils.cdc.ignite_to_kafka_cdc_configurer import KafkaCdcParams, \
    IgniteToKafkaCdcConfigurer
from ignitetest.services.utils.cdc.ignite_to_ignite_client_cdc_configurer import IgniteToIgniteClientCdcConfigurer
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.cache import CacheConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.services.utils.ssl.client_connector_configuration import ClientConnectorConfiguration
from ignitetest.services.zk.zookeeper import ZookeeperSettings, ZookeeperService
from ignitetest.tests.cdc.cdc_ext_base_test import CdcExtBaseTest
from ignitetest.tests.client_test import check_topology
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion


CACHE_NAME = "cdc-test-cache"
JAVA_CLIENT_CLASS_NAME = "org.apache.ignite.internal.ducktest.tests.client_test.IgniteCachePutClient"

WAL_FORCE_ARCHIVE_TIMEOUT_MS = 100
TEST_DURATION_SEC = 5


class CdcExtTest(CdcExtBaseTest):
    """
    CDC extensions tests.
    """
    @cluster(num_nodes=6)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @defaults(pds=[True, False])
    def cdc_ignite_to_ignite_test(self, ignite_version, pds):
        return self.run(ignite_version, pds,
                        IgniteToIgniteCdcConfigurer(),
                        CdcParams(caches=[CACHE_NAME]))

    @cluster(num_nodes=6)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @defaults(pds=[True, False])
    def cdc_ignite_to_ignite_client_test(self, ignite_version, pds):
        return self.run(ignite_version, pds,
                        IgniteToIgniteClientCdcConfigurer(),
                        CdcParams(caches=[CACHE_NAME]))

    @cluster(num_nodes=12)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @defaults(pds=[True, False])
    def cdc_ignite_to_kafka_to_ignite_test(self, ignite_version, pds):
        zk, kafka = start_kafka(self.test_context, 2)

        res = self.run(ignite_version, pds, IgniteToKafkaCdcConfigurer(),
                       KafkaCdcParams(caches=[CACHE_NAME], kafka=kafka))

        stop_kafka(zk, kafka)

        return res

    @cluster(num_nodes=12)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @defaults(pds=[True, False])
    def cdc_ignite_to_kafka_to_ignite_client_test(self, ignite_version, pds):
        zk, kafka = start_kafka(self.test_context, 2)

        res = self.run(ignite_version, pds, IgniteToKafkaCdcConfigurer(),
                       KafkaCdcParams(
                           caches=[CACHE_NAME],
                           kafka=kafka,
                           kafka_to_ignite_client_type=IgniteServiceType.THIN_CLIENT
                       ))

        stop_kafka(zk, kafka)

        return res

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

        src_cluster = IgniteService(self.test_context, src_cluster_config(config), 2, modules=["cdc-ext"])
        dst_cluster = IgniteService(self.test_context, dst_cluster_config(config), 2, modules=["cdc-ext"])

        cdc_configurer.setup_active_passive(src_cluster, dst_cluster, cdc_params)

        dst_cluster.start()
        ControlUtility(dst_cluster).activate()
        src_cluster.start()
        ControlUtility(src_cluster).activate()

        cdc_configurer.start_ignite_cdc(src_cluster)

        client = IgniteApplicationService(
            self.test_context,
            client_cluster_config(src_cluster),
            java_class_name=JAVA_CLIENT_CLASS_NAME,
            num_nodes=2,
            params={"cacheName": CACHE_NAME, "pacing": 10})

        client.start()

        check_topology(ControlUtility(src_cluster), 4)

        sleep(TEST_DURATION_SEC)
        client.stop()

        cdc_configurer.wait_cdc(no_new_events_period_secs=10, timeout_sec=300)

        cdc_streamer_metrics = cdc_configurer.stop_ignite_cdc(src_cluster, timeout_sec=300)

        partitions_are_same = self.check_partitions_are_same(src_cluster, dst_cluster)

        self.logger.info(f"Results:\n{json.dumps(cdc_streamer_metrics, indent=4)}")

        if not partitions_are_same:
            raise AssertionError("Partitions are different in source and target clusters")

        src_cluster.stop()

        dst_cluster.stop()

        return cdc_streamer_metrics


def src_cluster_config(config):
    cfg = deepcopy(config)

    return cfg._replace(
        ignite_instance_name="source",
        data_storage=cfg.data_storage._replace(
            default=cfg.data_storage.default._replace(
                cdc_enabled=True
            )
        )
    )


def dst_cluster_config(config):
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


def start_kafka(test_context, kafka_nodes, zk_nodes=1):
    zk_settings = ZookeeperSettings()
    zk = ZookeeperService(test_context, zk_nodes, settings=zk_settings)

    kafka_settings = KafkaSettings(zookeeper_connection_string=zk.connection_string())
    kafka = KafkaService(test_context, kafka_nodes, settings=kafka_settings)

    zk.start_async()
    kafka.start()

    return zk, kafka


def stop_kafka(zk, kafka):
    kafka.stop(force_stop=False, allow_fail=True)

    zk.stop(force_stop=False)
