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

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.kafka.kafka import KafkaSettings, KafkaService
from ignitetest.services.utils import IgniteServiceType
from ignitetest.services.utils.cdc.cdc_configurer import CdcParams
from ignitetest.services.utils.cdc.ignite_to_ignite_cdc_configurer import IgniteToIgniteCdcConfigurer
from ignitetest.services.utils.cdc.ignite_to_kafka_cdc_configurer import KafkaCdcParams, \
    IgniteToKafkaCdcConfigurer
from ignitetest.services.utils.cdc.ignite_to_ignite_client_cdc_configurer import IgniteToIgniteClientCdcConfigurer
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.cache import CacheConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster, TcpDiscoverySpi, TcpDiscoveryVmIpFinder
from ignitetest.services.utils.ssl.client_connector_configuration import ClientConnectorConfiguration
from ignitetest.services.zk.zookeeper import ZookeeperSettings, ZookeeperService
from ignitetest.tests.cdc.cdc_ext_base_test import CdcExtBaseTest
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion


CACHE_NAME = "cdc-test-cache"
JAVA_CLIENT_CLASS_NAME = "org.apache.ignite.internal.ducktest.tests.cdc.CdcContinuousUpdatesApplication"

WAL_FORCE_ARCHIVE_TIMEOUT_MS = 100
TEST_DURATION_SEC = 10
RANGE = 5000


class CdcExtTest(CdcExtBaseTest):
    """
    CDC extensions tests.
    """
    @cluster(num_nodes=6)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @defaults(pds=[True, False], mode=["active-active", "active-passive"])
    def cdc_ignite_to_ignite_test(self, ignite_version, pds, mode):
        return self.run(ignite_version, pds, mode,
                        IgniteToIgniteCdcConfigurer(),
                        CdcParams(caches=[CACHE_NAME]))

    @cluster(num_nodes=6)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @defaults(pds=[True, False], mode=["active-active", "active-passive"])
    def cdc_ignite_to_ignite_client_test(self, ignite_version, pds, mode):
        return self.run(ignite_version, pds, mode,
                        IgniteToIgniteClientCdcConfigurer(),
                        CdcParams(caches=[CACHE_NAME]))

    @cluster(num_nodes=12)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @defaults(pds=[True, False], mode=["active-active", "active-passive"])
    def cdc_ignite_to_kafka_to_ignite_test(self, ignite_version, pds, mode):
        zk, kafka = start_kafka(self.test_context, 1)

        res = self.run(ignite_version, pds, mode, IgniteToKafkaCdcConfigurer(),
                       KafkaCdcParams(caches=[CACHE_NAME], kafka=kafka))

        stop_kafka(zk, kafka)

        return res

    @cluster(num_nodes=12)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @defaults(pds=[True, False], mode=["active-active", "active-passive"])
    def cdc_ignite_to_kafka_to_ignite_client_test(self, ignite_version, pds, mode):
        zk, kafka = start_kafka(self.test_context, 1)

        res = self.run(ignite_version, pds, mode, IgniteToKafkaCdcConfigurer(),
                       KafkaCdcParams(
                           caches=[CACHE_NAME],
                           kafka=kafka,
                           kafka_to_ignite_client_type=IgniteServiceType.THIN_CLIENT
                       ))

        stop_kafka(zk, kafka)

        return res

    def run(self, ignite_version, pds, mode, cdc_configurer, cdc_params):
        src_cluster = IgniteService(self.test_context, ignite_config(ignite_version, pds, "src"),
                                    2, modules=["cdc-ext"])

        dst_cluster = IgniteService(self.test_context, ignite_config(ignite_version, pds, "dst"),
                                    2, modules=["cdc-ext"])

        if mode == "active-active":
            cdc_streamer_metrics = self.run_active_active(src_cluster, dst_cluster, cdc_configurer, cdc_params)
        else:
            cdc_streamer_metrics = self.run_active_passive(src_cluster, dst_cluster, cdc_configurer, cdc_params)

        partitions_are_same = self.check_partitions_are_same(src_cluster, dst_cluster)

        self.logger.info(f"Results:\n{json.dumps(cdc_streamer_metrics, indent=4)}")

        if not partitions_are_same:
            raise AssertionError("Partitions are different in source and target clusters")

        src_cluster.stop()

        dst_cluster.stop()

        return cdc_streamer_metrics

    def run_active_passive(self, src_cluster, dst_cluster, cdc_configurer, cdc_params):
        src_ctx = self.start_active_passive(src_cluster, dst_cluster, cdc_configurer, cdc_params)

        client = IgniteApplicationService(
            self.test_context,
            client_cluster_config(src_cluster),
            java_class_name=JAVA_CLIENT_CLASS_NAME,
            num_nodes=1,
            params={"cacheName": CACHE_NAME, "pacing": 5, "clusterCnt": 1, "clusterIdx": 0, "range": RANGE})

        client.start()

        sleep(TEST_DURATION_SEC)

        client.stop()

        assert int(client.extract_result("putCnt")) > 0
        assert int(client.extract_result("removeCnt")) > 0

        cdc_configurer.wait_cdc(src_ctx, no_new_events_period_secs=10, timeout_sec=300)

        cdc_streamer_metrics = cdc_configurer.stop_ignite_cdc(src_ctx, timeout_sec=300)

        return cdc_streamer_metrics

    def run_active_active(self, src_cluster, dst_cluster, cdc_configurer, cdc_params):
        src_ctx, dst_ctx = self.start_active_active(src_cluster, dst_cluster, cdc_configurer, cdc_params)

        client1 = IgniteApplicationService(
            self.test_context,
            client_cluster_config(src_cluster),
            java_class_name=JAVA_CLIENT_CLASS_NAME,
            num_nodes=1,
            params={"cacheName": CACHE_NAME, "pacing": 1, "clusterCnt": 2, "clusterIdx": 0, "range": RANGE})

        client2 = IgniteApplicationService(
            self.test_context,
            client_cluster_config(dst_cluster),
            java_class_name=JAVA_CLIENT_CLASS_NAME,
            num_nodes=1,
            params={"cacheName": CACHE_NAME, "pacing": 1, "clusterCnt": 2, "clusterIdx": 1, "range": RANGE})

        client1.start()
        client2.start()

        sleep(TEST_DURATION_SEC)

        client1.stop()
        client2.stop()

        assert int(client1.extract_result("putCnt")) > 0
        assert int(client2.extract_result("putCnt")) > 0

        cdc_configurer.wait_cdc(src_ctx, no_new_events_period_secs=10, timeout_sec=300)
        cdc_configurer.wait_cdc(dst_ctx, no_new_events_period_secs=10, timeout_sec=300)

        cdc_streamer_metrics = {
            "src": cdc_configurer.stop_ignite_cdc(src_ctx, timeout_sec=300),
            "dst": cdc_configurer.stop_ignite_cdc(dst_ctx, timeout_sec=300)
        }

        return cdc_streamer_metrics


def client_cluster_config(ignite):
    return ignite.config._replace(
        client_mode=True,
        data_storage=None,
        plugins=[],
        ext_beans=[],
        discovery_spi=from_ignite_cluster(ignite)
    )


def ignite_config(ignite_version, pds, ignite_instance_name):
    config = IgniteConfiguration(
        discovery_spi=TcpDiscoverySpi(ip_finder=TcpDiscoveryVmIpFinder()),
        ignite_instance_name=ignite_instance_name,
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

    return config


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
