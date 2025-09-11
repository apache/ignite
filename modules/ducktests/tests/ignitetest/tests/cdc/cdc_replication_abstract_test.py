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

import os
import difflib
import json
from copy import copy
from time import sleep

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.kafka.kafka import KafkaSettings, KafkaService
from ignitetest.services.utils.cdc.cdc_configurer import CdcParams
from ignitetest.services.utils.cdc.ignite_to_kafka_cdc_configurer import IgniteToKafkaCdcConfigurer
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.cache import CacheConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster, TcpDiscoveryVmIpFinder, \
    TcpDiscoverySpi
from ignitetest.services.utils.ssl.client_connector_configuration import ClientConnectorConfiguration
from ignitetest.services.zk.zookeeper import ZookeeperSettings, ZookeeperService
from ignitetest.utils.bean import Bean
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import IgniteVersion


TEST_CACHE_NAME = "cdc-test-cache"
JAVA_CLIENT_CLASS_NAME = "org.apache.ignite.internal.ducktest.tests.cdc.CdcContinuousUpdatesApplication"

TEST_DURATION_SEC = 10
RANGE = 5000


class CdcReplicationAbstractTest(IgniteTest):
    def setup_active_passive(self, src_cluster, dst_cluster, cdc_configurer, cdc_params):
        enable_cdc(src_cluster)

        setup_conflict_resolver(dst_cluster, "2", cdc_params)

        dst_cluster.config.discovery_spi.prepare_on_start(cluster=dst_cluster)

        ctx = cdc_configurer.configure_source_cluster(src_cluster, dst_cluster, cdc_params)

        src_cluster.start()
        ControlUtility(src_cluster).activate()
        self.on_src_cluster_start(src_cluster)

        dst_cluster.start()
        ControlUtility(dst_cluster).activate()
        self.on_dst_cluster_start(dst_cluster)

        cdc_configurer.start_ignite_cdc(ctx)

        return ctx

    def setup_active_active(self, src_cluster, dst_cluster, cdc_configurer, cdc_params):
        enable_cdc(src_cluster)
        enable_cdc(dst_cluster)

        setup_conflict_resolver(src_cluster, "1", cdc_params)
        setup_conflict_resolver(dst_cluster, "2", cdc_params)

        src_cdc_params = copy(cdc_params)
        dst_cdc_params = copy(cdc_params)

        if isinstance(cdc_configurer, IgniteToKafkaCdcConfigurer):
            src_cdc_params.topic = src_cdc_params.topic + "-src-to-dst"
            src_cdc_params.metadata_topic = src_cdc_params.metadata_topic + "-src-to-dst"

            dst_cdc_params.topic = dst_cdc_params.topic + "-dst-to-src"
            dst_cdc_params.metadata_topic = dst_cdc_params.metadata_topic + "-dst-to-src"

        src_cluster.config.discovery_spi.prepare_on_start(cluster=src_cluster)
        dst_cluster.config.discovery_spi.prepare_on_start(cluster=dst_cluster)

        src_ctx = cdc_configurer.configure_source_cluster(src_cluster, dst_cluster, src_cdc_params)
        dst_ctx = cdc_configurer.configure_source_cluster(dst_cluster, src_cluster, dst_cdc_params)

        src_cluster.start()
        ControlUtility(src_cluster).activate()
        self.on_src_cluster_start(src_cluster)

        dst_cluster.start()
        ControlUtility(dst_cluster).activate()
        self.on_dst_cluster_start(dst_cluster)

        cdc_configurer.start_ignite_cdc(src_ctx)
        cdc_configurer.start_ignite_cdc(dst_ctx)

        return src_ctx, dst_ctx

    def on_src_cluster_start(self, src_cluster):
        pass

    def on_dst_cluster_start(self, dst_cluster):
        pass

    def caches(self):
        return [TEST_CACHE_NAME]

    def check_partitions_are_same(self, source_cluster, target_cluster, strict=True):
        """
        Compare partitions on source and target clusters.

        @:return True if there is no any divergence between partitions on source and target clusters.
        """
        source_cluster_dump = self.dump_partitions(source_cluster)

        target_cluster_dump = self.dump_partitions(target_cluster)

        if source_cluster_dump != target_cluster_dump:
            def diff(source, target):
                return "".join(difflib.unified_diff(
                    source.splitlines(True),
                    target.splitlines(True),
                    "source",
                    "target",
                    n=1)
                )

            self.logger.debug("Partitions are different in source and target clusters:\n"
                              f"{diff(source_cluster_dump, target_cluster_dump)}")
            if strict:
                raise AssertionError("Partitions are different in source and target clusters")
            else:
                return False
        else:
            return True

    def dump_partitions(self, ignite):
        """
        Dump partitions info skipping the cluster-specific fields.

        Saves original dump file in the service log directory.
        """
        dump_filename = ControlUtility(ignite).idle_verify_dump(ignite.nodes[0])

        orig_dump_filename = os.path.join(ignite.log_dir, 'idle_verify_dump_orig.txt')

        ignite.nodes[0].account.ssh(f"mv {dump_filename} {orig_dump_filename}")

        processed_dump_filename = os.path.join(ignite.log_dir, 'idle_verify_dump.txt')

        ignite.nodes[0].account.ssh(
            f"cat {orig_dump_filename} | "
            f"sed -E 's/, partVerHash=[-0-9]+]/]/g' | "
            f"sed -E 's/ consistentId=[^,]+,//g' > "
            f"{processed_dump_filename}")

        return ignite.nodes[0].account.ssh_output(f"cat {processed_dump_filename}").decode("utf-8")

    def start_kafka(self, kafka_nodes, zk_nodes=1):
        zk_settings = ZookeeperSettings()
        zk = ZookeeperService(self.test_context, zk_nodes, settings=zk_settings)

        kafka_settings = KafkaSettings(zookeeper_connection_string=zk.connection_string())
        kafka = KafkaService(self.test_context, kafka_nodes, settings=kafka_settings)

        zk.start_async()
        kafka.start()

        return zk, kafka

    def stop_kafka(self, zk, kafka):
        kafka.stop(force_stop=False, allow_fail=True)

        zk.stop(force_stop=False)

    def run(self, ignite_version, pds, mode, cdc_configurer, cdc_params=None):
        if cdc_params is None:
            cdc_params = CdcParams()

        if cdc_params.caches is None:
            cdc_params.caches = self.caches()

        src_cluster = self.src_cluster(ignite_version, pds)

        dst_cluster = self.dst_cluster(ignite_version, pds)

        if mode == "active-active":
            cdc_streamer_metrics = self.run_active_active(src_cluster, dst_cluster, cdc_configurer, cdc_params,
                                                          self.do_load_active_active)
        else:
            cdc_streamer_metrics = self.run_active_passive(src_cluster, dst_cluster, cdc_configurer, cdc_params,
                                                           self.do_load_active_passive)

        self.logger.info(f"Cdc metrics:\n{json.dumps(cdc_streamer_metrics, indent=4)}")

        self.check_partitions_are_same(src_cluster, dst_cluster, strict=True)

        src_cluster.stop()

        dst_cluster.stop()

        return cdc_streamer_metrics

    def run_active_passive(self, src_cluster, dst_cluster, cdc_configurer, cdc_params, do_load=None):
        cdc_params.kafka_to_ignite_nodes = 2

        src_ctx = self.setup_active_passive(src_cluster, dst_cluster, cdc_configurer, cdc_params)

        if do_load is None:
            self.do_load_active_passive(src_cluster, dst_cluster)
        else:
            do_load(src_cluster, dst_cluster)

        cdc_configurer.wait_cdc(src_ctx, no_new_events_period_secs=10, timeout_sec=300)

        cdc_streamer_metrics = cdc_configurer.stop_ignite_cdc(src_ctx, timeout_sec=300)

        return cdc_streamer_metrics

    def run_active_active(self, src_cluster, dst_cluster, cdc_configurer, cdc_params, do_load=None):
        cdc_params.kafka_to_ignite_nodes = 1

        src_ctx, dst_ctx = self.setup_active_active(src_cluster, dst_cluster, cdc_configurer, cdc_params)

        if do_load is None:
            self.do_load_active_active(src_cluster, dst_cluster)
        else:
            do_load(src_cluster, dst_cluster)

        cdc_configurer.wait_cdc(src_ctx, no_new_events_period_secs=10, timeout_sec=300)
        cdc_configurer.wait_cdc(dst_ctx, no_new_events_period_secs=10, timeout_sec=300)

        cdc_streamer_metrics = {
            "src": cdc_configurer.stop_ignite_cdc(src_ctx, timeout_sec=300),
            "dst": cdc_configurer.stop_ignite_cdc(dst_ctx, timeout_sec=300)
        }

        return cdc_streamer_metrics

    def do_load_active_active(self, src_cluster, dst_cluster):
        client1 = IgniteApplicationService(
            self.test_context,
            self.client_config(src_cluster),
            java_class_name=JAVA_CLIENT_CLASS_NAME,
            num_nodes=1,
            params={"cacheName": TEST_CACHE_NAME, "pacing": 1, "clusterCnt": 2, "clusterIdx": 0, "range": RANGE},
            modules=self.modules())

        client2 = IgniteApplicationService(
            self.test_context,
            self.client_config(dst_cluster),
            java_class_name=JAVA_CLIENT_CLASS_NAME,
            num_nodes=1,
            params={"cacheName": TEST_CACHE_NAME, "pacing": 1, "clusterCnt": 2, "clusterIdx": 1, "range": RANGE},
            modules=self.modules())

        client1.start()
        client2.start()

        sleep(TEST_DURATION_SEC)

        client1.stop()
        client2.stop()

        assert int(client1.extract_result("putCnt")) > 0
        assert int(client1.extract_result("removeCnt")) > 0

        assert int(client2.extract_result("putCnt")) > 0
        assert int(client2.extract_result("removeCnt")) > 0

    def do_load_active_passive(self, src_cluster, dst_cluster):
        client = IgniteApplicationService(
            self.test_context,
            self.client_config(src_cluster),
            java_class_name=JAVA_CLIENT_CLASS_NAME,
            num_nodes=1,
            params={"cacheName": TEST_CACHE_NAME, "pacing": 5, "clusterCnt": 1, "clusterIdx": 0, "range": RANGE},
            modules=self.modules())

        client.start()

        sleep(TEST_DURATION_SEC)

        client.stop()

        assert int(client.extract_result("putCnt")) > 0
        assert int(client.extract_result("removeCnt")) > 0

    def src_cluster(self, ignite_version, pds):
        return IgniteService(self.test_context, self.ignite_config(ignite_version, pds, "src"),
                             2, modules=self.modules())

    def dst_cluster(self, ignite_version, pds):
        return IgniteService(self.test_context, self.ignite_config(ignite_version, pds, "dst"),
                             2, modules=self.modules())

    def modules(self):
        return ["cdc-ext"]

    def client_config(self, ignite):
        return ignite.config._replace(
            client_mode=True,
            data_storage=None,
            # plugins=[],
            # ext_beans=[],
            discovery_spi=from_ignite_cluster(ignite)
        )

    def ignite_config(self, ignite_version, pds, ignite_instance_name):
        config = IgniteConfiguration(
            discovery_spi=TcpDiscoverySpi(ip_finder=TcpDiscoveryVmIpFinder()),
            ignite_instance_name=ignite_instance_name,
            version=IgniteVersion(ignite_version),
            data_storage=DataStorageConfiguration(),
            caches=[CacheConfiguration(name=TEST_CACHE_NAME)],
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


def enable_cdc(cluster):
    cluster.config = cluster.config._replace(
        data_storage=cluster.config.data_storage._replace(
            default=cluster.config.data_storage.default._replace(
                cdc_enabled=True
            )
        )
    )


def setup_conflict_resolver(cluster, cluster_id, cdc_params: CdcParams):
    cluster.config = cluster.config._replace(
        plugins=[*cluster.config.plugins,
                 ('bean.j2',
                  Bean("org.apache.ignite.cdc.conflictresolve.CacheVersionConflictResolverPluginProvider",
                       cluster_id=cluster_id,
                       conflict_resolve_field=cdc_params.conflict_resolve_field,
                       caches=cdc_params.caches))],
    )
