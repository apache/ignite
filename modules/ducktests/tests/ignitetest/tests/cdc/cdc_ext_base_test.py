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
from copy import copy

from ignitetest.services.kafka.kafka import KafkaSettings, KafkaService
from ignitetest.services.utils.cdc.cdc_configurer import CdcParams
from ignitetest.services.utils.cdc.ignite_to_kafka_cdc_configurer import IgniteToKafkaCdcConfigurer
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.zk.zookeeper import ZookeeperSettings, ZookeeperService
from ignitetest.utils.bean import Bean
from ignitetest.utils.ignite_test import IgniteTest


class CdcExtBaseTest(IgniteTest):
    def start_active_passive(self, src_cluster, dst_cluster, cdc_configurer, cdc_params):
        enable_cdc(src_cluster)

        ctx = cdc_configurer.configure_source_cluster(src_cluster, dst_cluster, cdc_params)

        dst_cluster.start()
        ControlUtility(dst_cluster).activate()

        src_cluster.start()
        ControlUtility(src_cluster).activate()

        cdc_configurer.start_ignite_cdc(ctx)

        return ctx

    def start_active_active(self, src_cluster, dst_cluster, cdc_configurer, cdc_params):
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

        dst_cluster.start()
        ControlUtility(dst_cluster).activate()

        cdc_configurer.start_ignite_cdc(src_ctx)
        cdc_configurer.start_ignite_cdc(dst_ctx)

        return src_ctx, dst_ctx

    def check_partitions_are_same(self, source_cluster, target_cluster):
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
