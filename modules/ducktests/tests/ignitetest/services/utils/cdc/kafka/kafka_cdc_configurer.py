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

import os, time, math
from abc import ABCMeta, abstractmethod
from typing import NamedTuple

from ignitetest.services.kafka.kafka import KafkaSettings, KafkaService
from ignitetest.services.utils import IgniteServiceType
from ignitetest.services.utils.cdc.cdc_configurer import CdcConfigurer
from ignitetest.services.utils.cdc.cdc_spec import get_cdc_spec
from ignitetest.services.utils.cdc.kafka.kafka_properties_template import KafkaPropertiesTemplate
from ignitetest.services.utils.cdc.kafka.kafka_to_ignite import KafkaToIgniteService
from ignitetest.services.zk.zookeeper import ZookeeperSettings, ZookeeperService
from ignitetest.utils.bean import BeanRef, Bean


class AbstractKafkaCdcConfigurer(CdcConfigurer, metaclass=ABCMeta):
    """
    Abstract base class for IgniteToKafkaCdcStreamer configurer.
    """
    def __init__(self):
        super().__init__()

        self.kafka = None
        self.zk = None
        self.kafka_to_ignite = None

    @abstractmethod
    def get_client_type(self):
        """
        Return ignite client type to be used in kafka-to-ignite.sh.
        :return: Client type.
        """

    def configure_source_cluster(self, source_cluster, target_cluster, cdc_params):
        super().configure_source_cluster(source_cluster, target_cluster, cdc_params)

        source_cluster.spec = get_ignite_to_kafka_spec(source_cluster.spec.__class__,
                                                       self.kafka.connection_string(),
                                                       source_cluster)

    def get_cdc_beans(self, source_cluster, target_cluster, cdc_params):
        beans: list = super().get_cdc_beans(source_cluster, target_cluster, cdc_params)

        zk_settings = ZookeeperSettings()
        self.zk = ZookeeperService(target_cluster.context, 1, settings=zk_settings)
        self.zk.start_async()

        kafka_settings = KafkaSettings(zookeeper_connection_string=self.zk.connection_string())
        self.kafka = KafkaService(target_cluster.context, cdc_params.cdc_kafka_nodes, settings=kafka_settings)
        self.kafka.start()

        ignite_to_kafka_params = IgniteToKafkaCdcStreamerTemplateParams(
            caches=cdc_params.cdc_caches,
            max_batch_size=cdc_params.cdc_max_batch_size,
            only_primary=cdc_params.cdc_only_primary
        )

        if cdc_params.cdc_kafka_partitions is not None:
            ignite_to_kafka_params = ignite_to_kafka_params._replace(
                kafka_partitions=cdc_params.cdc_kafka_partitions)

        self.kafka.create_topic("ignite", partitions=ignite_to_kafka_params.kafka_partitions,
                                retention_ms=cdc_params.cdc_kafka_retention_ms)
        self.kafka.create_topic("ignite-metadata", partitions=ignite_to_kafka_params.kafka_partitions)

        self.kafka_to_ignite = KafkaToIgniteService(
            target_cluster.context,
            self.kafka,
            target_cluster,
            self.get_client_type(),
            num_nodes=cdc_params.cdc_kafka_to_ignite_nodes,
            streamer_config=KafkaToIgniteCdcStreamerTemplateParams(
                kafka_request_timeout=3_000,
                thread_count=cdc_params.cdc_kafka_to_ignite_threads if cdc_params.cdc_kafka_to_ignite_threads
                else math.floor(ignite_to_kafka_params.kafka_partitions /
                                cdc_params.cdc_kafka_to_ignite_nodes)
            ),
            parts=ignite_to_kafka_params.kafka_partitions,
            jvm_opts=target_cluster.spec.jvm_opts,
            merge_with_default=True,
            modules=target_cluster.modules
        )

        self.kafka_to_ignite.start()

        beans.append((
            "ignite_to_kafka_cdc_streamer.j2",
            ignite_to_kafka_params.to_bean()
        ))

        return beans

    def close_cdc_beans(self, timeout_sec):
        start = time.time()

        self.kafka_to_ignite.await_all_consumed(timeout_sec)

        kafka_to_ignite_lag_sec = time.time() - start

        self.kafka_to_ignite.stop()

        self.kafka.stop(force_stop=False, allow_fail=True)

        self.zk.stop(force_stop=False)

        metrics = {
            "kafka_to_ignite_lag_sec": kafka_to_ignite_lag_sec
        }

        return metrics


def get_ignite_to_kafka_spec(base, kafka_connection_string, service):
    """
    :param base: either IgniteNodeSpec or ISENodeSpec
    :param kafka_connection_string: Kafka connection string
    :param service IgniteService
    :return: Spec for ignite-to-kafka application
    """
    class IgniteToKafkaSpec(base):
        def libs(self):
            libs = super().libs()

            libs.extend([os.path.join(self.service.config_dir)])

            return libs

        def config_templates(self):
            templates = super().config_templates()

            templates.extend([
                ("kafka.properties", KafkaPropertiesTemplate("ignite_to_kafka.properties.j2", {
                    "kafka_connection_string": kafka_connection_string
                }))])

            return templates

    return get_cdc_spec(IgniteToKafkaSpec, service)


class IgniteToKafkaCdcStreamerTemplateParams(NamedTuple):
    caches: list
    kafka_partitions: int = 8
    kafka_request_timeout: int = None
    metadata_topic: str = "ignite-metadata"
    max_batch_size: int = None
    only_primary: bool = None
    topic: str = "ignite"

    def to_bean(self, **kwargs):
        filtered = {k: v for k, v in self._asdict().items() if v is not None}

        return Bean("org.apache.ignite.cdc.kafka.IgniteToKafkaCdcStreamer",
                    bean_id="cdcConsumer",
                    **filtered,
                    kafka_properties=BeanRef("kafkaProperties"),
                    **kwargs)


class KafkaToIgniteCdcStreamerTemplateParams(NamedTuple):
    caches: list = None
    kafka_request_timeout: int = None
    max_batch_size: int = None
    metadata_consumer_group: str = None
    metadata_topic: str = "ignite-metadata"
    thread_count: int = None
    topic: str = "ignite"

    def to_bean(self, **kwargs):
        filtered = {k: v for k, v in self._asdict().items() if v is not None}

        return Bean("org.apache.ignite.cdc.kafka.KafkaToIgniteCdcStreamerConfiguration",
                    **filtered, **kwargs)


class CdcIgniteToKafkaToIgniteConfigurer(AbstractKafkaCdcConfigurer):
    """
    Configurer for IgniteToKafkaCdcStreamer with kafka-to-ignite.sh working as Ignite node.
    """
    def get_client_type(self):
        return IgniteServiceType.NODE


class CdcIgniteToKafkaToIgniteClientConfigurer(AbstractKafkaCdcConfigurer):
    """
    Configurer for IgniteToKafkaCdcStreamer with kafka-to-ignite.sh working as Ignite thin client.
    """
    def get_client_type(self):
        return IgniteServiceType.THIN_CLIENT
