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
import time

from ignitetest.services.utils import IgniteServiceType
from ignitetest.services.utils.cdc.cdc_configurer import CdcConfigurer, CdcParams
from ignitetest.services.utils.cdc.kafka.kafka_properties_template import KafkaPropertiesTemplate
from ignitetest.services.utils.cdc.kafka.kafka_to_ignite import KafkaToIgniteService
from ignitetest.utils.bean import BeanRef, Bean


class KafkaCdcParams(CdcParams):
    def __init__(self,
                 caches,
                 kafka,
                 kafka_partitions=16,
                 kafka_request_timeout=None,
                 topic="ignite",
                 metadata_topic="ignite-metadata",
                 kafka_to_ignite_client_type=IgniteServiceType.NODE,
                 kafka_to_ignite_nodes=2,
                 kafka_to_ignite_thread_count=8,
                 kafka_to_ignite_max_batch_size=None,
                 kafka_to_ignite_metadata_consumer_group=None,
                 kafka_to_ignite_kafka_consumer_poll_timeout=None,
                 kafka_to_ignite_kafka_request_timeout=None,
                 kafka_retention_ms=None,
                 **kwargs):
        super().__init__(caches, **kwargs)

        self.kafka = kafka
        self.kafka_partitions = kafka_partitions
        self.kafka_request_timeout = kafka_request_timeout
        self.topic = topic
        self.metadata_topic = metadata_topic
        self.kafka_retention_ms = kafka_retention_ms

        self.kafka_to_ignite_client_type = kafka_to_ignite_client_type
        self.kafka_to_ignite_nodes = kafka_to_ignite_nodes
        self.kafka_to_ignite_thread_count = kafka_to_ignite_thread_count
        self.kafka_to_ignite_max_batch_size = kafka_to_ignite_max_batch_size
        self.kafka_to_ignite_metadata_consumer_group = kafka_to_ignite_metadata_consumer_group
        self.kafka_to_ignite_kafka_consumer_poll_timeout = kafka_to_ignite_kafka_consumer_poll_timeout
        self.kafka_to_ignite_kafka_request_timeout = kafka_to_ignite_kafka_request_timeout


class IgniteToKafkaCdcConfigurer(CdcConfigurer):
    """
    IgniteToKafkaCdcStreamer configurer.
    """
    def __init__(self):
        super().__init__()

        self.kafka_to_ignite = None
        self.cdc_params = None

    def configure_source_cluster(self, src_cluster, dst_cluster, cdc_params: KafkaCdcParams):
        super().configure_source_cluster(src_cluster, dst_cluster, cdc_params)

        src_cluster.spec = get_ignite_to_kafka_spec(src_cluster.spec.__class__,
                                                    cdc_params.kafka.connection_string(),
                                                    src_cluster)

    def get_cdc_beans(self, source_cluster, target_cluster, cdc_params: KafkaCdcParams):
        beans: list = super().get_cdc_beans(source_cluster, target_cluster, cdc_params)

        self.kafka_to_ignite = KafkaToIgniteService(
            target_cluster.context,
            cdc_params.kafka,
            target_cluster,
            cdc_params=cdc_params,
            jvm_opts=target_cluster.spec.jvm_opts,
            merge_with_default=True,
            modules=target_cluster.modules
        )

        beans.append((
            "ignite_to_kafka_cdc_streamer.j2",
            Bean("org.apache.ignite.cdc.kafka.IgniteToKafkaCdcStreamer",
                 bean_id="cdcConsumer",
                 kafka_properties=BeanRef("kafkaProperties"),
                 caches=cdc_params.caches,
                 max_batch_size=cdc_params.max_batch_size,
                 only_primary=cdc_params.only_primary,
                 kafka_partitions=cdc_params.kafka_partitions,
                 kafka_request_timeout=cdc_params.kafka_request_timeout,
                 metadata_topic=cdc_params.metadata_topic,
                 topic=cdc_params.topic)
        ))

        return beans

    def start_ignite_cdc(self, source_cluster):
        self.cdc_params.kafka.create_topic(
            name=self.cdc_params.topic,
            partitions=self.cdc_params.kafka_partitions,
            retention_ms=self.cdc_params.kafka_retention_ms)

        self.cdc_params.kafka.create_topic(
            name=self.cdc_params.metadata_topic,
            partitions=self.cdc_params.kafka_partitions)

        self.kafka_to_ignite.start()

        super().start_ignite_cdc(source_cluster)

    def stop_ignite_cdc(self, source_cluster, timeout_sec):
        super().stop_ignite_cdc(source_cluster, timeout_sec)

        start = time.time()

        self.kafka_to_ignite.await_all_consumed(timeout_sec)

        kafka_to_ignite_lag_sec = time.time() - start

        self.kafka_to_ignite.stop()

        metrics = {
            "kafka_to_ignite_lag_sec": kafka_to_ignite_lag_sec
        }

        return metrics


def get_ignite_to_kafka_spec(base, kafka_connection_string, service):
    """
    :param base: IgniteNodeSpec
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

    return IgniteToKafkaSpec(service, service.spec.jvm_opts, merge_with_default=True)
