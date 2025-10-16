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
from ignitetest.services.utils.cdc.cdc_helper import CdcHelper, CdcParams
from ignitetest.services.utils.cdc.kafka_to_ignite import KafkaToIgniteService, KafkaPropertiesTemplate
from ignitetest.utils.bean import BeanRef, Bean

DEFAULT_KAFKA_PARTITIONS_COUNT = 16
DEFAULT_KAFKA_TO_IGNITE_NODES = 1
DEFAULT_KAFKA_TO_IGNITE_THREAD_COUNT = 8
DEFAULT_KAFKA_TOPIC = "ignite"
DEFAULT_KAFKA_METADATA_TOPIC = "ignite-metadata"


class IgniteToKafkaCdcHelper(CdcHelper):
    """
    CDC helper for IgniteToKafkaCdcStreamer.
    """
    def configure(self, src_cluster, dst_cluster, cdc_params):
        ctx = super().configure(src_cluster, dst_cluster, cdc_params)

        src_cluster.spec = get_ignite_to_kafka_spec(src_cluster.spec.__class__,
                                                    cdc_params.kafka.connection_string(),
                                                    src_cluster)
        return ctx

    def get_src_cluster_cdc_ext_beans(self, src_cluster, dst_cluster, cdc_params, ctx):
        ctx.kafka_to_ignite = KafkaToIgniteService(
            dst_cluster.context,
            cdc_params.kafka,
            dst_cluster,
            cdc_params=cdc_params,
            jvm_opts=dst_cluster.spec.jvm_opts,
            merge_with_default=True,
            modules=dst_cluster.modules
        )

        beans = super().get_src_cluster_cdc_ext_beans(src_cluster, dst_cluster, cdc_params, ctx)

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

    def start_ignite_cdc(self, ctx):
        ctx.cdc_params.kafka.create_topic(
            name=ctx.cdc_params.topic,
            partitions=ctx.cdc_params.kafka_partitions,
            retention_ms=ctx.cdc_params.kafka_retention_ms)

        ctx.cdc_params.kafka.create_topic(
            name=ctx.cdc_params.metadata_topic,
            partitions=ctx.cdc_params.kafka_partitions)

        ctx.kafka_to_ignite.start()

        super().start_ignite_cdc(ctx)

    def stop_ignite_cdc(self, ctx, timeout_sec):
        super().stop_ignite_cdc(ctx, timeout_sec)

        start = time.time()

        ctx.kafka_to_ignite.await_all_consumed(timeout_sec)

        kafka_to_ignite_lag_sec = time.time() - start

        ctx.kafka_to_ignite.stop()

        metrics = {
            "kafka_to_ignite_lag_sec": kafka_to_ignite_lag_sec
        }

        return metrics


def get_ignite_to_kafka_spec(base, kafka_connection_string, service):
    """
    Dynamically create IgniteSpec subclass to run IgniteToKafkaCdcStreamer in scope of service provided.

    :param base: Base IgniteSpec class.
    :param kafka_connection_string: Kafka connection string.
    :param service: IgniteService.
    :return: IgniteSpec instance for IgniteToKafkaCdcStreamer.
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


class KafkaCdcParams(CdcParams):
    """
    Parameters for Kafka CDC.
    """
    def __init__(self,
                 kafka=None,
                 kafka_partitions=None,
                 kafka_request_timeout=None,
                 topic=None,
                 metadata_topic=None,
                 kafka_to_ignite_client_type=None,
                 kafka_to_ignite_nodes=None,
                 kafka_to_ignite_thread_count=None,
                 kafka_to_ignite_max_batch_size=None,
                 kafka_to_ignite_metadata_consumer_group=None,
                 kafka_to_ignite_kafka_consumer_poll_timeout=None,
                 kafka_to_ignite_kafka_request_timeout=None,
                 kafka_retention_ms=None,
                 **kwargs):
        super().__init__(**kwargs)

        self.kafka = kafka

        self.kafka_partitions = kafka_partitions \
            if kafka_partitions is not None else DEFAULT_KAFKA_PARTITIONS_COUNT

        self.kafka_request_timeout = kafka_request_timeout

        self.topic = topic \
            if topic is not None else DEFAULT_KAFKA_TOPIC

        self.metadata_topic = metadata_topic \
            if metadata_topic is not None else DEFAULT_KAFKA_METADATA_TOPIC

        self.kafka_retention_ms = kafka_retention_ms

        self.kafka_to_ignite_client_type = kafka_to_ignite_client_type \
            if kafka_to_ignite_client_type is not None else IgniteServiceType.NODE

        self.kafka_to_ignite_nodes = kafka_to_ignite_nodes \
            if kafka_to_ignite_nodes is not None else DEFAULT_KAFKA_TO_IGNITE_NODES

        self.kafka_to_ignite_thread_count = kafka_to_ignite_thread_count \
            if kafka_to_ignite_thread_count is not None else DEFAULT_KAFKA_TO_IGNITE_THREAD_COUNT

        self.kafka_to_ignite_max_batch_size = kafka_to_ignite_max_batch_size

        self.kafka_to_ignite_metadata_consumer_group = kafka_to_ignite_metadata_consumer_group

        self.kafka_to_ignite_kafka_consumer_poll_timeout = kafka_to_ignite_kafka_consumer_poll_timeout

        self.kafka_to_ignite_kafka_request_timeout = kafka_to_ignite_kafka_request_timeout
