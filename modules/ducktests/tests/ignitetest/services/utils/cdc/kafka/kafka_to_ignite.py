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

"""
This module contains kafka-to-ignite.sh utility wrapper.
"""
import os
import time
from copy import deepcopy

from ignitetest.services.ignite import IgniteService
from ignitetest.services.utils import IgniteServiceType
from ignitetest.services.utils.cdc.kafka.kafka_properties_template import KafkaPropertiesTemplate
from ignitetest.services.utils.ignite_configuration import IgniteThinClientConfiguration
from ignitetest.services.utils.ignite_spec import envs_to_exports
from ignitetest.utils.bean import Bean


class KafkaToIgniteService(IgniteService):
    """
    Kafka to Ignite utility (kafka-to-ignite.sh) wrapper.
    """
    def __init__(self, context, kafka, target_cluster, cdc_params, jvm_opts=None,
                 merge_with_default=True, startup_timeout_sec=60, shutdown_timeout_sec=60, modules=None):
        def add_cdc_ext_module(_modules):
            if _modules:
                _modules.append("cdc-ext")
            else:
                _modules = ["cdc-ext"]

            return _modules

        assert cdc_params.kafka_to_ignite_nodes <= cdc_params.kafka_partitions, \
            (f"number of nodes ({cdc_params.kafka_to_ignite_nodes}) more then "
             f"kafka topic partitions ({cdc_params.kafka_partitions})")

        super().__init__(context, self.__get_config(target_cluster, cdc_params.kafka_to_ignite_client_type),
                         cdc_params.kafka_to_ignite_nodes, jvm_opts, merge_with_default, startup_timeout_sec,
                         shutdown_timeout_sec, add_cdc_ext_module(modules))

        self.cdc_params = cdc_params

        self.main_java_class = "org.apache.ignite.cdc.kafka.KafkaToIgniteCommandLineStartup"

        self.spec = get_kafka_to_ignite_spec(self.spec.__class__,
                                             kafka.connection_string(), self)

        self.spec.jvm_opts += ["-Dlog4j.configurationFile=file:" + self.log_config_file]

        self.kafka = kafka

    def _prepare_configs(self, node):
        parts_per_node = round(self.cdc_params.kafka_partitions / self.num_nodes)

        idx = self.idx(node) - 1

        parts_from = idx * parts_per_node

        if idx == self.num_nodes - 1:
            parts_to = self.cdc_params.kafka_partitions
        else:
            parts_to = (idx + 1) * parts_per_node

        self.config = self.__add_kafka_streamer_config(parts_from, parts_to)

        super()._prepare_configs(node)

    def __add_kafka_streamer_config(self, parts_from, parts_to):
        ext_beans = [("bean.j2", Bean(
                "org.apache.ignite.cdc.kafka.KafkaToIgniteCdcStreamerConfiguration",
                caches=self.cdc_params.caches,
                max_batch_size=self.cdc_params.kafka_to_ignite_max_batch_size,
                kafka_request_timeout=self.cdc_params.kafka_to_ignite_kafka_request_timeout,
                kafka_consumer_poll_timeout=self.cdc_params.kafka_to_ignite_kafka_consumer_poll_timeout,
                metadata_consumer_group=self.cdc_params.kafka_to_ignite_metadata_consumer_group,
                metadata_topic=self.cdc_params.metadata_topic,
                topic=self.cdc_params.topic,
                thread_count=self.cdc_params.kafka_to_ignite_thread_count,
                kafka_parts_to=parts_to,
                kafka_parts_from=parts_from)),
            ("properties_macro.j2", {
                "id": "kafkaProperties",
                "location": "kafka.properties"
            })
        ]

        return self.config._replace(ext_beans=ext_beans)

    @staticmethod
    def __get_config(target_cluster, client_type):
        if client_type == IgniteServiceType.NODE:
            return target_cluster.config._replace(
                client_mode=True
            )
        else:
            addresses = [target_cluster.nodes[0].account.hostname + ":" +
                         str(target_cluster.config.client_connector_configuration.port)]

            return IgniteThinClientConfiguration(
                addresses=addresses,
                version=target_cluster.config.version
            )

    def await_started(self):
        """
        Awaits start finished.
        """
        self.logger.info("Waiting for KafkaToIgnite(s) to start ...")

        self.await_event(">>> Kafka partitions", self.startup_timeout_sec, from_the_beginning=True,
                         log_file="kafka-ignite-streamer.log")

    def await_all_consumed(self, timeout_sec):
        """
        Awaits all events are consumed from Kafka
        :param timeout_sec: Timeout
        """
        start = time.time()
        end = start + timeout_sec

        while True:
            now = time.time()
            if now > end:
                raise TimeoutError(f"Timed out waiting {timeout_sec} seconds for kafka_to_ignite.sh "
                                   f"to consume all events from kafka.")

            offsets = self.kafka.offsets([
                self.cdc_params.topic,
                self.cdc_params.metadata_topic
            ])

            all_consumed = all(map(lambda o: o.lag == 0, offsets))

            if all_consumed:
                self.logger.info("All events are consumed from kafka.")

                break
            elif any([not self.alive(n) for n in self.nodes]):
                raise RuntimeError("Some kafka_to_ignite.sh instances are not alive while not "
                                   "all events are consumed from kafka.")
            else:
                for offset in offsets:
                    if offset.lag > 0:
                        self.logger.debug(f"offsets: [topic={offset.topic}, part={offset.part}, "
                                          f"lag={offset.lag}]")

                time.sleep(2)


def get_kafka_to_ignite_spec(base, kafka_connection_string, service):
    """
    :param base: either IgniteNodeSpec or ISENodeSpec
    :param kafka_connection_string: Kafka connection string
    :param service IgniteService
    :return: Spec for kafka-to-ignite application
    """
    class KafkaToIgniteSpec(base):
        def command(self, _):
            envs = deepcopy(self.envs())

            envs["IGNITE_HOME"] = self.service.home_dir

            cmd = "%s %s %s %s 2>&1 | tee -a %s &" % \
                  (envs_to_exports(envs),
                   self.script("kafka-to-ignite.sh"),
                   self.config_file_path(),
                   self._jvm_opts(),
                   os.path.join(self.service.log_dir, "console.log"))

            return cmd

        def libs(self):
            libs = super().libs()

            libs.extend([os.path.join(self.service.config_dir)])

            return libs

        def config_templates(self):
            templates = super().config_templates()

            templates.extend([
                ("kafka.properties", KafkaPropertiesTemplate("kafka_to_ignite.properties.j2", {
                    "kafka_connection_string": kafka_connection_string
                }))])

            return templates

        def config_file_path(self):
            return self.service.config_file if self.service.config.service_type == IgniteServiceType.NODE \
                else self.service.thin_client_config_file

        def script(self, cmd):
            if self.service.config.version.is_dev:
                return os.path.join(self.service.spec.extensions_home(), "modules", "cdc-ext", "bin", cmd)
            else:
                return self.service.script(cmd)

    return KafkaToIgniteSpec(service, service.spec.jvm_opts, merge_with_default=True)
