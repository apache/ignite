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
# limitations under the License.

import os
from looseversion import LooseVersion
from typing import NamedTuple

from ducktape.utils.util import wait_until

from ignitetest.services.utils.ducktests_service import DucktestsService
from ignitetest.services.utils.log_utils import monitor_log
from ignitetest.services.utils.path import PathAware


class KafkaSettings:
    """
    Settings for kafka nodes.
    """
    def __init__(self, **kwargs):
        self.zookeeper_connection_string = kwargs.get("zookeeper_connection_string")
        self.port = kwargs.get("port", 9092)

        self.host = None
        self.broker_id = None

        version = kwargs.get("version")
        if version:
            if isinstance(version, str):
                version = LooseVersion(version)

            self.version = version
        else:
            self.version = LooseVersion("3.9.1")


class KafkaService(DucktestsService, PathAware):
    """
    Kafka service.
    """
    LOG_FILENAME = "kafka.log"

    def __init__(self, context, num_nodes, settings: KafkaSettings, start_timeout_sec=60):
        super().__init__(context, num_nodes)
        self.settings = settings
        self.start_timeout_sec = start_timeout_sec
        self.init_logs_attribute()

    @property
    def product(self):
        return "%s-%s" % ("kafka", self.settings.version)

    @property
    def globals(self):
        return self.context.globals

    @property
    def log_config_file(self):
        return os.path.join(self.config_dir, "log4j.properties")

    @property
    def config_file(self):
        return os.path.join(self.config_dir, "server.properties")

    def start(self, **kwargs):
        super().start(**kwargs)
        self.logger.info("Waiting for Kafka ...")

        for node in self.nodes:
            self.await_kafka(node, self.start_timeout_sec)

        self.logger.info("Kafka cluster is formed.")

    def start_node(self, node, **kwargs):
        idx = self.idx(node)

        self.logger.info("Starting Kafka broker %d on %s", idx, node.account.hostname)

        self.init_persistent(node)

        self.settings.host=node.account.externally_routable_ip
        self.settings.broker_id=idx

        config_file = self.render('server.properties.j2', settings=self.settings, data_dir=self.work_dir)
        node.account.create_file(self.config_file, config_file)
        self.logger.info("Kafka config %s", config_file)

        log_config_file = self.render('log4j.properties.j2', log_dir=self.log_dir)
        node.account.create_file(self.log_config_file, log_config_file)

        start_cmd = f"nohup java -Dlog4j.configuration=file:{self.log_config_file} " \
                    f"-cp {os.path.join(self.home_dir, 'libs')}/*:{self.config_dir} " \
                    f"kafka.Kafka {self.config_file} >/tmp/log 2>&1 &"

        node.account.ssh(start_cmd)

    def wait_node(self, node, timeout_sec=20):
        wait_until(lambda: not self.alive(node), timeout_sec=timeout_sec)

        return not self.alive(node)

    def await_kafka(self, node, timeout):
        """
        Await kafka broker started on node.
        :param node: Kafka service node.
        :param timeout: Wait timeout.
        """
        with monitor_log(node, self.log_file, from_the_beginning=True) as monitor:
            monitor.wait_until("KafkaServer.*started",
                               timeout_sec=timeout,
                               err_msg=f"Kafka cluster was not formed on {node.account.hostname}")

    def create_topic(self, name, partitions=1, replication_factor=1, retention_ms=None):
        """
        Create kafka topic
        :param name: Topic name
        :param partitions: Number of partitions
        :param replication_factor: Replication factor
        :param retention_ms: Retention in milliseconds
        """
        create_topic_cmd = f"{os.path.join(self.home_dir, 'bin', 'kafka-topics.sh')} --create " \
                           f"--topic {name} --bootstrap-server {self.connection_string()} " \
                           f"--partitions {partitions} --replication-factor {replication_factor}"

        if retention_ms is not None:
            create_topic_cmd = create_topic_cmd + f" --config retention.ms={retention_ms}"

        self.nodes[0].account.ssh(create_topic_cmd)

    def offsets(self, topics=None):
        """
        Return offset info for all consumer groups and partitions.
        :param topics: List of topics to process. Return info about all topics if None.
        """
        kafka_consumer_groups_cmd =\
            f"{os.path.join(self.home_dir, 'bin', 'kafka-consumer-groups.sh')} --describe " \
            f"--all-groups --bootstrap-server {self.connection_string()} 2>/dev/null |" \
            f"grep . | grep -v GROUP | sed -E 's/  */ /g'"

        def callback(line):
            fields = line.strip().split(' ')

            return ConsumerOffsetInfo(
                group=fields[0],
                topic=fields[1],
                part=int(fields[2]),
                current_offset=int(fields[3]),
                log_end_offset=int(fields[4]),
                lag=int(fields[5]),
                consumer_id=fields[6] if fields[6] != '-' else None,
                host=fields[7] if fields[7] != '-' else None,
                client_id=fields[8] if fields[8] != '-' else None
            )

        offsets = self.nodes[0].account.ssh_capture(kafka_consumer_groups_cmd, callback=callback)

        return [o for o in offsets if not topics or (o.topic in topics)]

    @property
    def log_file(self):
        """
        :return: current log file of node.
        """
        return os.path.join(self.log_dir, self.LOG_FILENAME)

    @staticmethod
    def java_class_name():
        """ The class name of the Kafka broker. """
        return "kafka.Kafka"

    def pids(self, node):
        """
        Get pids of kafka service node.
        :param node: Kafka service node.
        :return: List of pids.
        """
        return node.account.java_pids(self.java_class_name())

    def alive(self, node):
        """
        Check if kafka service node is alive.
        :param node: Kafka service node.
        :return: True if node is alive
        """
        return len(self.pids(node)) > 0

    def connection_string(self):
        """
        Form a connection string to kafka cluster.
        :return: Connection string.
        """
        return ','.join([node.account.hostname + ":" + str(self.settings.port) for node in self.nodes])

    def stop_node(self, node, force_stop=True, **kwargs):
        idx = self.idx(node)
        self.logger.info("Stopping %s node %d on %s" % (type(self).__name__, idx, node.account.hostname))

        allow_fail = kwargs["allow_fail"] if "allow_fail" in kwargs else False

        node.account.kill_process("kafka", clean_shutdown=not force_stop, allow_fail=allow_fail)

    def clean_node(self, node, **kwargs):
        super().clean_node(node, **kwargs)

        self.logger.info("Cleaning Kafka node %d on %s", self.idx(node), node.account.hostname)
        node.account.ssh(f"rm -rf -- {self.persistent_root}", allow_fail=False)


class ConsumerOffsetInfo(NamedTuple):
    """
    Consumer offset record.
    """
    group: str
    topic: str
    part: int
    current_offset: int
    log_end_offset: int
    lag: int
    consumer_id: str
    host: str
    client_id: str
