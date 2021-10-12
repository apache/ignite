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

"""
This module contains classes and utilities to start kafka cluster.
"""

import os.path
from distutils.version import LooseVersion

from ducktape.utils.util import wait_until

from ignitetest.services.utils.ducktests_service import DucktestsService
from ignitetest.services.utils.log_utils import monitor_log
from ignitetest.services.utils.path import PathAware

KAFKA_VERSION = "2.8.1"


class KafkaSettings:
    """
    Settings for kafka cluster.
    """
    def __init__(self, version=None, props: dict = None):
        self.config = {
            "broker.id": 0,
            "listeners": "PLAINTEXT://:9092",
            "num.network.threads": 3,
            "num.io.threads": 8,
            "socket.send.buffer.bytes": 102400,
            "socket.receive.buffer.bytes": 102400,
            "socket.request.max.bytes": 104857600,
            "log.dirs": "/tmp/kafka-logs",
            "num.partitions": 1,
            "num.recovery.threads.per.data.dir": 1,
            "offsets.topic.replication.factor": 1,
            "transaction.state.log.replication.factor": 1,
            "transaction.state.log.min.isr": 1,
            "log.retention.hours": 168,
            "log.segment.bytes": 1073741824,
            "log.retention.check.interval.ms": 300000,
            "zookeeper.connect": "localhost:2181",
            "zookeeper.connection.timeout.ms": 18000,
            "group.initial.rebalance.delay.ms": 0
        }

        if props:
            self.config.update(props)

        if version:
            if isinstance(version, str):
                version = LooseVersion(version)
            self.version = version
        else:
            self.version = LooseVersion(KAFKA_VERSION)


class KafkaService(DucktestsService, PathAware):
    """
    Kafka service.
    """
    ZOOKEEPER_LOG = "zookeeper.log"
    KAFKA_LOG = "kafka.log"

    def __init__(self, context, num_nodes, settings=KafkaSettings(), start_timeout_sec=60, start_with_zk=False):
        super().__init__(context, num_nodes)
        self.settings = settings
        self.start_timeout_sec = start_timeout_sec
        self.init_logs_attribute()
        self.start_with_zk = start_with_zk

    @property
    def product(self):
        return "%s-%s" % ("kafka", self.settings.version)

    @property
    def globals(self):
        return self.context.globals

    @property
    def log_config_file(self):
        return os.path.join(self.persistent_root, "log4j.properties")

    @property
    def config_file(self):
        return os.path.join(self.persistent_root, "kafka.properties")

    def start(self, **kwargs):
        super().start(**kwargs)
        self.logger.info("Waiting for Zookeeper quorum...")

        for node in self.nodes:
            self.await_quorum(node, self.start_timeout_sec)

        self.logger.info("Zookeeper quorum is formed.")

    def start_node(self, node, **kwargs):
        idx = self.idx(node)

        self.logger.info("Starting Zookeeper node %d on %s", idx, node.account.hostname)

        self.init_persistent(node)
        node.account.ssh(f"echo {idx} > {self.work_dir}/myid")

        config_file = self.render('kafka.properties.j2', settings=self.settings, data_dir=self.work_dir)
        node.account.create_file(self.config_file, config_file)
        self.logger.info("ZK config %s", config_file)

        log_config_file = self.render('log4j.properties.j2', log_dir=self.log_dir)
        node.account.create_file(self.log_config_file, log_config_file)

        start_cmd = f"nohup java -cp {os.path.join(self.home_dir, 'lib')}/*:{self.persistent_root} " \
                    f"org.apache.zookeeper.server.quorum.QuorumPeerMain {self.config_file} >/dev/null 2>&1 &"

        node.account.ssh(start_cmd)

    def wait_node(self, node, timeout_sec=20):
        wait_until(lambda: not self.alive(node), timeout_sec=timeout_sec)

        return not self.alive(node)

    def await_quorum(self, node, timeout):
        """
        Await quorum formed on node (leader election ready).
        :param node:  Zookeeper service node.
        :param timeout: Wait timeout.
        """
        with monitor_log(node, self.log_file, from_the_beginning=True) as monitor:
            monitor.wait_until(
                "LEADER ELECTION TOOK",
                timeout_sec=timeout,
                err_msg=f"Zookeeper quorum was not formed on {node.account.hostname}"
            )

    @property
    def log_file(self):
        """
        :return: current log file of node.
        """
        return os.path.join(self.log_dir, self.KAFKA_LOG)

    @staticmethod
    def java_class_name():
        """ The class name of the Kafka service. """
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

    def zk_connection_string(self):
        """
        Form a connection string to zookeeper cluster.
        :return: Connection string.
        """
        return ','.join([node.account.hostname + ":" + str(2181) for node in self.nodes])

    def stop_node(self, node, force_stop=False, **kwargs):
        idx = self.idx(node)
        self.logger.info("Stopping %s node %d on %s" % (type(self).__name__, idx, node.account.hostname))
        node.account.kill_process("kafka", clean_shutdown=not force_stop, allow_fail=False)

    def clean_node(self, node, **kwargs):
        super().clean_node(node, **kwargs)

        self.logger.info("Cleaning Kafka node %d on %s", self.idx(node), node.account.hostname)
        node.account.ssh(f"rm -rf -- {self.persistent_root}", allow_fail=False)
