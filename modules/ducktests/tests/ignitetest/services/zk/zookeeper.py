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
This module contains classes and utilities to start zookeeper cluster for testing ZookeeperDiscovery.
"""

import os.path
from looseversion import LooseVersion

from ducktape.utils.util import wait_until

from ignitetest.services.utils.ducktests_service import DucktestsService
from ignitetest.services.utils.log_utils import monitor_log
from ignitetest.services.utils.path import PathAware


class ZookeeperSettings:
    """
    Settings for zookeeper quorum nodes.
    """
    def __init__(self, **kwargs):
        self.min_session_timeout = kwargs.get('min_session_timeout', 2000)
        self.tick_time = kwargs.get('tick_time', self.min_session_timeout // 3)
        self.init_limit = kwargs.get('init_limit', 10)
        self.sync_limit = kwargs.get('sync_limit', 5)
        self.force_sync = kwargs.get('force_sync', 'no')
        self.client_port = kwargs.get('client_port', 2181)

        version = kwargs.get("version")
        if version:
            if isinstance(version, str):
                version = LooseVersion(version)
            self.version = version
        else:
            self.version = LooseVersion("3.5.8")

        assert self.tick_time <= self.min_session_timeout // 2, "'tick_time' must be <= 'min_session_timeout' / 2"


class ZookeeperService(DucktestsService, PathAware):
    """
    Zookeeper service.
    """
    LOG_FILENAME = "zookeeper.log"

    def __init__(self, context, num_nodes, settings=ZookeeperSettings(), start_timeout_sec=60):
        super().__init__(context, num_nodes)
        self.settings = settings
        self.start_timeout_sec = start_timeout_sec
        self.init_logs_attribute()

    @property
    def product(self):
        return "%s-%s" % ("zookeeper", self.settings.version)

    @property
    def globals(self):
        return self.context.globals

    @property
    def log_config_file(self):
        return os.path.join(self.persistent_root, "log4j.properties")

    @property
    def config_file(self):
        return os.path.join(self.persistent_root, "zookeeper.properties")

    def start(self, **kwargs):
        self.start_async(**kwargs)
        self.await_started()

    def start_async(self, **kwargs):
        """
        Starts in async way.
        """
        super().start(**kwargs)

    def await_started(self):
        self.logger.info("Waiting for Zookeeper quorum...")

        for node in self.nodes:
            self.await_quorum(node, self.start_timeout_sec)

        self.logger.info("Zookeeper quorum is formed.")

    def start_node(self, node, **kwargs):
        idx = self.idx(node)

        self.logger.info("Starting Zookeeper node %d on %s", idx, node.account.hostname)

        self.init_persistent(node)
        node.account.ssh(f"echo {idx} > {self.work_dir}/myid")

        config_file = self.render('zookeeper.properties.j2', settings=self.settings, data_dir=self.work_dir)
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
        return os.path.join(self.log_dir, self.LOG_FILENAME)

    @staticmethod
    def java_class_name():
        """ The class name of the Zookeeper quorum peers. """
        return "org.apache.zookeeper.server.quorum.QuorumPeerMain"

    def pids(self, node):
        """
        Get pids of zookeeper service node.
        :param node: Zookeeper service node.
        :return: List of pids.
        """
        return node.account.java_pids(self.java_class_name())

    def alive(self, node):
        """
        Check if zookeeper service node is alive.
        :param node: Zookeeper service node.
        :return: True if node is alive
        """
        return len(self.pids(node)) > 0

    def connection_string(self):
        """
        Form a connection string to zookeeper cluster.
        :return: Connection string.
        """
        return ','.join([node.account.hostname + ":" + str(2181) for node in self.nodes])

    def stop_node(self, node, force_stop=False, **kwargs):
        idx = self.idx(node)
        self.logger.info("Stopping %s node %d on %s" % (type(self).__name__, idx, node.account.hostname))
        node.account.kill_process("zookeeper", clean_shutdown=not force_stop, allow_fail=False)

    def clean_node(self, node, **kwargs):
        super().clean_node(node, **kwargs)

        self.logger.info("Cleaning Zookeeper node %d on %s", self.idx(node), node.account.hostname)
        node.account.ssh(f"rm -rf -- {self.persistent_root}", allow_fail=False)
