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

from ducktape.services.service import Service
from ducktape.utils.util import wait_until


class ZookeeperSettings:
    """
    Settings for zookeeper quorum nodes.
    """
    def __init__(self, tick_time=1000, init_limit=10, sync_limit=5, client_port=2181):
        self.tick_time = tick_time
        self.init_limit = init_limit
        self.sync_limit = sync_limit
        self.client_port = client_port


class ZookeeperService(Service):
    """
    Zookeeper service.
    """
    PERSISTENT_ROOT = "/mnt/zookeeper"
    CONFIG_ROOT = os.path.join(PERSISTENT_ROOT, "conf")
    LOG_FILE = os.path.join(PERSISTENT_ROOT, "zookeeper.log")
    DATA_DIR = os.path.join(PERSISTENT_ROOT, "data")
    CONFIG_FILE = os.path.join(CONFIG_ROOT, "zookeeper.properties")
    LOG_CONFIG_FILE = os.path.join(CONFIG_ROOT, "log4j.properties")
    ZK_LIB_DIR = "/opt/zookeeper-3.5.8/lib"

    logs = {
        "zk_log": {
            "path": LOG_FILE,
            "collect_default": True
        }
    }

    def __init__(self, context, num_nodes, settings=ZookeeperSettings(), start_timeout_sec=60):
        super(ZookeeperService, self).__init__(context, num_nodes)
        self.settings = settings
        self.start_timeout_sec = start_timeout_sec

    def start(self):
        super(ZookeeperService, self).start()
        self.logger.info("Waiting for Zookeeper quorum...")

        for node in self.nodes:
            self.await_quorum(node, self.start_timeout_sec)

        self.logger.info("Zookeeper quorum is formed.")

    def start_node(self, node):
        idx = self.idx(node)

        self.logger.info("Starting Zookeeper node %d on %s", idx, node.account.hostname)

        node.account.ssh("mkdir -p %s" % self.DATA_DIR)
        node.account.ssh("mkdir -p %s" % self.CONFIG_ROOT)
        node.account.ssh("echo %d > %s/myid" % (idx, self.DATA_DIR))

        config_file = self.render('zookeeper.properties.j2', settings=self.settings)
        node.account.create_file(self.CONFIG_FILE, config_file)
        self.logger.info("ZK config %s", config_file)

        log_config_file = self.render('log4j.properties.j2')
        node.account.create_file(self.LOG_CONFIG_FILE, log_config_file)

        start_cmd = "nohup java -cp %s/*:%s org.apache.zookeeper.server.quorum.QuorumPeerMain %s >/dev/null 2>&1 &" % \
                    (self.ZK_LIB_DIR, self.CONFIG_ROOT, self.CONFIG_FILE)

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
        with node.account.monitor_log(self.LOG_FILE) as monitor:
            monitor.offset = 0
            monitor.wait_until(
                "LEADER ELECTION TOOK",
                timeout_sec=timeout,
                err_msg="Zookeeper quorum was not formed on %s" % node.account.hostname
            )

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

    def stop_node(self, node):
        idx = self.idx(node)
        self.logger.info("Stopping %s node %d on %s" % (type(self).__name__, idx, node.account.hostname))
        node.account.kill_process("zookeeper", allow_fail=False)

    def clean_node(self, node):
        self.logger.info("Cleaning Zookeeper node %d on %s", self.idx(node), node.account.hostname)
        if self.alive(node):
            self.logger.warn("%s %s was still alive at cleanup time. Killing forcefully..." %
                             (self.__class__.__name__, node.account))
        node.account.kill_process("zookeeper", clean_shutdown=False, allow_fail=True)
        node.account.ssh("rm -rf %s %s %s" % (self.CONFIG_ROOT, self.DATA_DIR, self.LOG_FILE), allow_fail=False)
