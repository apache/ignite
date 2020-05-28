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

from ducktape.services.background_thread import BackgroundThreadService

from ignitetest.ignite_utils.ignite_config import IgniteConfig
from ignitetest.ignite_utils.ignite_path import IgnitePath
from ignitetest.version import DEV_BRANCH

"""
The Ignite client application is a main class that implements custom logic.
First CMD param is an absolute path to the Ignite config file.
"""


class IgniteClientApp(BackgroundThreadService):
    # Root directory for persistent output
    PERSISTENT_ROOT = "/mnt/client_app"
    STDOUT_STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "console.log")
    WORK_DIR = os.path.join(PERSISTENT_ROOT, "work")
    CLIENT_CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "ignite-client-config.xml")
    LOG4J_CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "ignite-log4j.xml")

    logs = {
        "console_log": {
            "path": STDOUT_STDERR_CAPTURE,
            "collect_default": True}
    }

    def __init__(self, context, java_class_name, version=DEV_BRANCH, num_nodes=1):
        """
        Args:
            num_nodes:                  number of nodes to use (this should be 1)
        """
        BackgroundThreadService.__init__(self, context, num_nodes)

        self.log_level = "DEBUG"
        self.config = IgniteConfig()
        self.path = IgnitePath()
        self.java_class_name = java_class_name
        self.timeout_sec = 60
        self.stop_timeout_sec = 10

        for node in self.nodes:
            node.version = version

    def start_cmd(self, node):
        """Return the start command appropriate for the given node."""

        cmd = self.env()
        cmd += "%s %s %s 1>> %s 2>> %s " % \
               (self.path.script("ignite.sh", node),
                self.jvm_opts(),
                self.app_args(),
                IgniteClientApp.STDOUT_STDERR_CAPTURE,
                IgniteClientApp.STDOUT_STDERR_CAPTURE)
        return cmd

    def start_node(self, node):
        BackgroundThreadService.start_node(self, node)

    def stop_node(self, node):
        self.logger.info("%s Stopping node %s" % (self.__class__.__name__, str(node.account)))
        node.account.kill_java_processes(self.java_class_name,
                                         clean_shutdown=True,
                                         allow_fail=True)

        stopped = self.wait_node(node, timeout_sec=self.stop_timeout_sec)
        assert stopped, "Node %s: did not stop within the specified timeout of %s seconds" % \
                        (str(node.account), str(self.stop_timeout_sec))

    def clean_node(self, node):
        if self.alive(node):
            self.logger.warn("%s %s was still alive at cleanup time. Killing forcefully..." %
                             (self.__class__.__name__, node.account))

        node.account.kill_java_processes(self.java_class_name,
                                         clean_shutdown=False,
                                         allow_fail=True)

        node.account.ssh("rm -rf %s" % IgniteClientApp.PERSISTENT_ROOT, allow_fail=False)

    def pids(self, node):
        return node.account.java_pids(self.java_class_name)

    def alive(self, node):
        return len(self.pids(node)) > 0

    def _worker(self, idx, node):
        create_client_configs(node, self.config)

        # Just run application.
        cmd = self.start_cmd(node)
        self.logger.info("Ignite client application command: %s", cmd)

        with node.account.monitor_log(IgniteClientApp.STDOUT_STDERR_CAPTURE) as monitor:
            node.account.ssh(cmd, allow_fail=False)
            monitor.wait_until("Ignite Client Finish.", timeout_sec=self.timeout_sec, backoff_sec=5,
                               err_msg="Ignite client don't finish before timeout %s" % self.timeout_sec)

    def app_args(self):
        return IgniteClientApp.CLIENT_CONFIG_FILE

    def jvm_opts(self):
        return "-J-DIGNITE_SUCCESS_FILE=" + IgniteClientApp.PERSISTENT_ROOT + "/success_file " + \
               "-J-Dlog4j.configDebug=true " \
               "-J-Xmx1G"

    def env(self):
        return "export MAIN_CLASS={main_class}; ".format(main_class=self.java_class_name) + \
               "export EXCLUDE_TEST_CLASSES=true; " + \
               "export IGNITE_LOG_DIR={log_dir}; ".format(log_dir=IgniteClientApp.PERSISTENT_ROOT)


class SparkIgniteClientApp(IgniteClientApp):
    def __init__(self, context, master_node):
        IgniteClientApp.__init__(self, context, java_class_name="org.apache.ignite.internal.test.SparkApplication")
        self.master_node = master_node
        self.timeout_sec = 120

    def app_args(self):
        return " spark://" + self.master_node.account.hostname + ":7077"

    def env(self):
        return IgniteClientApp.env(self) + \
               "export EXCLUDE_MODULES=\"kubernetes,aws,gce,mesos,rest-http,web-agent,zookeeper,serializers,store," \
               "rocketmq\"; "


def create_client_configs(node, config):
    node.account.mkdirs(IgniteClientApp.PERSISTENT_ROOT)
    node.account.create_file(IgniteClientApp.CLIENT_CONFIG_FILE,
                             config.render(IgniteClientApp.PERSISTENT_ROOT,
                                           IgniteClientApp.WORK_DIR,
                                           "true"))
    node.account.create_file(IgniteClientApp.LOG4J_CONFIG_FILE,
                             config.render_log4j(IgniteClientApp.WORK_DIR))
