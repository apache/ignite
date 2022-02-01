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
This module contains spark service class.
"""

import os.path
from distutils.version import LooseVersion

from ducktape.cluster.remoteaccount import RemoteCommandError

from ignitetest.services.utils.ducktests_service import DucktestsService
from ignitetest.services.utils.log_utils import monitor_log
from ignitetest.services.utils.path import PathAware


class SparkService(DucktestsService, PathAware):
    """
    Start a spark node.
    """
    def __init__(self, context, num_nodes=3, version=LooseVersion("2.3.4")):
        """
        :param context: test context
        :param num_nodes: number of Ignite nodes.
        """
        super().__init__(context, num_nodes)

        self.log_level = "DEBUG"
        self._version = version
        self.init_logs_attribute()

    @property
    def product(self):
        return "%s-%s" % ("spark", self._version)

    @property
    def globals(self):
        return self.context.globals

    @property
    def config_file(self):
        return None

    @property
    def log_config_file(self):
        return None

    def start(self, **kwargs):
        super().start(**kwargs)

        self.logger.info("Waiting for Spark to start...")

    def start_cmd(self, node):
        """
        Prepare command to start Spark nodes
        """
        if node == self.nodes[0]:
            script = "start-master.sh"
        else:
            script = "start-slave.sh spark://{spark_master}:7077".format(spark_master=self.nodes[0].account.hostname)

        start_script = os.path.join(self.home_dir, "sbin", script)

        cmd = "export SPARK_LOG_DIR={spark_dir}; ".format(spark_dir=self.persistent_root)
        cmd += "export SPARK_WORKER_DIR={spark_dir}; ".format(spark_dir=self.persistent_root)
        cmd += "{start_script} &".format(start_script=start_script)

        return cmd

    def init_logs_attribute(self):
        for node in self.nodes:
            self.logs["master_logs" + node.account.hostname] = {
                "path": self.master_log_path(node),
                "collect_default": True
            }
            self.logs["worker_logs" + node.account.hostname] = {
                "path": self.slave_log_path(node),
                "collect_default": True
            }

    def start_node(self, node, **kwargs):
        self.init_persistent(node)

        cmd = self.start_cmd(node)
        self.logger.debug("Attempting to start SparkService on %s with command: %s" % (str(node.account), cmd))

        if node == self.nodes[0]:
            log_file = self.master_log_path(node)
            log_msg = "Started REST server for submitting applications"
        else:
            log_file = self.slave_log_path(node)
            log_msg = "Successfully registered with master"

        self.logger.debug("Monitoring - %s" % log_file)

        timeout_sec = 30
        with monitor_log(node, log_file) as monitor:
            node.account.ssh(cmd)
            monitor.wait_until(log_msg, timeout_sec=timeout_sec, backoff_sec=.1,
                               err_msg="Spark doesn't start at %d seconds" % timeout_sec)

        if len(self.pids(node)) == 0:
            raise Exception("No process ids recorded on node %s" % node.account.hostname)

    def stop_node(self, node, force_stop=False, **kwargs):
        if force_stop:
            node.account.kill_java_processes(self.java_class_name(node), clean_shutdown=False, allow_fail=True)
        else:
            if node == self.nodes[0]:
                node.account.ssh(os.path.join(self.home_dir, "sbin", "stop-master.sh"))
            else:
                node.account.ssh(os.path.join(self.home_dir, "sbin", "stop-slave.sh"))

    def clean_node(self, node, **kwargs):
        """
        Clean spark persistence files
        """
        super().clean_node(node, **kwargs)

        node.account.ssh("rm -rf -- %s" % self.persistent_root, allow_fail=False)

    def pids(self, node):
        """
        :return: list of service pids on specific node
        """
        try:
            cmd = "jcmd | grep -e %s | awk '{print $1}'" % self.java_class_name(node)
            return list(node.account.ssh_capture(cmd, allow_fail=True, callback=int))
        except (RemoteCommandError, ValueError):
            return []

    def java_class_name(self, node):
        """
        :param node: Spark node.
        :return: Class name depending on node type (master or slave).
        """
        if node == self.nodes[0]:
            return "org.apache.spark.deploy.master.Master"

        return "org.apache.spark.deploy.worker.Worker"

    def master_log_path(self, node):
        """
        :param node: Spark master node.
        :return: Path to log file.
        """
        return "{SPARK_LOG_DIR}/spark-{userID}-org.apache.spark.deploy.master.Master-{instance}-{host}.out".format(
            SPARK_LOG_DIR=self.persistent_root,
            userID=node.account.user,
            instance=1,
            host=node.account.hostname)

    def slave_log_path(self, node):
        """
        :param node: Spark slave node.
        :return: Path to log file.
        """
        return "{SPARK_LOG_DIR}/spark-{userID}-org.apache.spark.deploy.worker.Worker-{instance}-{host}.out".format(
            SPARK_LOG_DIR=self.persistent_root,
            userID=node.account.user,
            instance=1,
            host=node.account.hostname)
