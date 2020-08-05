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

from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.services.service import Service

from ignitetest.services.utils.ignite_aware import IgniteAwareService
from ignitetest.tests.utils.version import DEV_BRANCH


class SparkService(IgniteAwareService):
    """
    Start a spark node.
    """
    INSTALL_DIR = "/opt/spark-{version}".format(version="2.3.4")
    SPARK_PERSISTENT_ROOT = "/mnt/spark"

    logs = {}

    # pylint: disable=R0913
    def __init__(self, context, modules=None, version=DEV_BRANCH, num_nodes=3, properties=""):
        """
        :param context: test context
        :param num_nodes: number of Ignite nodes.
        """
        modules = modules or []
        modules.extend(["ignite-spark"])

        IgniteAwareService.__init__(self, context, num_nodes, modules, False, version, properties)

        self.log_level = "DEBUG"

        for node in self.nodes:
            self.logs["master_logs" + node.account.hostname] = {
                "path": self.master_log_path(node),
                "collect_default": True
            }
            self.logs["worker_logs" + node.account.hostname] = {
                "path": self.slave_log_path(node),
                "collect_default": True
            }

    def start(self):
        Service.start(self)

        self.logger.info("Waiting for Spark to start...")

    def start_cmd(self, node):
        if node == self.nodes[0]:
            script = "start-master.sh"
        else:
            script = "start-slave.sh spark://{spark_master}:7077".format(spark_master=self.nodes[0].account.hostname)

        start_script = os.path.join(SparkService.INSTALL_DIR, "sbin", script)

        cmd = "export SPARK_LOG_DIR={spark_dir}; ".format(spark_dir=SparkService.SPARK_PERSISTENT_ROOT)
        cmd += "export SPARK_WORKER_DIR={spark_dir}; ".format(spark_dir=SparkService.SPARK_PERSISTENT_ROOT)
        cmd += "{start_script} &".format(start_script=start_script)

        return cmd

    # pylint: disable=W0221
    def start_node(self, node, timeout_sec=30):
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

        with node.account.monitor_log(log_file) as monitor:
            node.account.ssh(cmd)
            monitor.wait_until(log_msg, timeout_sec=timeout_sec, backoff_sec=5,
                               err_msg="Spark doesn't start at %d seconds" % timeout_sec)

        if len(self.pids(node)) == 0:
            raise Exception("No process ids recorded on node %s" % node.account.hostname)

    def stop_node(self, node):
        if node == self.nodes[0]:
            node.account.ssh(os.path.join(SparkService.INSTALL_DIR, "sbin", "stop-master.sh"))
        else:
            node.account.ssh(os.path.join(SparkService.INSTALL_DIR, "sbin", "stop-slave.sh"))

    def clean_node(self, node):
        node.account.kill_java_processes(self.java_class_name(node),
                                         clean_shutdown=False, allow_fail=True)
        node.account.ssh("sudo rm -rf -- %s" % SparkService.SPARK_PERSISTENT_ROOT, allow_fail=False)

    def pids(self, node):
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

    @staticmethod
    def master_log_path(node):
        """
        :param node: Spark master node.
        :return: Path to log file.
        """
        return "{SPARK_LOG_DIR}/spark-{userID}-org.apache.spark.deploy.master.Master-{instance}-{host}.out".format(
            SPARK_LOG_DIR=SparkService.SPARK_PERSISTENT_ROOT,
            userID=node.account.user,
            instance=1,
            host=node.account.hostname)

    @staticmethod
    def slave_log_path(node):
        """
        :param node: Spark slave node.
        :return: Path to log file.
        """
        return "{SPARK_LOG_DIR}/spark-{userID}-org.apache.spark.deploy.worker.Worker-{instance}-{host}.out".format(
            SPARK_LOG_DIR=SparkService.SPARK_PERSISTENT_ROOT,
            userID=node.account.user,
            instance=1,
            host=node.account.hostname)
