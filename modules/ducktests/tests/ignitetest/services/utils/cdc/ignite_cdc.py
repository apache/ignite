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
This module contains Ignite CDC utility (ignite-cdc.sh) wrapper.
"""
import signal

from ignitetest.services.utils.ignite_spec import envs_to_exports


class IgniteCdcUtility:
    """
    Ignite CDC utility (ignite-cdc.sh) wrapper.
    """
    BASE_COMMAND = "ignite-cdc.sh"
    JAVA_CLASS = "org.apache.ignite.startup.cmdline.CdcCommandLineStartup"

    def __init__(self, cluster):
        self.cluster = cluster
        self.logger = cluster.context.logger

    def start(self):
        """
        Start ignite-cdc.sh on cluster nodes.
        """
        def __start(node):
            self.logger.info(f"{self.__service_node_id(node)}: starting {self.BASE_COMMAND}")

            raw_output = node.account.ssh_capture(
                self.__form_cmd(f"{self.BASE_COMMAND} -v {self.cluster.config_file}"))

            code, _ = self.__parse_output(raw_output)

            self.logger.debug(f"{self.__service_node_id(node)}: {self.BASE_COMMAND} finished with exit code: {code}")

        self.logger.info(f"{self.cluster.service_id}: starting {self.BASE_COMMAND} ...")

        self.cluster.exec_on_nodes_async(self.cluster.nodes, __start, timeout_sec=1)

    def stop(self, force_stop=False):
        """
        Stop ignite-cdc.sh on cluster nodes.
        """
        def __stop(node):
            self.logger.info(f"{self.__service_node_id(node)}: stopping {self.BASE_COMMAND}")

            pids = self.__pids(node)

            for pid in pids:
                node.account.signal(pid, signal.SIGKILL if force_stop else signal.SIGTERM, allow_fail=False)

        self.logger.info(f"{self.cluster.service_id}: stopping {self.BASE_COMMAND} ...")

        self.cluster.exec_on_nodes_async(self.cluster.nodes, __stop)

    def __service_node_id(self, node):
        return f"{self.cluster.service_id} node {self.cluster.idx(node)} on {node.account.hostname}"

    def __pids(self, node):
        cmd = "pgrep -ax java | awk '/%s/ {print $1}'" % self.JAVA_CLASS

        return [int(pid) for pid in node.account.ssh_capture(cmd, allow_fail=True)]

    def __form_cmd(self, cmd):
        envs = self.cluster.spec.envs()

        envs["CDC_JVM_OPTS"] = f"\"{' '.join(self.cluster.spec.jvm_opts)}\""

        return f"{envs_to_exports(envs)} bash " + self.cluster.script(cmd)

    @staticmethod
    def __parse_output(raw_output):
        exit_code = raw_output.channel_file.channel.recv_exit_status()
        output = "".join(raw_output)

        return exit_code, output
