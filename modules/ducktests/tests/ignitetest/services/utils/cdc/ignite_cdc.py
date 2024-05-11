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
import os
import re
import signal
from copy import deepcopy

from ignitetest.services.utils.ignite_spec import envs_to_exports
from ignitetest.services.utils.jvm_utils import JvmProcessMixin


class IgniteCdcUtility(JvmProcessMixin):
    """
    Ignite CDC utility (ignite-cdc.sh) wrapper.
    """
    BASE_COMMAND = "ignite-cdc.sh"
    APP_SERVICE_CLASS = "org.apache.ignite.startup.cmdline.CdcCommandLineStartup"

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

            code, output = self.__parse_output(raw_output)

            self.logger.debug(f"{self.__service_node_id(node)}: {self.BASE_COMMAND} finished with exit code: {code}"
                              f"; output: {output}" if code != 0 else "")

        self.logger.info(f"{self.cluster.service_id}: starting {self.BASE_COMMAND} ...")

        self.cluster.exec_on_nodes_async(self.cluster.nodes, __start, timeout_sec=1)

    def stop(self, force_stop=False):
        """
        Stop ignite-cdc.sh on cluster nodes.
        """
        def __stop(node):
            self.logger.info(f"{self.__service_node_id(node)}: stopping {self.BASE_COMMAND}")

            pids = self.pids(node, self.APP_SERVICE_CLASS)

            for pid in pids:
                node.account.signal(pid, signal.SIGKILL if force_stop else signal.SIGTERM, allow_fail=False)

        self.logger.info(f"{self.cluster.service_id}: stopping {self.BASE_COMMAND} ...")

        self.cluster.exec_on_nodes_async(self.cluster.nodes, __stop)

    @property
    def nodes(self):
        return self.cluster.nodes

    def __service_node_id(self, node):
        return f"{self.cluster.service_id} node {self.cluster.idx(node)} on {node.account.hostname}"

    def __form_cmd(self, cmd):
        envs = self.cluster.spec.envs()

        jvm_opts = deepcopy(self.cluster.spec.jvm_opts)

        def replace_ports(opt):
            for port in ["opencensus.metrics.port", "com.sun.management.jmxremote.port"]:
                opt = re.sub(f"-D{port}=(\\d+)", lambda x: f"-D{port}={int(x.group(1)) + 1}", opt)

            return opt

        cdc_jvm_opts = list(map(lambda opt: replace_ports(opt), jvm_opts))

        envs["CDC_JVM_OPTS"] = f"\"{' '.join(cdc_jvm_opts)}\""

        return (f"{envs_to_exports(envs)} bash " + self.cluster.script(cmd) +
                f" 2>&1 | tee -a {os.path.join(self.cluster.log_dir, 'ignite-cdc-console.log')} &")

    @staticmethod
    def __parse_output(raw_output):
        exit_code = raw_output.channel_file.channel.recv_exit_status()
        output = "".join(raw_output)

        return exit_code, output
