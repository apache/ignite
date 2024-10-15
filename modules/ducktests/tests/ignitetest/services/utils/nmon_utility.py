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
This module contains nmon (system metrics collector) wrapper.
"""


class NmonUtility:
    """
    nmon wrapper.
    """
    BASE_COMMAND = "nmon"

    def __init__(self, cluster):
        self.cluster = cluster
        self.logger = cluster.context.logger

    def start(self, freq_sec=5, count=100000, extra_args=""):
        """
        Start nmon on cluster nodes.
        """
        def __start(node):
            self.logger.info(f"{self.__service_node_id(node)}: starting {self.BASE_COMMAND}")

            code = node.account.ssh(f"cd {self.cluster.log_dir}; {self.BASE_COMMAND} -ftD -g auto -s {freq_sec} "
                                    f"-c {count} -F {node.name}.nmon {extra_args}")

            self.logger.debug(f"{self.__service_node_id(node)}: {self.BASE_COMMAND} finished with exit code: {code}")

        self.logger.info(f"{self.cluster.service_id}: starting {self.BASE_COMMAND} ...")

        self.cluster.exec_on_nodes_async(self.cluster.nodes, __start, timeout_sec=1)

    def stop(self):
        """
        Stop nmon on cluster nodes.
        """
        def __stop(node):
            self.logger.info(f"{self.__service_node_id(node)}: stopping {self.BASE_COMMAND}")

            node.account.ssh(f"pkill -USR2 {self.BASE_COMMAND}")

        self.logger.info(f"{self.cluster.service_id}: stopping {self.BASE_COMMAND} ...")

        self.cluster.exec_on_nodes_async(self.cluster.nodes, __stop)

    def __service_node_id(self, node):
        return f"{self.cluster.service_id} node {self.cluster.idx(node)} on {node.account.hostname}"
