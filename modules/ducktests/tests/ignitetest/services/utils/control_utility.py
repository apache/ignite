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
This module contains control utility wrapper.
"""
import random
import re
from collections import namedtuple

from ducktape.cluster.remoteaccount import RemoteCommandError


class ControlUtility:
    """
    Control utility (control.sh) wrapper.
    """
    BASE_COMMAND = "control.sh"

    def __init__(self, cluster, text_context):
        self._cluster = cluster
        self.logger = text_context.logger

    def baseline(self):
        """
        :return Baseline nodes.
        """
        return self.cluster_state().baseline

    def cluster_state(self):
        """
        :return: Cluster state.
        """
        output = self.__run("--baseline")

        return self.__parse_cluster_state(output)

    def set_baseline(self, baseline):
        """
        :param baseline: Baseline nodes or topology version to set as baseline.
        """
        if isinstance(baseline, int):
            result = self.__run("--baseline version %d --yes" % baseline)
        else:
            result = self.__run("--baseline set %s --yes" %
                                ",".join([node.account.externally_routable_ip for node in baseline]))

        return self.__parse_cluster_state(result)

    def add_to_baseline(self, nodes):
        """
        :param nodes: Nodes that should be added to baseline.
        """
        result = self.__run("--baseline add %s --yes" %
                            ",".join([node.account.externally_routable_ip for node in nodes]))

        return self.__parse_cluster_state(result)

    def remove_from_baseline(self, nodes):
        """
        :param nodes: Nodes that should be removed to baseline.
        """
        result = self.__run("--baseline remove %s --yes" %
                            ",".join([node.account.externally_routable_ip for node in nodes]))

        return self.__parse_cluster_state(result)

    def disable_baseline_auto_adjust(self):
        """
        Disable baseline auto adjust.
        """
        return self.__run("--baseline auto_adjust disable --yes")

    def enable_baseline_auto_adjust(self, timeout=None):
        """
        Enable baseline auto adjust.
        :param timeout: Auto adjust timeout in millis.
        """
        timeout_str = "timeout %d" % timeout if timeout else ""
        return self.__run("--baseline auto_adjust enable %s --yes" % timeout_str)

    def activate(self):
        """
        Activate cluster.
        """
        return self.__run("--activate --yes")

    def deactivate(self):
        """
        Deactivate cluster.
        """
        return self.__run("--deactivate --yes")

    @staticmethod
    def __parse_cluster_state(output):
        state_pattern = re.compile("Cluster state: ([^\\s]+)")
        topology_pattern = re.compile("Current topology version: (\\d+)")
        baseline_pattern = re.compile("Consistent(Id|ID)=([^\\s]+),\\sS(tate|TATE)=([^\\s]+),?(\\sOrder=(\\d+))?")

        match = state_pattern.search(output)
        state = match.group(1) if match else None

        match = topology_pattern.search(output)
        topology = int(match.group(1)) if match else None

        baseline = [BaselineNode(consistent_id=m[1], state=m[3], order=int(m[5]) if m[5] else None)
                    for m in baseline_pattern.findall(output)]

        return ClusterState(state=state, topology_version=topology, baseline=baseline)

    def __run(self, cmd):
        node = random.choice(self.__alives())

        self.logger.debug("Run command %s on node %s", cmd, node.name)

        raw_output = node.account.ssh_capture(self.__form_cmd(node, cmd), allow_fail=True)
        code, output = self.__parse_output(raw_output)

        self.logger.debug("Output of command %s on node %s, exited with code %d, is %s", cmd, node.name, code, output)

        if code != 0:
            raise ControlUtilityError(node.account, cmd, code, output)

        return output

    def __form_cmd(self, node, cmd):
        return self._cluster.path.script("%s --host %s %s" % (self.BASE_COMMAND, node.account.externally_routable_ip,
                                                              cmd))

    @staticmethod
    def __parse_output(raw_output):
        exit_code = raw_output.channel_file.channel.recv_exit_status()
        output = "".join(raw_output)

        pattern = re.compile("Command \\[[^\\s]*\\] finished with code: (\\d+)")
        match = pattern.search(output)

        if match:
            return int(match.group(1)), output
        return exit_code, output

    def __alives(self):
        return [node for node in self._cluster.nodes if self._cluster.alive(node)]


BaselineNode = namedtuple("BaselineNode", ["consistent_id", "state", "order"])
ClusterState = namedtuple("ClusterState", ["state", "topology_version", "baseline"])


class ControlUtilityError(RemoteCommandError):
    """
    Error is raised when control utility failed.
    """
    def __init__(self, account, cmd, exit_status, output):
        super(ControlUtilityError, self).__init__(account, cmd, exit_status, "".join(output))
