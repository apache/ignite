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
This module contains control.sh utility tests.
"""
import random
import re
import time
from collections import namedtuple

from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.mark import parametrize
from ducktape.mark.resource import cluster
from jinja2 import Template

from ignitetest.services.ignite import IgniteService
from ignitetest.tests.utils.ignite_test import IgniteTest
from ignitetest.tests.utils.version import DEV_BRANCH, LATEST_2_8, IgniteVersion, LATEST_2_7

BaselineNode = namedtuple("BaselineNode", ["consistent_id", "state", "order"])
ClusterState = namedtuple("ClusterState", ["state", "topology_version", "baseline"])

# pylint: disable=W0223
class BaselineTests(IgniteTest):
    """
    Tests baseline command
    """
    NUM_NODES = 3

    CONFIG_TEMPLATE = """
        {% if version > "2.9.0" %}
            <property name="clusterStateOnStart" value="INACTIVE"/>
        {%  else %}
            <property name="activeOnStart" value="false"/>
        {% endif %}
        <property name="dataStorageConfiguration">
            <bean class="org.apache.ignite.configuration.DataStorageConfiguration">
                <property name="defaultDataRegionConfiguration">
                    <bean class="org.apache.ignite.configuration.DataRegionConfiguration">
                        <property name="persistenceEnabled" value="true"/>
                        <property name="maxSize" value="#{100L * 1024 * 1024}"/>
                    </bean>
                </property>
            </bean>
        </property>
    """

    @staticmethod
    def properties(version):
        return Template(BaselineTests.CONFIG_TEMPLATE) \
            .render(version=version)

    def __init__(self, test_context):
        super(BaselineTests, self).__init__(test_context)
        self.servers = None

    @cluster(num_nodes=NUM_NODES)
    @parametrize(version=str(DEV_BRANCH))
    @parametrize(version=str(LATEST_2_8))
    @parametrize(version=str(LATEST_2_7))
    def test_baseline(self, version):
        """
        Test setting baseline.
        """
        self.servers = self.__start_ignite_nodes(version, self.NUM_NODES - 1)

        control_utility = ControlUtility(self.servers, self.test_context)

        control_utility.activate()

        new_node = self.__start_ignite_nodes(version, 1)

        baseline = control_utility.set_baseline(self.servers.nodes + new_node.nodes)

        new_node.stop()

        self.servers.await_event("Node left topology", timeout_sec=30, from_the_beginning=True)

        baseline = control_utility.remove_from_baseline(new_node.nodes)

        new_node.start(timeout_sec=30)

        baseline = control_utility.add_to_baseline(new_node.nodes)

        return {"new_baseline": ", ".join([str(n) for n in baseline])}

    @cluster(num_nodes=NUM_NODES)
    @parametrize(version=str(DEV_BRANCH))
    @parametrize(version=str(LATEST_2_8))
    @parametrize(version=str(LATEST_2_7))
    def test_activate_deactivate(self, version):
        self.servers = self.__start_ignite_nodes(version, self.NUM_NODES)

        control_utility = ControlUtility(self.servers, self.test_context)

        control_utility.activate()

        state, _, _ = control_utility.cluster_state()

        assert state.lower() == 'active', 'Unexpected state %s' % state

        control_utility.deactivate()

        state, _, _ = control_utility.cluster_state()

        assert state.lower() == 'inactive', 'Unexpected state %s' % state

    def __start_ignite_nodes(self, version, num_nodes, timeout_sec=30):
        ignite_version = IgniteVersion(version)

        servers = IgniteService(self.test_context, num_nodes=num_nodes, version=ignite_version,
                                properties=self.properties(ignite_version))

        servers.start(timeout_sec=timeout_sec)

        return servers


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
        Print current baseline nodes.
        """
        return self.cluster_state().baseline

    def cluster_state(self):
        output = self.__run("--baseline")

        return self.__parse_cluster_state(output)

    def set_baseline(self, baseline):
        if type(baseline) == int:
            result = self.__run("--baseline version %d --yes" % baseline)
        else:
            result = self.__run("--baseline set %s --yes" % ",".join([node.account.externally_routable_ip for node in baseline]))

        return self.__parse_cluster_state(result)

    def add_to_baseline(self, nodes):
        result = self.__run("--baseline add %s --yes" % ",".join([node.account.externally_routable_ip for node in nodes]))

        return self.__parse_cluster_state(result)

    def remove_from_baseline(self, nodes):
        result = self.__run("--baseline remove %s --yes" % ",".join([node.account.externally_routable_ip for node in nodes]))

        return self.__parse_cluster_state(result)

    def activate(self):
        return self.__run("--activate --yes")

    def deactivate(self):
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

        baseline = [BaselineNode(consistent_id=match[1], state=match[3], order=int(match[5]) if match[5] else None)
                for match in baseline_pattern.findall(output)]

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


class ControlUtilityError(RemoteCommandError):
    def __init__(self, account, cmd, exit_status, output):
        super(ControlUtilityError, self).__init__(account, cmd, exit_status,"".join(output))
