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
import socket
import time
from datetime import datetime, timedelta
from typing import NamedTuple

from ducktape.cluster.remoteaccount import RemoteCommandError

from ignitetest.services.utils.ignite_persistence import IgnitePersistenceAware
from ignitetest.services.utils.jmx_utils import JmxClient


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
        result = self.__run("--baseline")

        return self.__parse_cluster_state(result)

    def set_baseline(self, baseline):
        """
        :param baseline: Baseline nodes or topology version to set as baseline.
        """
        if isinstance(baseline, int):
            result = self.__run(f"--baseline version {baseline} --yes")
        else:
            result = self.__run(
                f"--baseline set {','.join([node.account.externally_routable_ip for node in baseline])} --yes")

        return self.__parse_cluster_state(result)

    def add_to_baseline(self, nodes):
        """
        :param nodes: Nodes that should be added to baseline.
        """
        result = self.__run(
            f"--baseline add {','.join([node.account.externally_routable_ip for node in nodes])} --yes")

        return self.__parse_cluster_state(result)

    def remove_from_baseline(self, nodes):
        """
        :param nodes: Nodes that should be removed to baseline.
        """
        result = self.__run(
            f"--baseline remove {','.join([node.account.externally_routable_ip for node in nodes])} --yes")

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
        timeout_str = f"timeout {timeout}" if timeout else ""
        return self.__run(f"--baseline auto_adjust enable {timeout_str} --yes")

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

    def tx(self, **kwargs):
        """
        Get list of transactions, various filters can be applied.
        """
        output = self.__run(self.__tx_command(**kwargs))
        res = self.__parse_tx_list(output)
        return res if res else output

    def tx_info(self, xid):
        """
        Get verbose transaction info by xid.
        """
        return self.__parse_tx_info(self.__run(f"--tx --info {xid}"))

    def tx_kill(self, **kwargs):
        """
        Kill transaction by xid or by various filter.
        """
        output = self.__run(self.__tx_command(kill=True, **kwargs))
        res = self.__parse_tx_list(output)
        return res if res else output

    def validate_indexes(self, check_assert: bool = None):
        """
        Validate indexes.
        If check is true, will be return
        """
        data = self.__run("--cache validate_indexes")

        if check_assert is not None:
            assert (('no issues found.' in data) == check_assert), data

        return data

    def idle_verify(self, check_assert: bool = None):
        """
        Idle verify.
        """
        data = self.__run("--cache idle_verify")

        if check_assert is not None:
            assert (('idle_verify check has finished, no conflicts have been found.' in data) == check_assert), data

        return data

    def idle_verify_dump(self, node=None, return_path: bool = False):
        """
        Idle verify dump.
        """
        data = self.__run("--cache idle_verify --dump", node=node)

        if return_path & ('VisorIdleVerifyDumpTask successfully' in data):
            match = re.search(r'/.*.txt', data)
            return match[0]

        return data

    def snapshot_create(self, snapshot_name: str, sync_mode: bool = True, time_out: int = 60):
        """
        Create snapshot.
        """
        res = self.__run(f"--snapshot create {snapshot_name}")

        self.logger.info(res)

        if ("Command [SNAPSHOT] finished with code: 0" in res) & sync_mode:
            self.await_snapshot(snapshot_name=snapshot_name, time_out=time_out)

        return res

    def snapshot_cancel(self, snapshot_name: str):
        """
        Cancel snapshot.
        """
        return self.__run(f"--snapshot cancel {snapshot_name}")

    def snapshot_kill(self, snapshot_name: str):
        """
        Kill snapshot.
        """
        return self.__run(f"--kill SNAPSHOT {snapshot_name}")

    def await_snapshot(self, snapshot_name: str, time_out=60):
        """
        Waiting for the snapshot to complete.
        """
        delta_time = datetime.now() + timedelta(seconds=time_out)

        while datetime.now() < delta_time:
            for node in self._cluster.nodes:
                mbean = JmxClient(node).find_mbean('snapshot')
                start_time = int(list(mbean.__getattr__('LastSnapshotStartTime'))[0])
                end_time = int(list(mbean.__getattr__('LastSnapshotEndTime'))[0])
                err_msg = list(mbean.__getattr__('LastSnapshotErrorMessage'))[0]

                if (0 < start_time < end_time) & (err_msg == ''):
                    res = self._cluster.ssh_output_on_all_nodes(
                        f'du -hs {IgnitePersistenceAware.SNAPSHOT}/{snapshot_name} | ' + "awk '{print $1}'")

                    for items in res.items():
                        data = items[1].decode("utf-8")
                        self.logger.info(f'Snapshot {snapshot_name} on {items[0]}: {data}')
                    return

            time.sleep(1)

        raise TimeoutError(f'LastSnapshotStartTime={start_time}, '
                           f'LastSnapshotEndTime={end_time}, '
                           f'LastSnapshotErrorMessage={err_msg}')

    @staticmethod
    def __tx_command(**kwargs):
        tokens = ["--tx"]

        if 'xid' in kwargs:
            tokens.append(f"--xid {kwargs['xid']}")

        if kwargs.get('clients'):
            tokens.append("--clients")

        if kwargs.get('servers'):
            tokens.append("--servers")

        if 'min_duration' in kwargs:
            tokens.append(f"--min-duration {kwargs.get('min_duration')}")

        if 'min_size' in kwargs:
            tokens.append(f"--min-size {kwargs.get('min_size')}")

        if 'label_pattern' in kwargs:
            tokens.append(f"--label {kwargs['label_pattern']}")

        if kwargs.get("nodes"):
            tokens.append(f"--nodes {','.join(kwargs.get('nodes'))}")

        if 'limit' in kwargs:
            tokens.append(f"--limit {kwargs['limit']}")

        if 'order' in kwargs:
            tokens.append(f"--order {kwargs['order']}")

        if kwargs.get('kill'):
            tokens.append("--kill --yes")

        return " ".join(tokens)

    @staticmethod
    def __parse_tx_info(output):
        tx_info_pattern = re.compile(
            "Near XID version: (?P<xid_full>GridCacheVersion \\[topVer=\\d+, order=\\d+, nodeOrder=\\d+\\])\\n\\s+"
            "Near XID version \\(UUID\\): (?P<xid>[^\\s]+)\\n\\s+"
            "Isolation: (?P<isolation>[^\\s]+)\\n\\s+"
            "Concurrency: (?P<concurrency>[^\\s]+)\\n\\s+"
            "Timeout: (?P<timeout>\\d+)\\n\\s+"
            "Initiator node: (?P<initiator_id>[^\\s]+)\\n\\s+"
            "Initiator node \\(consistent ID\\): (?P<initiator_consistent_id>[^\\s+]+)\\n\\s+"
            "Label: (?P<label>[^\\s]+)\\n\\s+Topology version: AffinityTopologyVersion "
            "\\[topVer=(?P<top_ver>\\d+), minorTopVer=(?P<minor_top_ver>\\d+)\\]\\n\\s+"
            "Used caches \\(ID to name\\): {(?P<caches>.*)}\\n\\s+"
            "Used cache groups \\(ID to name\\): {(?P<cache_groups>.*)}\\n\\s+"
            "States across the cluster: \\[(?P<states>.*)\\]"
        )

        match = tx_info_pattern.search(output)

        str_fields = ['xid', 'xid_full', 'label', 'timeout', 'isolation', 'concurrency', 'initiator_id',
                      'initiator_consistent_id']
        dict_fields = ['caches', 'cache_groups']

        if match:
            kwargs = {v: match.group(v) for v in str_fields}
            kwargs['timeout'] = int(match.group('timeout'))
            kwargs.update({v: parse_dict(match.group(v)) for v in dict_fields})
            kwargs['top_ver'] = (int(match.group('top_ver')), int(match.group('minor_top_ver')))
            kwargs['states'] = parse_list(match.group('states'))

            return TxVerboseInfo(**kwargs)

        return None

    @staticmethod
    def __parse_tx_list(output):
        tx_pattern = re.compile(
            "Tx: \\[xid=(?P<xid>[^\\s]+), "
            "label=(?P<label>[^\\s]+), state=(?P<state>[^\\s]+), "
            "startTime=(?P<start_time>\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{3}), duration=(?P<duration>\\d+), "
            "isolation=(?P<isolation>[^\\s]+), concurrency=(?P<concurrency>[^\\s]+), "
            "topVer=AffinityTopologyVersion \\[topVer=(?P<top_ver>\\d+), minorTopVer=(?P<minor_top_ver>\\d+)\\], "
            "timeout=(?P<timeout>\\d+), size=(?P<size>\\d+), dhtNodes=\\[(?P<dht_nodes>.*)\\], "
            "nearXid=(?P<near_xid>[^\\s]+), parentNodeIds=\\[(?P<parent_nodes>.*)\\]\\]")

        str_fields = ['xid', 'label', 'state', 'isolation', 'concurrency', 'near_xid']
        int_fields = ['timeout', 'size', 'duration']
        list_fields = ['parent_nodes', 'dht_nodes']

        tx_list = []
        for match in tx_pattern.finditer(output):
            kwargs = {v: match.group(v) for v in str_fields}
            kwargs.update({v: int(match.group(v)) for v in int_fields})
            kwargs['top_ver'] = (int(match.group('top_ver')), int(match.group('minor_top_ver')))
            kwargs.update({v: parse_list(match.group(v)) for v in list_fields})
            kwargs['start_time'] = time.strptime(match.group('start_time'), "%Y-%m-%d %H:%M:%S.%f")
            tx_list.append(TxInfo(**kwargs))

        return tx_list

    @staticmethod
    def __parse_cluster_state(output):
        state_pattern = re.compile("Cluster state: (?P<cluster_state>[^\\s]+)")
        topology_pattern = re.compile("Current topology version: (?P<topology_version>\\d+)")
        baseline_pattern = re.compile("Consistent(Id|ID)=(?P<consistent_id>[^\\s]+),\\sS(tate|TATE)=(?P<state>[^\\s]+),"
                                      "?(\\sOrder=(?P<order>\\d+))?")

        match = state_pattern.search(output)
        state = match.group("cluster_state") if match else None

        match = topology_pattern.search(output)
        topology = int(match.group("topology_version")) if match else None

        baseline = []
        for match in baseline_pattern.finditer(output):
            node = BaselineNode(consistent_id=match.group("consistent_id"), state=match.group("state"),
                                order=int(match.group("order")) if match.group("order") else None)
            baseline.append(node)

        return ClusterState(state=state, topology_version=topology, baseline=baseline)

    def __run(self, cmd, node=None):
        if node is None:
            node = random.choice(self.__alives())

        self.logger.debug(f"Run command {cmd} on node {node.name}")

        node_ip = socket.gethostbyname(node.account.hostname)

        raw_output = node.account.ssh_capture(self.__form_cmd(node_ip, cmd), allow_fail=True)
        code, output = self.__parse_output(raw_output)

        self.logger.debug(f"Output of command {cmd} on node {node.name}, exited with code {code}, is {output}")

        if code != 0:
            raise ControlUtilityError(node.account, cmd, code, output)

        return output

    def __form_cmd(self, node_ip, cmd):
        return self._cluster.spec.path.script(f"{self.BASE_COMMAND} --host {node_ip} {cmd}")

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


class BaselineNode(NamedTuple):
    """
    Baseline node info.
    """
    consistent_id: str
    state: str
    order: int


class ClusterState(NamedTuple):
    """
    Cluster state info.
    """
    state: str
    topology_version: int
    baseline: list


class TxInfo(NamedTuple):
    """
    Transaction info.
    """
    xid: str
    near_xid: str
    label: str
    state: str
    start_time: time.struct_time
    duration: int
    isolation: str
    concurrency: str
    top_ver: tuple
    timeout: int
    size: int
    dht_nodes: list = []
    parent_nodes: list = []


class TxVerboseInfo(NamedTuple):
    """
    Transaction info returned with --info
    """
    xid: str
    xid_full: str
    label: str
    isolation: str
    concurrency: str
    timeout: int
    top_ver: tuple
    initiator_id: str
    initiator_consistent_id: str
    caches: dict
    cache_groups: dict
    states: list


class ControlUtilityError(RemoteCommandError):
    """
    Error is raised when control utility failed.
    """
    def __init__(self, account, cmd, exit_status, output):
        super().__init__(account, cmd, exit_status, "".join(output))


def parse_dict(raw):
    """
    Parse java Map.toString() to python dict.
    """
    res = {}
    for token in raw.split(','):
        key, value = tuple(token.strip().split('='))
        res[key] = value

    return res


def parse_list(raw):
    """
    Parse java List.toString() to python list
    """
    return [token.strip() for token in raw.split(',')]
