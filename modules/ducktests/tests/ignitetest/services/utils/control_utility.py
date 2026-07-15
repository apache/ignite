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

from ignitetest.services.utils.auth import get_credentials, is_auth_enabled
from ignitetest.services.utils.ignite_spec import envs_to_exports
from ignitetest.services.utils.ssl.ssl_params import get_ssl_params, is_ssl_enabled, IGNITE_ADMIN_ALIAS
from ignitetest.services.utils.jmx_utils import JmxClient
from ignitetest.utils.version import V_2_11_0


class ControlUtility:
    """
    Control utility (control.sh) wrapper.
    """
    BASE_COMMAND = "control.sh"

    # Column separator used by control.sh --system-view (SystemViewCommand.COLUMN_SEPARATOR).
    SYSTEM_VIEW_COLUMN_SEPARATOR = "    "

    def __init__(self, cluster, ssl_params=None, username=None, password=None):
        self._cluster = cluster
        self.logger = cluster.context.logger

        if ssl_params:
            self.ssl_params = ssl_params
        elif is_ssl_enabled(cluster.context.globals):
            self.ssl_params = get_ssl_params(cluster.context.globals, cluster.shared_root, IGNITE_ADMIN_ALIAS)

        if username and password:
            self.username, self.password = username, password
        elif is_auth_enabled(cluster.context.globals):
            self.username, self.password = get_credentials(cluster.context.globals)

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

    def validate_indexes(self):
        """
        Validate indexes.
        """
        data = self.__run("--cache validate_indexes")

        assert ('no issues found.' in data), data

    def idle_verify(self, cache_names=None):
        """
        Idle verify.
        """

        if cache_names is None:
            data = self.__run("--cache idle_verify")
        else:
            data = self.__run(f"--cache idle_verify {cache_names}")

        if self._cluster.config.version < V_2_11_0:
            msg = 'idle_verify check has finished, no conflicts have been found.'
        else:
            msg = 'The check procedure has finished, no conflicts have been found.'

        assert (msg in data), data
        return data

    def idle_verify_dump(self, node=None):
        """
        Idle verify dump.
        :param node: Node on which the command will be executed and the dump file will be located.
        """
        data = self.__run("--cache idle_verify --dump", node=node)

        assert ('IdleVerifyDumpTask successfully' in data), data

        return re.search(r'/.*.txt', data).group(0)

    def cache_distribution(self, node_id=None, cache_names=None, user_attributes=None):
        """
        Prints partition distribution.

        :param node_id: Node id to get distribution for, all nodes if None.
        :param cache_names: Cache name, or list of cache names, all caches if None.
        :param user_attributes: Node attribute name or list of names to add to the output
                                (e.g. "IGNITE_DATA_CENTER_ID").
        :return: CacheDistribution.
        """
        if isinstance(cache_names, str):
            cache_names = [cache_names]

        if isinstance(user_attributes, str):
            user_attributes = [user_attributes]

        cmd = f"--cache distribution {node_id if node_id else 'null'}"

        if cache_names:
            cmd += f" {','.join(cache_names)}"

        if user_attributes:
            cmd += f" --user-attributes {','.join(user_attributes)}"

        result = self.__run(cmd)

        return self.__parse_cache_distribution(result, user_attributes)

    @staticmethod
    def __parse_cache_distribution(output, user_attributes=None):
        group_pattern = re.compile(r"\[next group: id=(?P<group_id>-?\d+), name=(?P<name>[^\]]+)\]")

        # Trailing attribute values appear after nodeAddresses, comma-separated,
        # in the same order they were passed to --user-attributes.
        row_pattern = re.compile(r"(?P<group_id>-?\d+),"
                                 r"(?P<partition>\d+),"
                                 r"(?P<node_id>[0-9a-fA-F]+),"
                                 r"(?P<primary>[PB]),"
                                 r"(?P<state>[A-Z_]+),"
                                 r"(?P<update_counter>\d+),"
                                 r"(?P<partition_size>\d+),"
                                 r"\[(?P<node_addresses>[^\]]*)\]"
                                 r"(?:,(?P<attr_values>.*))?$")

        groups = {}
        cur_group = None

        for line in output.splitlines():
            line = line.strip()

            match = group_pattern.search(line)
            if match:
                cur_group = CacheGroupDistribution(group_id=int(match.group("group_id")),
                                                   name=match.group("name"),
                                                   partitions={})
                groups[cur_group.name] = cur_group
                continue

            match = row_pattern.match(line)
            if match and cur_group is not None:
                attrs = {}
                if user_attributes and match.group("attr_values") is not None:
                    values = [v.strip() for v in match.group("attr_values").split(",")]
                    attrs = dict(zip(user_attributes, values))

                copy = PartitionCopy(node_id=match.group("node_id"),
                                     primary=match.group("primary") == "P",
                                     state=match.group("state"),
                                     update_counter=int(match.group("update_counter")),
                                     partition_size=int(match.group("partition_size")),
                                     node_addresses=[a.strip() for a in match.group("node_addresses").split(",") if a],
                                     user_attributes=attrs)

                cur_group.partitions.setdefault(int(match.group("partition")), []).append(copy)

        return CacheDistribution(groups=groups)

    def system_view(self, name, node=None, node_id=None, node_ids=None, all_nodes=False):
        """
        Prints the content of a system view (``control.sh --system-view``).

        :param name: System view name. Both the SQL ("CACHES") and Java ("caches") styles are accepted.
        :param node: Node to run control.sh on (a random alive node if None).
        :param node_id: Single node id to read the view from (discouraged upstream, prefer
                        ``node_ids`` or ``all_nodes``).
        :param node_ids: List of node ids to read the view from.
        :param all_nodes: Read the view from all nodes.
        :return: Dict mapping node id (str) to a list of rows; each row is a dict keyed by the
                 view column name (e.g. ``row["CACHE_NAME"]``).
        """
        cmd = f"--system-view {name}"

        if all_nodes:
            cmd += " --all-nodes"
        elif node_ids:
            cmd += f" --node-ids {','.join(node_ids)}"
        elif node_id:
            cmd += f" --node-id {node_id}"

        return self.__parse_system_view(self.__run(cmd, node=node), name)

    def caches(self, cache_name=None, node=None, node_id=None, node_ids=None, all_nodes=False):
        """
        Reads the CACHES system view and returns its rows.

        Each row is a dict keyed by the view column names, e.g. ``CACHE_NAME``, ``CACHE_ID``,
        ``CACHE_GROUP_ID``, ``CACHE_GROUP_NAME``, ``CACHE_TYPE``, ``CACHE_MODE``,
        ``ATOMICITY_MODE``, ``BACKUPS``.

        :param cache_name: If set, only rows whose ``CACHE_NAME`` equals it are returned.
        :return: List of rows (dicts). CACHES is cluster-wide, so by default the rows of a
                 single node are returned to avoid duplicates; when ``node_ids`` or
                 ``all_nodes`` is requested the rows of every queried node are concatenated.
        """
        by_node = self.system_view("CACHES", node=node, node_id=node_id, node_ids=node_ids, all_nodes=all_nodes)

        if node_ids or all_nodes:
            rows = [row for node_rows in by_node.values() for row in node_rows]
        else:
            rows = next(iter(by_node.values()), [])

        if cache_name is not None:
            rows = [row for row in rows if row.get("CACHE_NAME") == cache_name]

        return rows

    @staticmethod
    def __parse_system_view(output, name):
        """
        Parses ``control.sh --system-view`` output.

        The command prints one block per node::

            Results from node with ID: <uuid>
            ---
            COL_A    COL_B    ...
            valA     valB     ...
            ---

        Columns are padded to their widest value and joined by the 4-space column separator;
        cells never hold an empty value (nulls print as "null"), so splitting on the separator
        and dropping the padding gaps recovers the columns - the same approach the upstream
        SystemViewCommandTest uses.
        """
        if "No system view with specified name was found" in output:
            raise AssertionError(f"No system view named '{name}' was found:\n{output}")

        node_pattern = re.compile(r"Results from node with ID: (?P<node_id>\S+)")
        sep = ControlUtility.SYSTEM_VIEW_COLUMN_SEPARATOR

        result = {}
        node_id = None
        header = None
        # idle -> await_open (saw the node header) -> body (between the two '---' delimiters).
        state = "idle"

        for line in output.splitlines():
            stripped = line.strip()

            match = node_pattern.match(stripped)
            if match:
                node_id = match.group("node_id")
                result[node_id] = []
                header = None
                state = "await_open"
                continue

            if state == "await_open":
                if stripped == "---":
                    state = "body"
                continue

            if state == "body":
                if stripped == "---":
                    state = "idle"
                    continue

                if not stripped:
                    continue

                cells = [cell.strip() for cell in line.split(sep) if cell.strip()]

                if header is None:
                    header = cells
                else:
                    result[node_id].append(dict(zip(header, cells)))

        return result

    def check_consistency(self, args):
        """
        Consistency check.
        """
        data = self.__run(f"--consistency {args} --enable-experimental")

        assert ('Command [CONSISTENCY] finished with code: 0' in data), data
        return data

    def snapshot_create(self, snapshot_name: str, timeout_sec: int = 60):
        """
        Create snapshot.
        :param snapshot_name: Name of Snapshot.
        :param timeout_sec: Timeout to await snapshot to complete.
        """
        res = self.__run(f"--snapshot create {snapshot_name}")

        assert "Command [SNAPSHOT] finished with code: 0" in res

        delta_time = datetime.now() + timedelta(seconds=timeout_sec)

        while datetime.now() < delta_time:
            for node in self._cluster.nodes:
                mbean = JmxClient(node).find_mbean('.*name=snapshot.*', negative_pattern='group=views')

                if snapshot_name != next(mbean.LastSnapshotName, ""):
                    continue

                start_time = int(next(mbean.LastSnapshotStartTime))
                end_time = int(next(mbean.LastSnapshotEndTime))
                err_msg = next(mbean.LastSnapshotErrorMessage)

                if (start_time < end_time) and (err_msg == ''):
                    assert snapshot_name == next(mbean.LastSnapshotName)
                    return

        raise TimeoutError(f'Failed to wait for the snapshot operation to complete: '
                           f'snapshot_name={snapshot_name} in {timeout_sec} seconds.')

    def snapshot_check(self, snapshot_name: str):
        """
        Check snapshot.
        :param snapshot_name: Name of snapshot.
        """
        res = self.__run(f"--snapshot check {snapshot_name}")

        assert "The check procedure has finished, no conflicts have been found." in res

        return res

    def start_performance_statistics(self):
        """
        Start performance statistics collecting in the cluster.
        """
        output = self.__performance_statistics_cmd("start")

        assert "Started." in output

        return output

    def stop_performance_statistics(self):
        """
        Stop performance statistics collecting in the cluster.
        """
        output = self.__performance_statistics_cmd("stop")

        assert "Stopped." in output

        return output

    def rotate_performance_statistics(self):
        """
        Rotate performance statistics collecting in the cluster.
        """
        output = self.__performance_statistics_cmd("rotate")

        assert "Rotated." in output

        return output

    def is_performance_statistics_enabled(self):
        """
        Check status of performance statistics collecting in the cluster.
        """
        output = self.__performance_statistics_cmd("status")

        assert "Enabled." in output or "Disabled." in output

        return "Enabled." in output

    def run(self, cmd, node=None):
        """
        Run arbitrary control.sh subcommand.
        :param cmd: Command line parameters for the control.sh.
        :param node: Node to run the control.sh on.
        :return: Output of the commands as a string.
        """
        return self.__run(cmd, node)

    def __performance_statistics_cmd(self, sub_command):
        return self.__run(f"--performance-statistics {sub_command}")

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
            "Near XID version: "
            "(?P<xid_full>GridCacheVersion \\[topVer=\\d+, order=\\d+, nodeOrder=\\d+(, dataCenterId=\\d+)?\\])\\n\\s+"
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
            "startTime=(?P<start_time>\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{3}), "
            "duration=(?P<duration>\\d+)( sec)?, "
            "isolation=(?P<isolation>[^\\s]+), concurrency=(?P<concurrency>[^\\s]+), "
            "topVer=AffinityTopologyVersion \\[topVer=(?P<top_ver>\\d+), minorTopVer=(?P<minor_top_ver>\\d+)\\], "
            "timeout=(?P<timeout>\\d+)( sec)?, size=(?P<size>\\d+), dhtNodes=\\[(?P<dht_nodes>.*)\\], "
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
        baseline_pattern = re.compile("Consistent(Id|ID)=(?P<consistent_id>[^\\s,]+)"
                                      "(,\\sA(ddress|DDRESS)=(?P<address>[^\\s,]+))?"
                                      ",\\sS(tate|TATE)=(?P<state>[^\\s,]+)"
                                      "(,\\sOrder=(?P<order>\\d+))?")

        coordinator_pattern = re.compile("\\(Coordinator: [^)]*Order=(?P<order>\\d+)\\)")

        match = state_pattern.search(output)
        state = match.group("cluster_state") if match else None

        match = topology_pattern.search(output)
        topology = int(match.group("topology_version")) if match else None

        baseline = []
        for match in baseline_pattern.finditer(output):
            node = BaselineNode(consistent_id=match.group("consistent_id"),
                                state=match.group("state"),
                                address=match.group("address"),
                                order=int(match.group("order")) if match.group("order") else None)
            baseline.append(node)

        coordinator = None

        match = coordinator_pattern.search(output)

        if match:
            order = int(match.group("order"))

            coordinator = next((node for node in baseline if node.order == order), None)

            if coordinator is None:
                raise AssertionError(f"Coordinator with order={order} is not found in baseline [baseline={baseline}]")

        return ClusterState(state=state, topology_version=topology, baseline=baseline, coordinator=coordinator)

    def __run(self, cmd, node=None):
        if node is None:
            node = random.choice(self._cluster.alive_nodes)

        self.logger.debug(f"Run command {cmd} on node {node.name}")

        node_ip = socket.gethostbyname(node.account.hostname)

        raw_output = node.account.ssh_capture(self.__form_cmd(node_ip, cmd), allow_fail=True)
        code, output = self.__parse_output(raw_output)

        self.logger.debug(f"Output of command {cmd} on node {node.name}, exited with code {code}, is {output}")

        if code != 0:
            raise ControlUtilityError(node.account, cmd, code, output)

        return output

    def __form_cmd(self, node_ip, cmd):
        ssl = ""
        if hasattr(self, "ssl_params"):
            ssl = f" --keystore {self.ssl_params.key_store_path} " \
                  f"--keystore-password {self.ssl_params.key_store_password} " \
                  f"--truststore {self.ssl_params.trust_store_path} " \
                  f"--truststore-password {self.ssl_params.trust_store_password}"
        auth = ""
        if hasattr(self, "username"):
            auth = f" --user {self.username} --password {self.password} "

        return "%s %s" % \
               (envs_to_exports(self.envs()),
                self._cluster.script(f"{self.BASE_COMMAND} --host {node_ip} {cmd} {ssl} {auth}"))

    def envs(self):
        """
        :return: environment set.
        """
        return {
            'EXCLUDE_TEST_CLASSES': 'true',
            'CONTROL_JVM_OPTS': '-Dlog4j.configurationFile=file:' + self._cluster.log_config_file
        }

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
    address: str
    order: int


class ClusterState(NamedTuple):
    """
    Cluster state info.
    """
    state: str
    topology_version: int
    baseline: list
    coordinator: BaselineNode = None


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


class PartitionCopy(NamedTuple):
    """
    Single copy (primary or backup) of a partition on a node.
    """
    node_id: str
    primary: bool
    state: str
    update_counter: int
    partition_size: int
    node_addresses: list
    user_attributes: dict


class CacheGroupDistribution(NamedTuple):
    """
    Distribution of a single cache group: partition id -> list of PartitionCopy.
    """
    group_id: int
    name: str
    partitions: dict


class CacheDistribution(NamedTuple):
    """
    Distribution info for all printed cache groups: group name -> CacheGroupDistribution.
    """
    groups: dict


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
