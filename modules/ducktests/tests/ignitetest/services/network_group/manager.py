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
import re
import socket
import struct
import sys
from concurrent.futures import ThreadPoolExecutor
from itertools import permutations
from time import monotonic
from typing import Dict, Iterator, List, Tuple

from ducktape.services.service import Service

from ignitetest.services.network_group.configuration import NetworkGroupStore, CrossNetworkGroupConfiguration
from ignitetest.services.network_group.tc_rule_args import (
    ACTION_ADD, ACTION_OVERWRITE,
    to_tcset_cmd, to_tcdel_all_cmd,
    partition_chain_name, to_partition_enable_cmd, to_partition_disable_cmd, to_partition_teardown_cmd,
    PARTITION_CHAIN_PREFIX
)
from ignitetest.services.utils.decorators import memoize


# Get the default network interface (e.g., eth0, ens3)
CMD_GET_NETWORK_INTERFACE = "ip route | grep default | awk -- '{printf $5}'"

# Pseudo-action used only on initial deployment: the first rule per node
# overwrites any stale state, subsequent rules are appended.
ACTION_DEPLOY = "deploy"

# Upper bound on concurrent SSH sessions used to apply tc rules cluster-wide.
MAX_PARALLEL_SSH_SESSIONS = 16

# A rule spec: (src_group, dst_group, action, config).
RuleSpec = Tuple[str, str, str, CrossNetworkGroupConfiguration]


class NetworkGroupManager:
    """
    Deploys and tears down traffic-control rules between logical node groups,
    and toggles full network partitions between them at test time.

    Baseline impairments (delay/loss/rate) are deployed once via tcset.
    Partitions are layered on top as per-pair iptables DROP chains. The netem
    rules are never touched by a partition, so healing is a pure chain flush
    that automatically restores the originally deployed impairments.

    All commands targeting a single node are batched into one SSH invocation,
    and nodes are configured in parallel, so that a partition (or its removal)
    takes effect near-atomically across the cluster instead of rolling out
    node by node.
    """
    def __init__(self, logger, network_group_store: NetworkGroupStore,
                 network_group_registry: Dict[str, List[Service]]):
        self.logger = logger

        self.network_group_store = network_group_store
        self.network_group_registry = network_group_registry

    def __enter__(self):
        self.deploy()

        self._log_network("ON_DEPLOY")

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.destroy()

        self._log_network("ON_EXIT")

    def deploy(self):
        """
        Compiles routing maps and deploys cross-group network constraints.
        """
        self._prefetch_network_interfaces()

        self.destroy()

        specs = []

        for src_group, dst_group in permutations(self.network_group_registry.keys(), 2):
            cfg = self.network_group_store.get_config(src_group, dst_group)

            if cfg is None:
                # No impairment configured between these groups: traffic flows unconstrained.
                self.logger.debug(f"No configuration for {src_group} -> {dst_group}, skipping.")
                continue

            specs.append((src_group, dst_group, ACTION_DEPLOY, cfg))

        self._apply_specs(specs, tag="DEPLOY")

    def destroy(self):
        """
        Restores network interfaces back to their un-throttled state and
        removes any leftover partition chains from iptables.
        """
        tasks = []

        for node in self._iter_all_nodes():
            interface = self._get_default_network_interface(node)

            tasks.append((node, f"{to_tcdel_all_cmd(interface)} && {to_partition_teardown_cmd()}"))

        self._ssh_parallel(tasks, tag="DESTROY")

    def enable_network_partition(self, group_a: str, group_b: str):
        """
        Creates a complete, bidirectional network partition between two groups
        by installing iptables DROP rules for all cross-group traffic,
        simulating a split-brain. The netem impairments deployed via tcset are
        left untouched underneath.
        """
        self.logger.info(f"Enabling network partition between [{group_a}] <---> [{group_b}]")

        chain = partition_chain_name(group_a, group_b)

        tasks = []

        for src_group, dst_group in self._bidirectional(group_a, group_b):
            remote_ips = self._resolve_group_ips(dst_group)

            cmd = to_partition_enable_cmd(chain, remote_ips)

            for node in self._iter_group_nodes(src_group):
                tasks.append((node, cmd))

        self._ssh_parallel(tasks, tag="PARTITION_ON")

        self._log_network(f"PARTITION {group_a} <-> {group_b}")

    def disable_network_partition(self, group_a: str, group_b: str):
        """
        Heals an active network partition between two groups by flushing the
        pair's iptables DROP chain on every node - a single native call per
        node. The originally deployed tcset impairments were never modified,
        so they are back in effect immediately without any re-application.
        """
        self.logger.info(f"Disabling network partition between [{group_a}] <---> [{group_b}]")

        cmd = to_partition_disable_cmd(partition_chain_name(group_a, group_b))

        tasks = [(node, cmd)
                 for group in (group_a, group_b)
                 for node in self._iter_group_nodes(group)]

        self._ssh_parallel(tasks, tag="PARTITION_OFF")

        self._log_network(f"NET RESTORED {group_a} <-> {group_b}")

    def _resolve_group_ips(self, group: str) -> List[str]:
        return [socket.gethostbyname(node.account.externally_routable_ip)
                for node in self._iter_group_nodes(group)]

    def _apply_specs(self, specs: List[RuleSpec], tag: str):
        """
        Compiles all rule specs into one batched command per source node and
        executes them across nodes in parallel.
        """
        node_cmds = self._collect_node_cmds(specs)

        # One SSH round-trip per node: all rules for a node take effect together.
        tasks = [(node, " && ".join(cmds)) for node, cmds in node_cmds]

        self._ssh_parallel(tasks, tag=tag)

    def _collect_node_cmds(self, specs: List[RuleSpec]):
        """
        Groups the tc commands produced by all specs by source node.

        :return: List of (node, [command, ...]) with insertion order preserved,
        so that on deployment the first rule per node is an '--overwrite' and
        every subsequent rule (across all destination groups) is an '--add'.
        """
        per_node = {}

        for src_group, dst_group, action, cfg in specs:
            dst_ips = [node.account.externally_routable_ip for node in self._iter_group_nodes(dst_group)]

            for src_node in self._iter_group_nodes(src_group):
                interface = self._get_default_network_interface(src_node)

                _, cmds = per_node.setdefault(id(src_node), (src_node, []))

                for dst_ip in dst_ips:
                    if action == ACTION_DEPLOY:
                        current_action = ACTION_OVERWRITE if not cmds else ACTION_ADD
                    else:
                        current_action = action

                    cmd = to_tcset_cmd(interface=interface, dst_host_or_ip=dst_ip, config=cfg,
                                       action=current_action)

                    if cmd:
                        cmds.append(cmd)
                    elif action == ACTION_DEPLOY:
                        raise ValueError(
                            f"No network constraints defined from {src_node.account.hostname} to {dst_ip}."
                        )

        return [entry for entry in per_node.values() if entry[1]]

    def _ssh_parallel(self, tasks: List[Tuple[object, str]], tag: str):
        """
        Executes one command per node concurrently and propagates the first failure.
        """
        if not tasks:
            return

        started = monotonic()

        def run(task):
            node, cmd = task
            node.account.ssh(cmd)

        with ThreadPoolExecutor(max_workers=min(MAX_PARALLEL_SSH_SESSIONS, len(tasks))) as pool:
            futures = [pool.submit(run, task) for task in tasks]

            for future in futures:
                future.result()

        self.logger.debug(f"[{tag}] tc rules applied on {len(tasks)} node(s) in {monotonic() - started:.2f}s")

    def _prefetch_network_interfaces(self):
        """
        Warms up the per-node network interface cache in parallel, so that
        command compilation later on requires no sequential SSH round-trips.
        """
        nodes = list(self._iter_all_nodes())

        if not nodes:
            return

        with ThreadPoolExecutor(max_workers=min(MAX_PARALLEL_SSH_SESSIONS, len(nodes))) as pool:
            list(pool.map(self._get_default_network_interface, nodes))

    @staticmethod
    def _bidirectional(group_a: str, group_b: str) -> List[Tuple[str, str]]:
        return [(group_a, group_b), (group_b, group_a)]

    def _iter_group_nodes(self, group: str) -> Iterator:
        for svc in self.network_group_registry[group]:
            yield from svc.nodes

    def _iter_all_nodes(self) -> Iterator:
        for group in self.network_group_registry:
            yield from self._iter_group_nodes(group)

    def _log_network(self, log_tag: str):
        """
        Logs a concise, structured overview of the active traffic control
        queuing disciplines (qdiscs) and routing filters across all cluster nodes.
        """
        self.logger.debug(f"Network State Overview: [START][{log_tag}]")

        node_to_status_map = {}

        for group, services in self.network_group_registry.items():
            for svc in services:
                for node in svc.nodes:
                    qdisc_output = self._exec_tc_show_command(node, "qdisc")
                    filter_output = self._exec_tc_show_command(node, "filter")

                    dst_ips = self._parse_filter_destinations(filter_output)
                    constraints = self._parse_qdisc_constraints(qdisc_output)

                    targets_str = f" -> to [{', '.join(dst_ips)}]" if dst_ips and constraints != "noqueue" else ""
                    node_ip = socket.gethostbyname(node.account.externally_routable_ip)

                    iptables_lines = self._get_ssh_output(
                        node, "sudo iptables -S 2>/dev/null || true").splitlines()
                    partition_str = self._format_partition_drops(
                        self._parse_partition_drops(iptables_lines))

                    node_status = f"[{group:<4}] {svc.who_am_i(node):<45}[{node_ip}] : " \
                                  f"{constraints}{targets_str}{partition_str}"

                    node_to_status_map.update({id(node): node_status})

        for node_id, node_status in node_to_status_map.items():
            self.logger.debug(node_status)

        self.logger.debug(f"Network State Overview: [END][{log_tag}]")

    def _exec_tc_show_command(self, node, sub_system: str) -> List[str]:
        """
        Executes a 'tc show' shell query command on a remote node over SSH
        and decodes the terminal response cleanly into strings.
        """
        interface = self._get_default_network_interface(node)

        cmd = f"sudo tc {sub_system} show dev {interface}"

        raw_bytes = node.account.ssh_output(cmd, allow_fail=False)

        return raw_bytes.decode(sys.getdefaultencoding()).splitlines()

    @staticmethod
    def _parse_filter_destinations(filter_lines: List[str]) -> List[str]:
        """
        Parses raw 'tc filter' output lines to extract destination IPs.
        Converts the internal u32 hexadecimal match filters back to human-readable strings.
        """
        dst_ips = []

        for line in filter_lines:
            match = re.search(r"match\s+([0-9a-fA-F]{8})/ffffffff\s+at\s+16", line)
            if match:
                hex_ip = match.group(1)
                try:
                    ip_bytes = struct.pack("!I", int(hex_ip, 16))
                    dst_ips.append(socket.inet_ntoa(ip_bytes))
                except (ValueError, struct.error, OSError):
                    continue

        return dst_ips

    @staticmethod
    def _parse_partition_drops(iptables_lines: List[str]) -> Dict[str, Dict[str, set]]:
        """
        Parses 'iptables -S' output lines into per-partition-chain drop sets.

        :return: {chain_name: {'s': {inbound-dropped ips}, 'd': {outbound-dropped ips}}}
        """
        pattern = re.compile(
            rf"^-A\s+({re.escape(PARTITION_CHAIN_PREFIX)}\S+)\s+"
            rf"-(s|d)\s+(\d{{1,3}}(?:\.\d{{1,3}}){{3}})(?:/32)?\s+-j\s+DROP$"
        )

        drops: Dict[str, Dict[str, set]] = {}

        for line in iptables_lines:
            match = pattern.match(line.strip())

            if match:
                chain, direction, ip = match.groups()

                drops.setdefault(chain, {"s": set(), "d": set()})[direction].add(ip)

        return drops

    @staticmethod
    def _format_partition_drops(drops: Dict[str, Dict[str, set]]) -> str:
        """
        Renders parsed partition drops into a compact, human-readable suffix.

        Fully cut peers (both inbound and outbound DROP present) are shown as
        '<-X->'. Peers with only a one-way drop are flagged explicitly as
        'in-X'/'out-X' — on a healthy partition these lists are empty, so any
        occurrence pinpoints a node with partially applied rules.
        """
        if not drops:
            return ""

        def fmt(ips: set) -> str:
            return ", ".join(sorted(ips, key=lambda ip: tuple(map(int, ip.split(".")))))

        chain_summaries = []

        for chain in sorted(drops):
            inbound, outbound = drops[chain]["s"], drops[chain]["d"]

            both, in_only, out_only = inbound & outbound, inbound - outbound, outbound - inbound

            details = []
            if both:
                details.append(f"<-X-> [{fmt(both)}]")
            if in_only:
                details.append(f"in-X only [{fmt(in_only)}]")
            if out_only:
                details.append(f"out-X only [{fmt(out_only)}]")

            chain_summaries.append(f"{chain} {' '.join(details)}")

        return " | partition: " + "; ".join(chain_summaries)

    @staticmethod
    def _parse_qdisc_constraints(qdisc_lines: List[str]) -> str:
        """
        Parses raw 'tc qdisc' output lines to identify active traffic impairments.
        Extracts active netem delay and loss parameters, ignoring verbose system handles.
        """
        for line in qdisc_lines:
            if "qdisc netem" in line:
                delay_match = re.search(r"delay\s+(\d+\w+)", line)
                loss_match = re.search(r"loss\s+(\d+%)", line)

                params = []
                if delay_match:
                    params.append(f"delay: {delay_match.group(1)}")
                if loss_match:
                    params.append(f"loss: {loss_match.group(1)}")

                if params:
                    return f"netem({', '.join(params)})"

        return "noqueue"

    @memoize
    def _get_default_network_interface(self, node):
        return self._get_ssh_output(node, CMD_GET_NETWORK_INTERFACE)

    @staticmethod
    def _get_ssh_output(node, cmd):
        return node.account.ssh_output(cmd) \
            .decode(sys.getdefaultencoding()) \
            .strip()
