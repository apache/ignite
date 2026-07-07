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
from itertools import permutations
from typing import Dict, Iterator, List, Optional, Tuple

from ducktape.services.service import Service

from ignitetest.services.network_group.configuration import NetworkGroupStore, CrossNetworkGroupConfiguration
from ignitetest.services.network_group.tc_rule_args import (
    ACTION_ADD, ACTION_CHANGE, ACTION_OVERWRITE,
    to_tcset_cmd, to_tcdel_all_cmd, to_tcdel_ip_cmd
)
from ignitetest.services.utils.decorators import memoize


# Get the default network interface (e.g., eth0, ens3)
CMD_GET_NETWORK_INTERFACE = "ip route | grep default | awk -- '{printf $5}'"

# Pseudo-action used only on initial deployment: the first rule per node
# overwrites any stale state, subsequent rules are appended.
ACTION_DEPLOY = "deploy"

# Complete bidirectional traffic drop, simulating a split-brain.
PARTITION_CFG = CrossNetworkGroupConfiguration(loss=1.0)


class NetworkGroupManager:
    """
    Deploys and tears down traffic-control rules between logical node groups,
    and toggles full network partitions between them at test time.
    """
    def __init__(self, logger, network_group_store: NetworkGroupStore, network_group_registry: Dict[str, List[Service]]):
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
        self.destroy()

        all_pairs = list(permutations(self.network_group_registry.keys(), 2))

        self._configure_cross_group_traffic(group_pairs=all_pairs, action=ACTION_DEPLOY)

    def destroy(self):
        """Restores network interfaces back to their un-throttled state."""
        for node in self._iter_all_nodes():
            interface = self._get_default_network_interface(node)

            node.account.ssh(to_tcdel_all_cmd(interface))

    def enable_network_partition(self, group_a: str, group_b: str):
        """
        Creates a complete, bidirectional network partition between two groups.
        All cross-group traffic is dropped (100% loss), simulating a split-brain.
        """
        self.logger.info(f"Enabling network partition between [{group_a}] <---> [{group_b}]")

        for src_group, dst_group in self._bidirectional(group_a, group_b):
            # tcset rejects '--add' for a destination network that already has a rule,
            # so modify the existing rule in-place when a default impairment is deployed.
            has_existing_rule = self.network_group_store.get_config(src_group, dst_group) is not None

            action = ACTION_CHANGE if has_existing_rule else ACTION_ADD

            self._configure_cross_group_traffic(group_pairs=[(src_group, dst_group)],
                                                action=action, override_cfg=PARTITION_CFG)

        self._log_network(f"PARTITION {group_a} <-> {group_b}")

    def disable_network_partition(self, group_a: str, group_b: str):
        """
        Removes an active network partition between two groups by restoring
        the originally stored rules in-place, or clearing the path entirely
        if no default impairment was configured.
        """
        self.logger.info(f"Disabling network partition between [{group_a}] <---> [{group_b}]")

        self._configure_cross_group_traffic(group_pairs=self._bidirectional(group_a, group_b),
                                            action=ACTION_CHANGE)

        self._log_network(f"NET RESTORED {group_a} <-> {group_b}")

    def _configure_cross_group_traffic(self, group_pairs: List[Tuple[str, str]], action: str,
                                       override_cfg: Optional[CrossNetworkGroupConfiguration] = None):
        """
        Applies tcset/tcdel rules from every node of each source group towards
        every node of the corresponding destination group.
        """
        for src_group, dst_group in group_pairs:
            cfg = override_cfg or self.network_group_store.get_config(src_group, dst_group)

            if cfg is None:
                if action == ACTION_DEPLOY:
                    # No impairment configured between these groups: traffic flows unconstrained.
                    self.logger.debug(f"No configuration for {src_group} -> {dst_group}, skipping.")
                    continue

                if action != ACTION_CHANGE:
                    raise Exception(f"Group configuration not found for {src_group}-{dst_group}.")

            dst_ips = [dst_node.account.externally_routable_ip for dst_node in self._iter_group_nodes(dst_group)]

            for src_node in self._iter_group_nodes(src_group):
                self._apply_node_rules(src_node, dst_ips, cfg, action)

    def _apply_node_rules(self, src_node, dst_ips: List[str],
                          cfg: Optional[CrossNetworkGroupConfiguration], action: str):
        """
        Applies the given configuration from a single source node to all destination IPs.
        """
        interface = self._get_default_network_interface(src_node)

        for idx, dst_ip in enumerate(dst_ips):
            if cfg is None:
                # Healing cycle with no originally stored impairment: clear the path.
                src_node.account.ssh(to_tcdel_ip_cmd(interface, dst_ip))
                continue

            if action == ACTION_DEPLOY:
                current_action = ACTION_OVERWRITE if idx == 0 else ACTION_ADD
            else:
                current_action = action

            cmd = to_tcset_cmd(interface=interface, dst_host_or_ip=dst_ip, config=cfg, action=current_action)

            if cmd:
                src_node.account.ssh(cmd)
            elif action == ACTION_DEPLOY:
                raise ValueError(f"No network constraints defined from {src_node.account.hostname} to {dst_ip}.")

    def _bidirectional(self, group_a: str, group_b: str) -> List[Tuple[str, str]]:
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

        for group, services in self.network_group_registry.items():
            for svc in services:
                for node in svc.nodes:
                    qdisc_output = self._exec_tc_show_command(node, "qdisc")
                    filter_output = self._exec_tc_show_command(node, "filter")

                    dst_ips = self._parse_filter_destinations(filter_output)
                    constraints = self._parse_qdisc_constraints(qdisc_output)

                    targets_str = f" -> to [{', '.join(dst_ips)}]" if dst_ips and constraints != "noqueue" else ""
                    node_ip = socket.gethostbyname(node.account.externally_routable_ip)

                    self.logger.debug(f"[{group:<4}] {svc.who_am_i(node):<45}[{node_ip}] : {constraints}{targets_str}")

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
