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
from typing import Dict, List

from ducktape.services.service import Service

from ignitetest.services.network_group.configuration import NetworkGroupStore
from ignitetest.services.network_group.tc_rule_args import TcRuleArgs


class NetworkGroupManager:
    def __init__(self, logger, network_group_store: NetworkGroupStore, network_group_registry: Dict[str, List[Service]]):
        self.logger = logger

        self.network_group_store = network_group_store
        self.network_group_registry = network_group_registry
        self.interface = "eth0"

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
        all_groups = list(self.network_group_registry.keys())

        self.destroy()

        for src_group in all_groups:
            for dst_group in all_groups:
                if src_group == dst_group:
                    continue

                cfg = self.network_group_store.get_config(src_group, dst_group)

                if not cfg:
                    raise Exception(f"Group configuration not found for {src_group}-{dst_group}.")

                dst_ips = [
                    node.account.externally_routable_ip
                    for svc in self.network_group_registry[dst_group]
                    for node in svc.nodes
                ]

                for src_svc in self.network_group_registry[src_group]:
                    for src_node in src_svc.nodes:
                        for idx, dst_ip in enumerate(dst_ips):
                            rule = TcRuleArgs(
                                interface=self.interface,
                                dst_host_or_ip=dst_ip,
                                config=cfg
                            )

                            # The first tcset cmd should use --overwrite
                            action = "--overwrite" if idx == 0 else "--add"

                            cmd = rule.to_tcset_cmd(action=action)

                            if cmd:
                                src_node.account.ssh(cmd)
                            else:
                                raise ValueError(
                                    f"No network constraints defined for traffic traveling from "
                                    f"{src_svc.who_am_i(src_node)} to destination IP {dst_ip}. "
                                    f"Ensure CrossNetworkGroupConfiguration is configured in the NetworkGroupStore."
                                )

    def destroy(self):
        """Restores network interfaces back to their un-throttled state."""
        clear_cmd = TcRuleArgs.to_tcdel_all_cmd(self.interface)

        for services in self.network_group_registry.values():
            for svc in services:
                for node in svc.nodes:
                    node.account.ssh(clear_cmd)

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
        cmd = f"sudo tc {sub_system} show dev {self.interface}"
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
                    readable_ip = socket.inet_ntoa(ip_bytes)
                    dst_ips.append(readable_ip)
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
