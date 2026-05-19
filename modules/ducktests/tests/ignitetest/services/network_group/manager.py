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
import sys
from typing import Dict, List

from ducktape.services.service import Service

from ignitetest.services.network_group.configuration import NetworkGroupStore
from ignitetest.services.network_group.tc_rule_args import TcRuleArgs


class NetworkGroupManager:
    def __init__(self, network_group_store: NetworkGroupStore, network_group_registry: Dict[str, List[Service]]):
        self.network_group_store = network_group_store
        self.network_group_registry = network_group_registry
        self.interface = "eth0"

    def __enter__(self):
        self.deploy()

        self._log_network()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.destroy()

        self._log_network()

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

    def _log_network(self):
        print("___ Network State ___")
        for group, services in self.network_group_registry.items():
            for svc in services:
                for node in svc.nodes:
                    cmd = f"sudo tc qdisc show dev {self.interface}"
                    output = (
                        node.account.ssh_output(cmd, allow_fail=False, combine_stderr=False)
                        .decode(sys.getdefaultencoding())
                        .splitlines()
                    )
                    print(f"{svc.who_am_i(node)}: {output}")
        print("___ ___")
