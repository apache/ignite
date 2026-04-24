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

from typing import Dict, List

from ducktape.services.service import Service
from enoslib import NetemHTB, Host

from ignitetest.services.network_group.configuration import NetworkGroupStore


class NetworkGroupManager:
    def __init__(self, network_group_store: NetworkGroupStore, network_group_registry: Dict[str, List[Service]]):
        self.network_group_store = network_group_store
        self.network_group_registry = network_group_registry
        self.netem = NetemHTB()

    def _build_roles(self) -> Dict[str, List[Host]]:
        """Converts IgniteAwareService into EnOSlib Host objects."""
        roles = {}

        for group, services in self.network_group_registry.items():
            roles[group] = []

            for svc in services:
                for node in svc.nodes:
                    roles[group].append(self._to_enos_host(node))

        return roles

    @staticmethod
    def _to_enos_host(node) -> Host:
        """
        Converts IgniteAwareService node into EnOSlib Host object.
        """
        cfg = node.account.ssh_config

        enos_host = Host(
            address=node.account.externally_routable_ip,
            alias=cfg.host,
            user=cfg.user,
            port=cfg.port,
            extra={
                "ansible_ssh_pass": cfg.password,
                "ansible_ssh_private_key_file": cfg.identityfile,
                "ansible_ssh_common_args": "-o PasswordAuthentication=yes" if cfg.password else "",
                "ansible_ssh_timeout": cfg.connecttimeout
            }
        )

        return enos_host

    def __enter__(self):
        self.deploy()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.destroy()

    def deploy(self):
        """
        Deploys network impairments across all registered services.

        Raises:
            Exception: If a configuration is missing for any source-to-destination network group,
                preventing a complete network simulation.
        """
        roles = self._build_roles()
        all_groups = list(roles.keys())

        self.destroy()

        for src_group in all_groups:
            for dst_group in all_groups:
                if src_group == dst_group:
                    continue

                cross_group_cfg = self.network_group_store.get_config(src_group, dst_group)

                if cross_group_cfg:
                    src_hosts = roles[src_group]
                    dst_hosts = roles[dst_group]

                    self.netem.add_constraints(
                        src=src_hosts,
                        dest=dst_hosts,
                        delay=cross_group_cfg.delay,
                        loss=cross_group_cfg.loss,
                        rate=cross_group_cfg.rate
                    )
                else:
                    raise Exception(f"Group configuration not found for {src_group}-{dst_group}. "
                                    f"Did you forget to add the configuration to NetworkGroupStore?")

        self.netem.deploy()

    def destroy(self):
        """Restores network to original state."""
        self.netem.destroy()