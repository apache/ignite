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

import socket
from typing import Dict, Optional

from ignitetest.services.ignite import IgniteService
from ignitetest.services.utils.ignite_aware import IgniteAwareService
from ignitetest.utils.netem_manager import NetworkEmulatorManager


class CrossDCNetworkEmulatorConfig:
    """
    Cross DC Network connection configuration for NetEm filter.
    """

    def __init__(self, delay_ms: Optional[int] = None, jitter_ms: Optional[int] = None,
                 loss_percent: Optional[float] = None, duplicate_percent: Optional[float] = None,
                 corrupt_percent: Optional[float] = None):
        self.delay_ms = delay_ms
        self.jitter_ms = jitter_ms
        self.loss_percent = loss_percent
        self.duplicate_percent = duplicate_percent
        self.corrupt_percent = corrupt_percent

    def __str__(self) -> str:
        """
        Returns the configuration as a NetEm-style descriptive string,
        including only non-None values.
        """
        parts = []

        if self.delay_ms is not None:
            delay_str = f"delay {self.delay_ms}ms "
            if self.jitter_ms is not None:
                delay_str += f"{self.jitter_ms}ms distribution normal "
            parts.append(delay_str)

        if self.loss_percent is not None:
            parts.append(f"loss {self.loss_percent}% ")

        if self.duplicate_percent is not None:
            parts.append(f"duplicate {self.duplicate_percent}% ")

        if self.corrupt_percent is not None:
            parts.append(f"corrupt {self.corrupt_percent}% ")

        return " ".join(parts)


class CrossDCConfigStore:
    """
    A store for managing NetEm configurations between datacenters.
    """
    def __init__(self):
        self._configs: Dict[int, Dict[int, CrossDCNetworkEmulatorConfig]] = {}

    def set_cross_dc_config(self, from_dc: int, to_dc: int, config: CrossDCNetworkEmulatorConfig) -> None:
        self.set_config(from_dc, to_dc, config)
        self.set_config(to_dc, from_dc, config)

    def set_config(self, from_dc: int, to_dc: int, config: CrossDCNetworkEmulatorConfig) -> None:
        if from_dc not in self._configs:
            self._configs[from_dc] = {}

        self._configs[from_dc][to_dc] = config

    def get_config(self, from_dc: int, to_dc: int) -> Optional[str]:
        """
        Get the NetEm configuration string for traffic from `from_dc` to `to_dc`.
        Returns a tc netem-compatible string (e.g. "delay 50ms loss 1.0%").
        Returns None if no config is set for this direction.
        """
        config = self._configs.get(from_dc, {}).get(to_dc)

        return str(config) if config is not None else None


class _DCServiceManager:
    """
    Internal manager that MUST be used only through the builder.
    Enforces lifecycle safety: cannot use outside `with`, cannot reuse.
    """
    def __init__(self, context, cross_dc_config_store: CrossDCConfigStore):
        self._active = False
        self._closed = False

        self.netem_manager = NetworkEmulatorManager(context)
        self.cross_dc_config_store = cross_dc_config_store

        self.context = context

        self._svc_by_svc_name: Dict[str, IgniteAwareService] = {}
        self._dc_by_svc_name: Dict[str, int] = {}

    def __enter__(self):
        if self._closed:
            raise AssertionError("DCServiceManager already closed and cannot be reused.")

        self._active = True

        return self

    def __exit__(self, exc_type, exc, tb):
        self._active = False
        self._closed = True

        for svc in self._svc_by_svc_name.values():
            svc.stop(force_stop=True)

        self._restore_cluster_network()

        self._svc_by_svc_name.clear()
        self._dc_by_svc_name.clear()

        return False

    @property
    def logger(self):
        """The logger instance for this manager."""
        return self.context.logger

    def start_ignite_service(self, context, ignite_cfg, num_nodes, svc_name, dc_idx):
        svc = IgniteService(context, ignite_cfg, num_nodes=num_nodes, jvm_opts=[f"-DIGNITE_DATA_CENTER_ID=DC{dc_idx}"])

        return self.start_service(svc_name=svc_name, dc_idx=dc_idx, svc=svc)

    def start_service(self, svc_name: str, dc_idx: int, svc):
        self._ensure_active()

        assert svc_name not in self._svc_by_svc_name, f"Service {svc_name} already defined."

        assert dc_idx >= 1, f"dc_idx must be 1-based, but received: {dc_idx}"

        self.logger.info(f"Starting service [svcName={svc_name}, dc_idx={dc_idx}].")

        self._svc_by_svc_name[svc_name] = svc
        self._dc_by_svc_name[svc_name] = dc_idx

        self._update_cluster_network_on_service_start(svc, dc_idx)

        svc.start()

        self.logger.info(f"Service start-up is complete [svcName={svc_name}].")

        return svc

    def stop_service(self, svc_name: str, force_stop=False, **kwargs):
        self._ensure_active()

        assert svc_name in self._svc_by_svc_name, f"Service {svc_name} is not known."

        svc = self._svc_by_svc_name.pop(svc_name)
        dc_idx = self._dc_by_svc_name.pop(svc_name)

        self.logger.info(f"Stopping service [svcName={svc_name}, dc_idx={dc_idx}].")

        svc.stop(force_stop=force_stop, **kwargs)

        self._update_cluster_network_on_service_stop(svc, dc_idx)

        self.logger.info(f"Service is stopped [svcName={svc_name}].")

    def _ensure_active(self):
        if not self._active:
            raise AssertionError("DCServiceManager must be used inside a 'with' block (manager inactive).")

    def _update_cluster_network_on_service_start(self, svc: IgniteAwareService, dc_idx: int):
        self.logger.info("Cluster network update on service start.")

        for node in svc.nodes:
            self.logger.info(f"{svc.who_am_i(node)} - {socket.gethostbyname(node.account.hostname)}")

        self._restore_service_network(svc)

        self._mark_cross_dc_traffic_on_service_start(svc, dc_idx)

        self._set_network_emulator_on_service_start(svc, dc_idx)

        self.logger.info("Cluster network update is complete.")

    def _update_cluster_network_on_service_stop(self, svc: IgniteAwareService, dc_idx: int):
        self.logger.info("Cluster network update on service stop.")

        self._restore_service_network(svc)

        self._unmark_incoming_traffic_on_service_stop(svc, dc_idx)

        self._remove_network_emulator_on_remote_servers_if_needed(dc_idx)

        self.logger.info("Cluster network update is complete.")

    def _mark_cross_dc_traffic_on_service_start(self, local_service: IgniteAwareService, local_dc_idx: int):
        """
        Mark outgoing traffic from nodes in `local_service` to services in other data centers.
        Applies firewall marks so that tc can apply NetEm (delay/loss) per DC.
        """
        for service_name, remote_service in self._svc_by_svc_name.items():
            remote_dc_idx = self._dc_by_svc_name.get(service_name)

            if remote_dc_idx == local_dc_idx:
                continue

            for local_node in local_service.nodes:
                for remote_node in remote_service.nodes:
                    self.logger.debug(f"Configuring cross-DC traffic marking "
                                      f"from {local_service.who_am_i(local_node)} "
                                      f"to {remote_service.who_am_i(remote_node)}.")

                    self.netem_manager.mark_traffic(
                        node=local_node,
                        destination_ip=remote_node.account.externally_routable_ip,
                        dc_idx=remote_dc_idx
                    )

                    self.logger.debug(f"Configuring cross-DC traffic marking "
                                      f"from {remote_service.who_am_i(remote_node)} "
                                      f"to {remote_service.who_am_i(remote_node)}.")

                    self.netem_manager.mark_traffic(
                        node=remote_node,
                        destination_ip=local_node.account.externally_routable_ip,
                        dc_idx=local_dc_idx
                    )

    def _unmark_incoming_traffic_on_service_stop(self, local_service: IgniteAwareService, local_dc_idx: int):
        """
        Remove iptables MARK rules on REMOTE nodes for traffic destined to this service's nodes.

        When a service stops, we need to clean up:
          - On each REMOTE node: remove the iptables rule that marked traffic to this service.

        This ensures that after shutdown, no residual packet marking affects network behaviour,
        and NetEm (delay/loss) rules are not applied to stale destinations.
        """
        self.logger.debug(
            f"Removing cross-DC traffic marking for incoming traffic to {local_service.who_am_i()} "
            f"in DC {local_dc_idx}."
        )

        for service_name, remote_service in self._svc_by_svc_name.items():
            remote_dc_idx = self._dc_by_svc_name.get(service_name)

            if remote_dc_idx == local_dc_idx:
                continue

            for local_node in local_service.nodes:
                for remote_node in remote_service.nodes:
                    self.netem_manager.unmark_traffic(
                        node=remote_node,
                        destination_ip=local_node.account.externally_routable_ip,
                        dc_idx=local_dc_idx
                    )

        self.logger.debug("Cleanup completed: incoming traffic marking removed.")

    def _set_network_emulator_on_service_start(self, local_service: IgniteAwareService, local_dc_idx: int):
        """
        Enable network emulation (NetEm) for cross-datacenter traffic on service start.

        This method ensures bidirectional traffic shaping by configuring:
          1. On LOCAL nodes: HTB class, NetEm qdisc, and tc filter for OUTBOUND traffic to each remote DC.
          2. On REMOTE nodes: same setup for RETURN traffic (to shape replies back to this DC).

        As a result, both outgoing and incoming cross-DC traffic will be subject to configured
        delay, loss, jitter, etc., ensuring realistic network conditions in both directions.
        """
        self.logger.debug(f"Creating HTB traffic control root for {local_service.who_am_i()} in DC {local_dc_idx}.")

        for local_node in local_service.nodes:
            self.netem_manager.init_traffic_control_root(local_node)

        self.logger.debug(f"Setting up outbound network emulation for {local_service.who_am_i()} in DC {local_dc_idx}.")

        remote_dcs = {dc for dc in self._dc_by_svc_name.values() if dc != local_dc_idx}

        for target_dc_idx in remote_dcs:
            netem_cfg = self.cross_dc_config_store.get_config(local_dc_idx, target_dc_idx)

            for local_node in local_service.nodes:
                self.netem_manager.set_network_emulator(local_node, target_dc_idx, netem_cfg)

        self.logger.debug(
            f"Ensuring return-path network emulation on REMOTE nodes for replies to {local_service.who_am_i()} "
            f"in DC {local_dc_idx}."
        )

        for service_name, remote_service in self._svc_by_svc_name.items():
            remote_dc_idx = self._dc_by_svc_name.get(service_name)

            if remote_dc_idx == local_dc_idx:
                continue

            netem_cfg = self.cross_dc_config_store.get_config(remote_dc_idx, local_dc_idx)

            for remote_node in remote_service.nodes:
                if not self.netem_manager.htb_class_exists(remote_node, local_dc_idx):
                    self.netem_manager.set_network_emulator(remote_node, local_dc_idx, netem_cfg)

        self.logger.debug(
            f"Network emulation fully configured for bidirectional traffic for {local_service.who_am_i()} "
            f"in DC {local_dc_idx}."
        )

    def _remove_network_emulator_on_remote_servers_if_needed(self, removed_svc_dc_idx: int):
        """
        Remove HTB/netem configuration associated with a data center that is no longer used.

        If the removed data center index is still referenced by any remaining service,
        the network emulator must be kept intact. Otherwise, all HTB classes, netem
        qdiscs, and related filters for this data center are removed from every node.
        """
        # Check whether the data center index is still used by any active service.
        # If it is referenced at least once, we must not remove its network emulation.
        for dc in self._dc_by_svc_name.values():
            if dc == removed_svc_dc_idx:
                return

        self.logger.debug(f"Removing network emulator for {removed_svc_dc_idx} in cluster.")

        for service in self._svc_by_svc_name.values():
            for node in service.nodes:
                if self.netem_manager.htb_class_exists(node, removed_svc_dc_idx):
                    self.netem_manager.remove_network_emulator(node, removed_svc_dc_idx)

    def _restore_service_network(self, svc: IgniteAwareService):
        """
        Restore network settings for all nodes of a specific service.
        Removes any previously applied traffic control (HTB, netem) and iptables marks,
        returning the node's network to its original state.
        """
        for node in svc.nodes:
            if self.netem_manager.has_root_htb_qdisc(node):
                self.logger.debug(f"Restoring network configuration for {svc.who_am_i(node)}.")

                self.netem_manager.reset_network_settings(node)

                self.logger.debug(f"Network configuration is restored for {svc.who_am_i(node)}.")

    def _restore_cluster_network(self):
        """
        Restore network settings for all services in the cluster.
        Used during cluster shutdown or test cleanup to ensure no residual network emulation remains.
        """
        self.logger.info("Starting full cluster network restoration.")

        for svc in self._svc_by_svc_name.values():
            for node in svc.nodes:
                if self.netem_manager.has_root_htb_qdisc(node):
                    self.logger.debug(f"Processing {svc.who_am_i(node)}")

                    self.netem_manager.reset_network_settings(node)

                self.logger.debug(f"Network configuration is restored for {svc.who_am_i(node)}.")

        self.logger.info("Cluster-wide network restoration completed.")


class DCService:
    """
    Returns a context manager for MultiDC service.
    """
    @classmethod
    def scope(cls, context, cross_dc_config_store):
        return _DCServiceManager(context, cross_dc_config_store)
