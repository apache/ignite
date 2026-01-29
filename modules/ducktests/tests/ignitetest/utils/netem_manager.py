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

import functools
import inspect
import sys

from ignitetest.services.utils.decorators import memoize

# Mark outgoing cross-DC traffic.
# Uses the 'mangle' table to set a firewall mark (fwmark) on packets.
# Traffic to {destination_ip} is marked with {traffic_mark}, which will be used by tc filters.
# Note: {traffic_mark} = {dc_idx} * 10 (e.g., DC 1 → mark=10)
CMD_MARK_TRAFFIC = "sudo iptables -t mangle -{action_flag} OUTPUT -d {destination_ip} -j MARK --set-mark {traffic_mark}"

# Initialize root HTB qdisc with default class for unclassified traffic.
# Sets up:
#   - root qdisc with handle 1:
#   - default class 1:1 for all traffic not matched by filters (SSH, DNS, local, etc.)
# Important: classid 1:1 is reserved for default traffic. DC-specific classes start from 1:10, 1:20, etc.
CMD_INIT_TRAFFIC_CONTROL_ROOT = ("sudo tc qdisc add dev {net} root handle 1: htb default 1 && "
                                 "sudo tc class add dev {net} parent 1: classid 1:1 htb rate {interface_rate_mbps}Mbps")

# Create an HTB class for a specific data center.
# Class ID is 1:{dc_mark}, where {dc_mark} = {dc_idx} * 10 (e.g., DC 1 → 1:10).
# Each class gets its own bandwidth allocation.
CMD_SET_DC_HTB_CLASS = "sudo tc class add dev {net} parent 1: classid 1:{dc_mark} htb rate {interface_rate_mbps}Mbps"

# Attach a netem qdisc to a specific HTB class to emulate network conditions.
# Adds delay, packet loss, jitter, etc. only to traffic in class 1:{dc_mark}.
# The netem qdisc is identified by handle {netem_id}: (usually equal to dc_mark).
CMD_SET_NETEM_FOR_DC_HTB_CLASS = "sudo tc qdisc add dev {net} parent 1:{dc_mark} handle {netem_id}: netem {netem_cfg}"

# Set up a filter to route marked traffic into the corresponding HTB class.
# Matches packets with fwmark={traffic_mark} (set by iptables) and routes them to classid 1:{dc_mark}.
# This links iptables marking with tc traffic classification.
CMD_SET_FILTER_FOR_DC_HTB_CLASS = \
    "sudo tc filter add dev {net} protocol ip parent 1:0 prio 1 handle {traffic_mark} fw classid 1:{dc_mark}"

# Delete an HTB class for a specific data center.
# Removes class 1:{dc_mark} from the HTB root (1:).
# NOTE: Any attached qdiscs and filters MUST be removed first.
CMD_DEL_DC_HTB_CLASS = "sudo tc class del dev {net} classid 1:{dc_mark}"

# Delete the netem qdisc attached to a specific HTB class.
# Removes traffic emulation (delay, loss, jitter, etc.) from class 1:{dc_mark}.
# Safe to call even if the qdisc is already absent.
CMD_DEL_NETEM_FOR_DC_HTB_CLASS = "sudo tc qdisc del dev {net} parent 1:{dc_mark}"

# Delete the fwmark-based tc filter for a specific data center.
# Removes the filter that redirects packets with fwmark={traffic_mark}
# from the HTB root (1:) into class 1:{dc_mark}.
# Only the filter matching (protocol, parent, prio, handle, type=fw) is removed.
CMD_DEL_FILTER_FOR_DC_HTB_CLASS = \
    "sudo tc filter del dev {net} protocol ip parent 1:0 prio 1 handle {traffic_mark} fw"

# Diagnostic commands — show current state of traffic control and marking rules.
# Useful for debugging and verification.

# Show all qdiscs (HTB root, netem, ingress, etc.)
CMD_TC_QDISC_SHOW = "sudo tc qdisc show dev {net}"
# Show all HTB classes (1:1, 1:10, 1:20, etc.)
CMD_TC_CLASS_SHOW = "sudo tc class show dev {net}"
# Show tc filters — how traffic is classified and routed
CMD_TC_FILTER_SHOW = "sudo tc filter show dev {net}"
# Show iptables rules in mangle/OUTPUT — what traffic is being marked
CMD_IPTABLES_SHOW = "sudo iptables -t mangle -nvL OUTPUT"

# Get the default network interface (e.g., eth0, ens3)
CMD_GET_NETWORK_INTERFACE = "ip route | grep default | awk -- '{printf $5}'"
# Read interface speed in Mbps from sysfs (e.g., 1000 for 1Gbps)
CMD_GET_INTERFACE_RATE = "cat /sys/class/net/{net}/speed"

# Reset network settings to clean state.
# Removes root qdisc (automatically deletes all child classes and filters).
# Clears iptables marks in the mangle table.

# Remove root HTB qdisc and all associated classes and qdiscs
CMD_RESET_QDISC = "sudo tc qdisc del dev {net} root"

# Clear all MARK rules in mangle/OUTPUT chain
CMD_RESET_IPTABLES = "sudo iptables -t mangle -F OUTPUT"


def log_network_components(debug_only=False):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, node, *args, **kwargs):
            sig = inspect.signature(func)
            bound_args = sig.bind(self, node, *args, **kwargs)
            bound_args.apply_defaults()

            # Arguments except 'self', and 'node'
            args_str = "".join(f", {k}={v}" for k, v in list(bound_args.arguments.items())[2:])

            call_label = f"{func.__name__}[node={node.account.hostname}{args_str}]"

            self.log_network(node, label=f"[{call_label}][before]", debug_only=debug_only)

            result = func(self, node, *args, **kwargs)

            self.log_network(node, label=f"[{call_label}][after]", debug_only=debug_only)

            return result

        return wrapper

    return decorator


class NetworkEmulatorManager:
    """
    NetEm manager
    """
    def __init__(self, context):
        self.context = context

    @property
    def logger(self):
        """The logger instance for this manager."""
        return self.context.logger

    def log_network(self, node, label, debug_only=False):
        net_settings = self.network_settings_on_node(node).items()

        line = f"{label}"

        for setting, value in net_settings:
            line = line + f"\n{setting}:\n{value}\n"

        if debug_only:
            self.logger.debug(line)
        else:
            self.logger.info(line)

    def network_settings_on_node(self, node):
        net = self._get_default_network_interface(node)

        settings = {
            "tc_qdisc": self._get_ssh_output(node, cmd=CMD_TC_QDISC_SHOW.format(net=net)),
            "tc_classes": self._get_ssh_output(node, cmd=CMD_TC_CLASS_SHOW.format(net=net)),
            "tc_filter": self._get_ssh_output(node, cmd=CMD_TC_FILTER_SHOW.format(net=net)),
            "iptables": self._get_ssh_output(node, cmd=CMD_IPTABLES_SHOW)
        }

        return settings

    @log_network_components(debug_only=True)
    def init_traffic_control_root(self, node):
        net = self._get_default_network_interface(node)
        interface_rate_mbps = self._get_default_interface_rate(node)

        node.account.ssh(CMD_INIT_TRAFFIC_CONTROL_ROOT.format(net=net, interface_rate_mbps=interface_rate_mbps))

    @log_network_components(debug_only=True)
    def set_network_emulator(self, node, dc_idx, netem_cfg):
        net = self._get_default_network_interface(node)
        interface_rate_mbps = self._get_default_interface_rate(node)

        traffic_mark = dc_idx * 10
        dc_mark = dc_idx * 10
        netem_id = dc_idx * 10

        node.account.ssh(CMD_SET_DC_HTB_CLASS.format(net=net, dc_mark=dc_mark, interface_rate_mbps=interface_rate_mbps))

        node.account.ssh(CMD_SET_NETEM_FOR_DC_HTB_CLASS.format(net=net, dc_mark=dc_mark, netem_id=netem_id,
                                                               netem_cfg=netem_cfg))

        node.account.ssh(CMD_SET_FILTER_FOR_DC_HTB_CLASS.format(net=net, traffic_mark=traffic_mark, dc_mark=dc_mark))

    @log_network_components(debug_only=True)
    def remove_network_emulator(self, node, dc_idx):
        net = self._get_default_network_interface(node)

        traffic_mark = dc_idx * 10
        dc_mark = dc_idx * 10

        node.account.ssh(CMD_DEL_FILTER_FOR_DC_HTB_CLASS.format(net=net, traffic_mark=traffic_mark))

        node.account.ssh(CMD_DEL_NETEM_FOR_DC_HTB_CLASS.format(net=net, dc_mark=dc_mark))

        node.account.ssh(CMD_DEL_DC_HTB_CLASS.format(net=net, dc_mark=dc_mark))

    def has_root_htb_qdisc(self, node):
        """
        Check if 'htb' is the root qdisc on the default network interface.
        """
        net = self._get_default_network_interface(node)

        tc_qdisc = self._get_ssh_output(node, cmd=CMD_TC_QDISC_SHOW.format(net=net))

        return "htb" in tc_qdisc

    def htb_class_exists(self, node, dc_idx):
        """
        Check if 'htb' class exists for specified dc_idx on the provided node
        """
        net = self._get_default_network_interface(node)
        output = self._get_ssh_output(node, cmd=CMD_TC_CLASS_SHOW.format(net=net))

        dc_mark = dc_idx * 10
        htb_class = f"class htb 1:{dc_mark} "

        return htb_class in output

    @log_network_components(debug_only=True)
    def mark_traffic(self, node, destination_ip, dc_idx):
        traffic_mark = dc_idx * 10

        node.account.ssh(CMD_MARK_TRAFFIC.format(action_flag="A", destination_ip=destination_ip,
                                                 traffic_mark=traffic_mark))

    @log_network_components(debug_only=True)
    def unmark_traffic(self, node, destination_ip, dc_idx):
        traffic_mark = dc_idx * 10

        node.account.ssh(CMD_MARK_TRAFFIC.format(action_flag="D", destination_ip=destination_ip,
                                                 traffic_mark=traffic_mark))

    @log_network_components()
    def reset_network_settings(self, node):
        net = self._get_default_network_interface(node)

        node.account.ssh(CMD_RESET_QDISC.format(net=net))
        node.account.ssh(CMD_RESET_IPTABLES)

    @memoize
    def _get_default_interface_rate(self, node):
        net = self._get_default_network_interface(node)

        return self._get_ssh_output(node, CMD_GET_INTERFACE_RATE.format(net=net))

    @memoize
    def _get_default_network_interface(self, node):
        return self._get_ssh_output(node, CMD_GET_NETWORK_INTERFACE)

    @staticmethod
    def _get_ssh_output(node, cmd):
        return node.account.ssh_output(cmd) \
            .decode(sys.getdefaultencoding()) \
            .strip()
