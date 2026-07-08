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
from typing import Iterable, Optional

from ignitetest.services.network_group.configuration import CrossNetworkGroupConfiguration

# In non-interactive SSH sessions, 'sudo' enforces a strict 'secure_path'
# and ignores the user's environment. We explicitly pass common installation
# paths (global, virtualenv, and local user directories) via 'env PATH' so
# the system can locate the 'tcset' binary regardless of how tcconfig was installed.
SUDO_PREFIX = 'sudo env "PATH=$PATH:/home/ducker/.local/bin:/opt/venv/bin"'

# tcset rule actions.
ACTION_OVERWRITE = "--overwrite"
ACTION_ADD = "--add"
ACTION_CHANGE = "--change"

# -w: wait on the xtables lock instead of failing
IPTABLES = "sudo iptables -w"

PARTITION_CHAIN_PREFIX = "NP_"

# Extraction filter to find user-defined chains starting with our prefix
# - iptables -S outputs "-N CHAIN_NAME" for new chains
# - awk filters rows where column 1 is "-N" and column 2 matches our prefix
FIND_CUSTOM_CHAINS_AWK = f"awk '$1==\"-N\" && $2 ~ /^{PARTITION_CHAIN_PREFIX}/ {{print $2}}'"

# iptables chain names are limited to 28 characters.
MAX_CHAIN_NAME_LEN = 28


def to_tcset_cmd(interface: str, dst_host_or_ip: str, config: CrossNetworkGroupConfiguration,
                 action: str = ACTION_OVERWRITE) -> Optional[str]:
    """
    Compiles the full 'tcset' command string.
    Returns None if there are no network limitations configured.
    """
    if config.is_empty:
        return None

    dst_ip = socket.gethostbyname(dst_host_or_ip)

    args = [
        f"{SUDO_PREFIX} tcset {interface}",
        f"--dst-network {dst_ip}/32",
        action
    ]

    if config.delay:
        args.append(f"--delay {config.delay}")

    if config.loss is not None:
        args.append(f"--loss {config.loss * 100:g}%")

    return " ".join(args)


def to_tcdel_all_cmd(interface: str) -> str:
    """
    Compiles the absolute clear command for an interface.
    """
    return f"{SUDO_PREFIX} tcdel {interface} --all"


def to_tcdel_ip_cmd(interface: str, dst_host_or_ip: str) -> str:
    """
    Compiles the absolute clear command for destination host.
    """
    return f"{SUDO_PREFIX} tcdel {interface} --peer {dst_host_or_ip}"


def partition_chain_name(group_a: str, group_b: str) -> str:
    """
    Deterministic, order-independent iptables chain name for a group pair.
    """
    a, b = sorted((group_a, group_b))

    raw = f"{PARTITION_CHAIN_PREFIX}{a}_{b}"

    return re.sub(r"[^A-Za-z0-9_]", "_", raw)[:MAX_CHAIN_NAME_LEN]


def to_partition_enable_cmd(chain: str, remote_ips: Iterable[str]) -> str:
    """
    Compiles a single shell command that (idempotently) creates the partition
    chain, hooks it into INPUT/OUTPUT, and drops all traffic to and from the
    given remote IPs.

    Dropping both '-s' and '-d' on each side means the partition between a
    node pair is effective as soon as *either* endpoint has applied its rules,
    minimizing the window in which the partition is only half-visible.
    """
    cmds = [
        f"{{ {IPTABLES} -N {chain} 2>/dev/null || true; }}",
        f"{{ {IPTABLES} -C INPUT -j {chain} 2>/dev/null || {IPTABLES} -I INPUT 1 -j {chain}; }}",
        f"{{ {IPTABLES} -C OUTPUT -j {chain} 2>/dev/null || {IPTABLES} -I OUTPUT 1 -j {chain}; }}",
    ]

    for ip in remote_ips:
        cmds.append(f"{IPTABLES} -A {chain} -s {ip}/32 -j DROP")
        cmds.append(f"{IPTABLES} -A {chain} -d {ip}/32 -j DROP")

    return " && ".join(cmds)


def to_partition_disable_cmd(chain: str) -> str:
    """
    Compiles the healing command: flushes the pair's DROP rules in one call.
    The (now empty) chain and its INPUT/OUTPUT hooks are intentionally left in
    place, so re-enabling the same partition later stays a pure append and the
    final teardown remains the single owner of chain removal.
    """
    return f"{IPTABLES} -F {chain} 2>/dev/null || true"


def to_partition_teardown_cmd() -> str:
    """
    Compiles the full cleanup command: unhooks, flushes, and deletes
    every partition chain on the node, restoring a pristine iptables state.
    """
    find_chains_subshell = f"sudo iptables -S 2>/dev/null | {FIND_CUSTOM_CHAINS_AWK}"

    return (
        f"for c in $({find_chains_subshell}); do "
        f"{IPTABLES} -D INPUT -j $c 2>/dev/null; "
        f"{IPTABLES} -D OUTPUT -j $c 2>/dev/null; "
        f"{IPTABLES} -F $c && {IPTABLES} -X $c; "
        f"done; true"
    )
