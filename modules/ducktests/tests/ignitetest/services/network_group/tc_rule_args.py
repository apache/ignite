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
from typing import Optional

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
