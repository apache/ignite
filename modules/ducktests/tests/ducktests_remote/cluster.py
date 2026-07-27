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
Cluster inventory: from the YAML ``cluster.nodes`` list to ducktape's ``cluster.json``.

Nothing here assumes a cluster size.  Two nodes and forty-nine nodes go down the same
code path and produce the same output shape.
"""

import json
import re
from dataclasses import dataclass
from typing import List, Optional

from ducktests_remote.config import ConfigError

_RANGE = re.compile(r"\[(\d+)-(\d+)\]")


@dataclass(frozen=True)
class Node:
    """One worker host as ducktape will see it."""

    host: str
    ip: Optional[str] = None
    user: Optional[str] = None
    port: int = 22
    identity_file: Optional[str] = None

    @property
    def target(self):
        """:return: ``user@host`` for ssh, or the bare host when no user is set."""
        return "%s@%s" % (self.user, self.host) if self.user else self.host

    @property
    def externally_routable_ip(self):
        """:return: the ip when the inventory gave one, else the hostname."""
        return self.ip or self.host

    def to_ssh_config(self):
        """:return: the ``ssh_config`` block ducktape's RemoteAccountSSHConfig accepts."""
        return {
            "host": self.host,
            "hostname": self.host,
            "user": self.user,
            "port": int(self.port),
            "identityfile": self.identity_file,
            "password": "",
        }


def expand_hosts(pattern):
    """
    Expand ``node[01-12].dc.local`` into twelve hostnames.

    Zero padding follows the width of the lower bound, so ``[01-12]`` yields ``node01``
    through ``node12`` and ``[1-12]`` yields ``node1`` through ``node12``.  Several
    ranges in one pattern expand as a cartesian product, left to right.
    """
    match = _RANGE.search(pattern)
    if not match:
        return [pattern]
    start_raw, end_raw = match.group(1), match.group(2)
    start, end = int(start_raw), int(end_raw)
    if end < start:
        raise ConfigError("invalid host range %r: %d > %d" % (pattern, start, end))
    width = len(start_raw)
    expanded = []
    for value in range(start, end + 1):
        replaced = pattern[:match.start()] + str(value).zfill(width) + pattern[match.end():]
        expanded.extend(expand_hosts(replaced))
    return expanded


def load_nodes(cluster_cfg) -> List[Node]:
    """
    Build the node list from the ``cluster`` config section.

    Entries may be a bare string (``10.0.0.13`` or ``node[01-12].dc.local``) or a mapping
    with ``host`` and optional ``ip`` / ``user`` / ``port`` / ``identity_file``.  Per-host
    overrides exist because mixed clusters, where an account exists on some machines and
    not others, are the normal case rather than the exception.
    """
    default_user = cluster_cfg.get("user")
    default_port = cluster_cfg.get("port", 22)
    default_identity = cluster_cfg.get("identity_file")

    nodes = []
    for index, entry in enumerate(cluster_cfg.get("nodes") or []):
        if isinstance(entry, str):
            entry = {"host": entry}
        if not isinstance(entry, dict):
            raise ConfigError("cluster.nodes[%d]: expected a string or a mapping, found %s"
                              % (index, type(entry).__name__))
        host = entry.get("host")
        if not host:
            raise ConfigError("cluster.nodes[%d]: missing 'host'" % index)
        unknown = set(entry) - {"host", "ip", "user", "port", "identity_file"}
        if unknown:
            raise ConfigError("cluster.nodes[%d]: unknown keys %s"
                              % (index, ", ".join(sorted(unknown))))
        expanded = expand_hosts(host)
        if len(expanded) > 1 and entry.get("ip"):
            raise ConfigError("cluster.nodes[%d]: 'ip' cannot be combined with a host range"
                              % index)
        for name in expanded:
            nodes.append(Node(host=name,
                              ip=entry.get("ip"),
                              user=entry.get("user", default_user),
                              port=int(entry.get("port", default_port)),
                              identity_file=entry.get("identity_file", default_identity)))

    seen = set()
    for node in nodes:
        if node.host in seen:
            raise ConfigError("cluster.nodes: duplicate host %r" % node.host)
        seen.add(node.host)
    return nodes


def load_extra_hosts(cluster_cfg) -> List[Node]:
    """:return: hosts targeted by provision/deploy/clean/doctor but kept out of cluster.json."""
    sub = dict(cluster_cfg)
    sub["nodes"] = cluster_cfg.get("extra_hosts") or []
    return load_nodes(sub)


def select_nodes(nodes, num_nodes=None):
    """
    Take the first ``num_nodes`` inventory entries.

    This is the analogue of ``IGNITE_NUM_CONTAINERS`` in ``docker/run_tests.sh``.
    """
    if num_nodes is None:
        return list(nodes)
    if num_nodes < 1:
        raise ConfigError("--num-nodes must be at least 1")
    if num_nodes > len(nodes):
        raise ConfigError(
            "--num-nodes %d exceeds the inventory, which lists %d host%s. "
            "Add hosts to cluster.nodes or lower --num-nodes."
            % (num_nodes, len(nodes), "" if len(nodes) == 1 else "s"))
    return list(nodes[:num_nodes])


def cluster_json(nodes, identity_file=None):
    """
    :param nodes: the selected :class:`Node` list.
    :param identity_file: runner-side fallback identity for nodes without their own.
    :return: the ducktape cluster file as a dict.

    The schema is the one ``ducktape.cluster.json.JsonCluster`` reads: a ``nodes`` list of
    ``{externally_routable_ip, ssh_config}``, where ``ssh_config`` is passed straight into
    ``RemoteAccountSSHConfig(host, hostname, user, port, password, identityfile)``.
    """
    if not nodes:
        raise ConfigError("cluster.nodes is empty; there is nothing for ducktape to run on")
    payload = {"nodes": []}
    for node in nodes:
        ssh_config = node.to_ssh_config()
        if not ssh_config.get("identityfile"):
            ssh_config["identityfile"] = identity_file
        payload["nodes"].append({
            "externally_routable_ip": node.externally_routable_ip,
            "ssh_config": ssh_config,
        })
    return payload


def dumps(payload):
    """:return: the cluster file rendered as JSON text."""
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"
