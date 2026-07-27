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

"""Checks for the inventory and the generated ducktape cluster file."""

import json

import pytest

from ducktests_remote.cluster import (cluster_json, dumps, expand_hosts, load_nodes,
                                      select_nodes)
from ducktests_remote.config import ConfigError


def _cfg(nodes, **kw):
    base = {"user": "tester", "port": 22, "identity_file": "/home/tester/.ssh/id_rsa",
            "nodes": nodes}
    base.update(kw)
    return base


class CheckRangeExpansion:
    """``node[01-12].dc.local`` shorthand."""

    def check_zero_padding_follows_the_lower_bound(self):
        assert expand_hosts("node[01-03].dc.local") == [
            "node01.dc.local", "node02.dc.local", "node03.dc.local"]

    def check_unpadded_range(self):
        assert expand_hosts("node[8-11]") == ["node8", "node9", "node10", "node11"]

    def check_a_plain_hostname_is_left_alone(self):
        assert expand_hosts("10.0.0.13") == ["10.0.0.13"]

    def check_forty_nine_nodes(self):
        assert len(expand_hosts("node[01-49].dc.local")) == 49

    def check_two_nodes(self):
        assert len(expand_hosts("node[01-02].dc.local")) == 2

    def check_inverted_range_is_rejected(self):
        with pytest.raises(ConfigError):
            expand_hosts("node[9-2]")


class CheckInventory:
    """Loading nodes from the config section."""

    def check_bare_string_shorthand(self):
        nodes = load_nodes(_cfg(["10.0.0.13"]))
        assert nodes[0].host == "10.0.0.13" and nodes[0].user == "tester"

    def check_per_host_user_override(self):
        nodes = load_nodes(_cfg([{"host": "a"}, {"host": "b", "user": "other"}]))
        assert [n.user for n in nodes] == ["tester", "other"]

    def check_externally_routable_ip_falls_back_to_the_hostname(self):
        nodes = load_nodes(_cfg([{"host": "a"}, {"host": "b", "ip": "10.0.0.2"}]))
        assert nodes[0].externally_routable_ip == "a"
        assert nodes[1].externally_routable_ip == "10.0.0.2"

    def check_duplicate_hosts_are_rejected(self):
        with pytest.raises(ConfigError):
            load_nodes(_cfg(["a", "a"]))

    def check_unknown_node_key_is_rejected(self):
        with pytest.raises(ConfigError):
            load_nodes(_cfg([{"host": "a", "usr": "typo"}]))

    def check_ip_cannot_be_combined_with_a_range(self):
        with pytest.raises(ConfigError):
            load_nodes(_cfg([{"host": "n[1-3]", "ip": "10.0.0.1"}]))


class CheckSelection:
    """``--num-nodes``, the analogue of IGNITE_NUM_CONTAINERS."""

    def check_truncation_takes_the_first_n(self):
        nodes = load_nodes(_cfg(["a", "b", "c", "d"]))
        assert [n.host for n in select_nodes(nodes, 2)] == ["a", "b"]

    def check_none_means_everything(self):
        nodes = load_nodes(_cfg(["a", "b"]))
        assert len(select_nodes(nodes, None)) == 2

    def check_too_many_names_the_inventory_size(self):
        nodes = load_nodes(_cfg(["a", "b"]))
        with pytest.raises(ConfigError) as ex:
            select_nodes(nodes, 5)
        assert "2 hosts" in str(ex.value)

    def check_zero_is_rejected(self):
        with pytest.raises(ConfigError):
            select_nodes(load_nodes(_cfg(["a"])), 0)


class CheckClusterFile:
    """The schema ducktape's JsonCluster reads."""

    def check_shape_matches_ducktape(self):
        nodes = load_nodes(_cfg([{"host": "node01.dc.local", "ip": "10.0.0.11"}]))
        payload = cluster_json(nodes)
        entry = payload["nodes"][0]
        assert entry["externally_routable_ip"] == "10.0.0.11"
        # RemoteAccountSSHConfig(host, hostname, user, port, password, identityfile)
        assert set(entry["ssh_config"]) == {
            "host", "hostname", "user", "port", "identityfile", "password"}
        assert entry["ssh_config"]["port"] == 22
        assert entry["ssh_config"]["identityfile"] == "/home/tester/.ssh/id_rsa"

    def check_runner_side_identity_fallback(self):
        nodes = load_nodes(_cfg(["a"], identity_file=None))
        payload = cluster_json(nodes, identity_file="/runner/side/key")
        assert payload["nodes"][0]["ssh_config"]["identityfile"] == "/runner/side/key"

    def check_two_and_forty_nine_have_the_same_shape(self):
        small = cluster_json(load_nodes(_cfg(["n[01-02]"])))
        large = cluster_json(load_nodes(_cfg(["n[01-49]"])))
        assert len(small["nodes"]) == 2 and len(large["nodes"]) == 49
        assert small["nodes"][0].keys() == large["nodes"][0].keys()

    def check_empty_inventory_is_rejected(self):
        with pytest.raises(ConfigError):
            cluster_json([])

    def check_output_is_valid_json(self):
        rendered = dumps(cluster_json(load_nodes(_cfg(["a", "b"]))))
        assert len(json.loads(rendered)["nodes"]) == 2
