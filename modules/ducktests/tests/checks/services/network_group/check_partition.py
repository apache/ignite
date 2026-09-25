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
Checks how NetworkGroupManager compiles a partition into per-node commands.

A cluster of three or more groups can be cut apart along several links at once, and the
intermediate topologies of a link-by-link rollout are themselves valid segmentations the
cluster would react to. So what matters is that every node is configured by exactly one
SSH round-trip carrying every chain that node takes part in - and no chain it does not.
"""

import logging

import pytest

from ignitetest.services.network_group.manager import NetworkGroupManager
from ignitetest.services.network_group.tc_rule_args import partition_chain_name

DC_1, DC_2, DC_3 = "DC1", "DC2", "DC3"

DCS = (DC_1, DC_2, DC_3)

NODES_PER_DC = 2


class FakeNode:
    """A node is only ever an identity and an account here."""
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return self.name


class FakeService:
    """The single member NetworkGroupManager reads off a registered service."""
    def __init__(self, nodes):
        self.nodes = nodes


@pytest.fixture(name="manager")
def _manager(monkeypatch):
    """
    A manager over three groups whose SSH layer is replaced by a recorder: yields the
    manager and the list of (node, command) tasks it submits.
    """
    registry = {dc: [FakeService([FakeNode(f"{dc}-{i}") for i in range(NODES_PER_DC)])] for dc in DCS}

    tasks = []

    monkeypatch.setattr(NetworkGroupManager, "_resolve_group_ips", lambda self, group: [f"{group}-ip"])
    monkeypatch.setattr(NetworkGroupManager, "_ssh_parallel", lambda self, submitted, tag: tasks.extend(submitted))
    monkeypatch.setattr(NetworkGroupManager, "_log_network", lambda self, log_tag: None)

    yield NetworkGroupManager(logging.getLogger(__name__), None, registry), tasks


def _commands_by_node(tasks):
    return {node.name: cmd for node, cmd in tasks}


class CheckNetworkPartition:
    """
    Checks the per-node command compilation of single and multi-link partitions.
    """
    def check_one_round_trip_per_node(self, manager):
        """A three way split configures each of the six nodes exactly once."""
        mgr, tasks = manager

        mgr.enable_network_partitions((DC_1, DC_2), (DC_1, DC_3), (DC_2, DC_3))

        assert len(tasks) == len(DCS) * NODES_PER_DC, "A node must be configured by a single SSH round-trip"

        assert len(_commands_by_node(tasks)) == len(tasks), "Node commands must not be split across tasks"

    def check_a_node_carries_every_chain_it_is_in(self, manager):
        """Each node gets the chains of its own links, and none of the link it sits out."""
        mgr, tasks = manager

        mgr.enable_network_partitions((DC_1, DC_2), (DC_1, DC_3), (DC_2, DC_3))

        cmds = _commands_by_node(tasks)

        for dc in DCS:
            own_chains = {partition_chain_name(dc, other) for other in DCS if other != dc}

            foreign_chain = partition_chain_name(*[other for other in DCS if other != dc])

            for i in range(NODES_PER_DC):
                cmd = cmds[f"{dc}-{i}"]

                for chain in own_chains:
                    assert chain in cmd, f"{dc}-{i} is missing chain {chain}"

                assert foreign_chain not in cmd, f"{dc}-{i} took part in the foreign chain {foreign_chain}"

    def check_isolating_one_group_leaves_the_others_connected(self, manager):
        """Cutting DC3 off touches DC1 and DC2 only through their links to DC3."""
        mgr, tasks = manager

        mgr.enable_network_partitions((DC_3, DC_1), (DC_3, DC_2))

        cmds = _commands_by_node(tasks)

        assert len(cmds) == len(DCS) * NODES_PER_DC, "Every node of every group takes part in the cut"

        assert partition_chain_name(DC_1, DC_2) not in cmds[f"{DC_1}-0"], \
            "The groups left behind must keep seeing each other"

        # The isolated group holds both chains; the ones left behind hold only their own.
        assert partition_chain_name(DC_1, DC_3) in cmds[f"{DC_3}-0"]
        assert partition_chain_name(DC_2, DC_3) in cmds[f"{DC_3}-0"]

        assert partition_chain_name(DC_2, DC_3) not in cmds[f"{DC_1}-0"]

    def check_single_pair_partition_is_a_multi_partition_of_one(self, manager):
        """The pairwise entry points stay the one-link case of the batched ones."""
        mgr, tasks = manager

        mgr.enable_network_partition(DC_1, DC_2)

        cmds = _commands_by_node(tasks)

        assert set(cmds) == {f"{DC_1}-0", f"{DC_1}-1", f"{DC_2}-0", f"{DC_2}-1"}, \
            "Only the two groups of the cut link are configured"

        chain = partition_chain_name(DC_1, DC_2)

        assert all(chain in cmd for cmd in cmds.values())

    def check_heal_flushes_every_chain_on_every_node(self, manager):
        """Healing a multi-link split flushes each node's chains in one round-trip."""
        mgr, tasks = manager

        mgr.disable_network_partitions((DC_1, DC_2), (DC_1, DC_3), (DC_2, DC_3))

        cmds = _commands_by_node(tasks)

        assert len(cmds) == len(DCS) * NODES_PER_DC

        for dc in DCS:
            for other in DCS:
                if other != dc:
                    assert partition_chain_name(dc, other) in cmds[f"{dc}-0"]
