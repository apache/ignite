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
Checks the batched network probe NetworkGroupManager._log_network issues.

The probe replaces three per-node SSH round-trips with a single command, so what
matters is that its combined output still splits back into exactly the three
sections the separate commands produced, and that an incomplete answer degrades
into a poorer overview instead of an exception - this runs inside an active
network partition, where nothing may fail the test around it.
"""

import pytest

from ignitetest.services.network_group.manager import NetworkGroupManager, PROBE_SECTION_SEPARATOR
from ignitetest.services.network_group.tc_rule_args import partition_chain_name

INTERFACE = "eth0"

CHAIN = partition_chain_name("DC1", "DC2")

QDISC_SECTION = "qdisc netem 8001: root refcnt 2 limit 1000 delay 100ms loss 5%"

# 'c0a80105' is 192.168.1.5 as tc reports it in a u32 destination match.
FILTER_SECTION = "filter parent 8001: protocol ip pref 1 u32 chain 0\n" \
                 "  match c0a80105/ffffffff at 16"

IPTABLES_SECTION = "\n".join([
    "-P INPUT ACCEPT",
    f"-N {CHAIN}",
    f"-A {CHAIN} -s 192.168.1.5/32 -j DROP",
    f"-A {CHAIN} -d 192.168.1.5/32 -j DROP"
])


def _probe(*sections):
    return f"\n{PROBE_SECTION_SEPARATOR}\n".join(sections)


# What each section renders as in the overview when the node did not answer with it.
NEUTRAL_RENDERINGS = [
    (NetworkGroupManager._parse_qdisc_constraints, "noqueue"),
    (NetworkGroupManager._parse_filter_destinations, []),
    (lambda lines: NetworkGroupManager._format_partition_drops(
        NetworkGroupManager._parse_partition_drops(lines)), "")
]


class CheckNetworkProbe:
    """
    Checks the composition and the parsing of the batched network probe.
    """
    def check_probe_cmd_delimits_every_section(self, monkeypatch):
        """The probe asks for all three dumps in one command, separator between each."""
        monkeypatch.setattr(NetworkGroupManager, "_get_default_network_interface",
                            lambda self, node: INTERFACE)

        cmd = NetworkGroupManager(logger=None, network_group_store=None,
                                  network_group_registry={})._to_network_probe_cmd(node=None)

        assert cmd.count(PROBE_SECTION_SEPARATOR) == 2, "Three sections need exactly two separators"

        assert f"tc qdisc show dev {INTERFACE}" in cmd
        assert f"tc filter show dev {INTERFACE}" in cmd
        assert "iptables -S" in cmd

        # Unconditional chaining: one unavailable dump must not swallow the ones after it.
        assert "&&" not in cmd

    def check_splits_into_the_three_sections(self):
        """A complete probe yields exactly what the three separate commands produced."""
        qdisc, flt, iptables = NetworkGroupManager._split_probe_output(
            _probe(QDISC_SECTION, FILTER_SECTION, IPTABLES_SECTION))

        assert qdisc == QDISC_SECTION.splitlines()
        assert flt == FILTER_SECTION.splitlines()
        assert iptables == IPTABLES_SECTION.splitlines()

    def check_parses_an_active_partition(self):
        """The deployed impairment and the fully cut peer both survive the round trip."""
        qdisc, flt, iptables = NetworkGroupManager._split_probe_output(
            _probe(QDISC_SECTION, FILTER_SECTION, IPTABLES_SECTION))

        assert NetworkGroupManager._parse_qdisc_constraints(qdisc) == "netem(delay: 100ms, loss: 5%)"
        assert NetworkGroupManager._parse_filter_destinations(flt) == ["192.168.1.5"]

        drops = NetworkGroupManager._format_partition_drops(
            NetworkGroupManager._parse_partition_drops(iptables))

        assert drops == f" | partition: {CHAIN} <-X-> [192.168.1.5]"

    def check_parses_a_healed_link(self):
        """After a heal the netem impairment is still reported, the drops are gone."""
        qdisc, _, iptables = NetworkGroupManager._split_probe_output(
            _probe(QDISC_SECTION, FILTER_SECTION, "-P INPUT ACCEPT"))

        assert NetworkGroupManager._parse_qdisc_constraints(qdisc) == "netem(delay: 100ms, loss: 5%)"
        assert NetworkGroupManager._format_partition_drops(
            NetworkGroupManager._parse_partition_drops(iptables)) == ""

    def check_flags_a_half_applied_partition(self):
        """A one-way drop is called out - the reason the iptables dump is worth keeping."""
        _, _, iptables = NetworkGroupManager._split_probe_output(
            _probe(QDISC_SECTION, FILTER_SECTION, f"-A {CHAIN} -d 192.168.1.5/32 -j DROP"))

        drops = NetworkGroupManager._format_partition_drops(
            NetworkGroupManager._parse_partition_drops(iptables))

        assert drops == f" | partition: {CHAIN} out-X only [192.168.1.5]"

    @pytest.mark.parametrize(["probe_output", "unanswered"], [
        ("", [0, 1, 2]),
        (QDISC_SECTION, [1, 2]),
        (_probe(QDISC_SECTION, FILTER_SECTION), [2]),
        (_probe("", "", ""), [0, 1, 2])
    ])
    def check_incomplete_probe_degrades(self, probe_output, unanswered):
        """A node that answers partially yields a poorer overview, never an exception."""
        sections = NetworkGroupManager._split_probe_output(probe_output)

        assert len(sections) == 3, "Callers unpack three sections regardless of what came back"

        for idx in unanswered:
            assert sections[idx] == [], f"Section {idx} went unanswered, it must come back empty"

            render, neutral = NEUTRAL_RENDERINGS[idx]

            assert render(sections[idx]) == neutral, f"Section {idx} must render as its neutral form"
