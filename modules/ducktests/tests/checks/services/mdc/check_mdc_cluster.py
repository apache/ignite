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
Checks the DC-count dependent parts of the MDC fixture.

The number of data centers decides which MdcTopologyValidator mode the caches are
configured for, how many backups spread one copy per DC, and which links a partition has
to cut. All of that is compiled without a cluster, so it is checked without one.
"""

import pytest

from ignitetest.services.mdc.mdc_cluster import MdcCluster, mdc_topology_params, min_backups, all_pairs, \
    isolation_pairs, cross_dc_network, per_dc, CACHE_TOP_VALIDATOR_GLOBAL, DCS_2, DCS_3, DC_1, DC_2, DC_3

DELAY_MS = 100

DFLT_DELAY = f"{DELAY_MS}ms"


class FakeMdcCluster:
    """
    The two members cross_dc_network() reads off an MdcCluster.
    """
    def __init__(self, dcs):
        self.dcs = dcs

    def network_registry(self):
        """Every DC group is non-empty; the services themselves are irrelevant here."""
        return {dc: [] for dc in self.dcs}


class CheckMdcTopologyParams:
    """
    Checks the cache parameters that select the topology validator mode.
    """
    def check_even_dc_count_uses_a_main_dc(self):
        """An even DC set is validated against a main DC, the first one by default."""
        assert mdc_topology_params(DCS_2) == {"dcsNum": 2, "mainDc": DC_1}

        assert mdc_topology_params(DCS_2, main_dc=DC_2) == {"dcsNum": 2, "mainDc": DC_2}

    def check_odd_dc_count_uses_the_dc_set(self):
        """An odd DC set is validated by majority, so it carries the DC set instead."""
        assert mdc_topology_params(DCS_3) == {"dcsNum": 3, "datacenters": [DC_1, DC_2, DC_3]}

    def check_the_two_modes_are_never_mixed(self):
        """
        MdcTopologyValidator.checkConfiguration() rejects a main DC alongside an odd DC
        set, so a main DC must not leak into the majority mode even when one is asked for.
        """
        params = mdc_topology_params(DCS_3, main_dc=DC_1)

        assert "mainDc" not in params, "A main DC alongside an odd DC set fails cache startup"

    @pytest.mark.parametrize(["dcs", "expected"], [(DCS_2, 1), (DCS_3, 2)])
    def check_min_backups_gives_one_copy_per_dc(self, dcs, expected):
        """(backups + 1) must divide by the DC count - the MdcAffinityBackupFilter contract."""
        assert min_backups(dcs) == expected

        assert (min_backups(dcs) + 1) % len(dcs) == 0


class CheckMdcNetworkLayout:
    """
    Checks the DC pairings a partition is expressed in, and the impairment mesh.
    """
    def check_all_pairs_covers_the_mesh(self):
        """Every cross-DC link appears exactly once, in a stable order."""
        assert all_pairs(DCS_2) == [(DC_1, DC_2)]

        assert all_pairs(DCS_3) == [(DC_1, DC_2), (DC_1, DC_3), (DC_2, DC_3)]

    def check_isolation_pairs_cut_one_dc_only(self):
        """Isolating a DC cuts its own links and leaves the rest of the mesh intact."""
        cut = isolation_pairs(DC_3, DCS_3)

        assert cut == [(DC_3, DC_1), (DC_3, DC_2)]

        assert (DC_1, DC_2) not in [tuple(sorted(pair)) for pair in cut], \
            "The DCs left behind must keep seeing each other"

    def check_symmetric_impairment_reaches_every_pair(self):
        """One delay argument impairs the whole mesh, not just the first pair."""
        net = cross_dc_network(None, FakeMdcCluster(DCS_3), delay_ms=DELAY_MS)

        for dc_a, dc_b in all_pairs(DCS_3):
            cfg = net.network_group_store.get_config(dc_a, dc_b)

            assert cfg is not None and cfg.delay == DFLT_DELAY, f"{dc_a} -> {dc_b} is unimpaired"

            assert net.network_group_store.get_config(dc_b, dc_a) == cfg, "Impairments are bidirectional"

    def check_no_impairment_leaves_the_store_empty(self):
        """Without delay or loss the manager still owns partitions, but deploys no netem."""
        net = cross_dc_network(None, FakeMdcCluster(DCS_3))

        assert net.network_group_store.matrix == {}


class CheckMdcPerDcCounts:
    """
    Checks how the per-DC service counts are spread over the DC set.
    """
    def check_a_scalar_count_covers_every_dc(self):
        """One number means that number of nodes in every DC the cluster spans."""
        assert per_dc(2, DCS_3) == {DC_1: 2, DC_2: 2, DC_3: 2}

    def check_a_dict_count_is_taken_as_is(self):
        """An asymmetric layout names only the DCs it populates."""
        assert per_dc({DC_1: 3}, DCS_3) == {DC_1: 3}

    def check_a_dict_naming_a_foreign_dc_is_rejected(self):
        """
        A DC outside the cluster's own set is skipped by network_registry(), so its nodes
        would run with no impairments and no partition rules - and nothing else would say
        so. It has to fail where it is declared.
        """
        with pytest.raises(AssertionError, match=DC_3):
            per_dc({DC_1: 1, DC_3: 1}, DCS_2)


def _fixture(dcs, top_validator=True):
    """
    An MdcCluster carrying only what _with_cache_params() reads - no services are built, so
    no ducktape cluster is needed. Bypassing the constructor is the point: it pins down how
    little of the fixture the cache parameter compilation actually depends on.
    """
    mdc = MdcCluster.__new__(MdcCluster)

    mdc.dcs = tuple(dcs)
    mdc.main_dc = dcs[0]
    mdc.cache_defaults = {"topologyValidator": top_validator}

    return mdc


class CheckMdcCacheParams:
    """
    Checks the single point every cache of an MDC test is configured from.
    """
    def check_an_app_that_creates_the_cache_is_handed_the_dc_set(self):
        """A cache created by a scenario must agree with the DC set the cluster spans."""
        params = _fixture(DCS_3)._with_cache_params({"cacheName": "c", "createCache": True})

        assert params["dcsNum"] == 3

        assert params["datacenters"] == [DC_1, DC_2, DC_3]

        assert params["topologyValidator"] is True

    def check_an_app_that_only_uses_the_cache_is_handed_nothing(self):
        """Cache parameters an application would only ignore must not reach it at all."""
        params = {"cacheName": "c", "mode": "GET"}

        assert _fixture(DCS_3)._with_cache_params(params) == params

    def check_an_app_that_always_creates_the_cache_needs_no_flag(self):
        """The generator carries no createCache parameter, so its call site says so instead."""
        params = _fixture(DCS_2)._with_cache_params({"cacheName": "c"}, creates_cache=True)

        assert params["mainDc"] == DC_1

    def check_an_explicit_parameter_wins(self):
        """A scenario stays able to override what the fixture injects."""
        params = _fixture(DCS_2)._with_cache_params({"createCache": True, "mainDc": DC_2})

        assert params["mainDc"] == DC_2

    def check_the_global_reaches_the_cache(self):
        """
        The global is only ever read into cache_defaults, so this covers the whole path from
        --global-json to the application parameters. Its name is part of the README.
        """
        assert CACHE_TOP_VALIDATOR_GLOBAL == "mdc_cache_topology_validator"

        params = _fixture(DCS_3, top_validator=False)._with_cache_params({"createCache": True})

        assert params["topologyValidator"] is False
