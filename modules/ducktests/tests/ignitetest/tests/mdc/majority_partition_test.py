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
Three data center MDC resilience: majority based topology validation.

With an ODD number of data centers MdcTopologyValidator switches from "the main DC must
be visible" to "a majority of the DC set must be visible". That is a different guarantee
and not a bigger version of the two DC one:

  - no data center is privileged - the cluster keeps accepting writes through the loss of
    ANY single DC, including the first one, which two DC mode cannot do;
  - a segment holding a minority of DCs goes read-only even though it is perfectly
    healthy internally - so a three way split leaves NO writable segment at all.

Every partition owns exactly one copy per DC (backups = 2), so every segment - down to a
single isolated DC - can still serve every read. That is what separates the read
assertions from the write ones throughout these tests.
"""
from time import sleep

from ducktape.mark import parametrize

from ignitetest.services.mdc.mdc_cluster import MdcCluster, cross_dc_network, all_pairs, isolation_pairs, \
    DCS_3, DC_1, DC_2, DC_3
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH

CACHE_NAME = "mdc-majority"

# Keys generated from each DC; the whole data set is [0, KEYS_PER_DC * len(DCS_3)).
KEYS_PER_DC = 100

# Time for discovery to detect the partition and for every segment to complete PME.
SPLIT_SETTLE_SECS = 20

# Probe key ranges of the put admissibility checks. Well above the data set, and one range
# per check, so that the writes an admissible check performs never collide.
PROBE_BASE = 1_000_000
PROBE_STRIDE = 1_000_000


def _probe_offset(idx: int) -> int:
    return PROBE_BASE + idx * PROBE_STRIDE


class MdcMajorityPartitionTest(IgniteTest):
    """
    Tests for majority based MDC topology validation across three data centers.
    """
    @cluster(num_nodes=9)
    @ignite_versions(str(DEV_BRANCH))
    @parametrize(cross_dc_latency_ms=100, isolated_dc=DC_3)
    @parametrize(cross_dc_latency_ms=100, isolated_dc=DC_1)
    def test_minority_dc_isolation(self, ignite_version, cross_dc_latency_ms, isolated_dc):
        """
        One DC is cut off from the two others. The majority segment must stay writable and
        the isolated one must go read-only while still serving every read.

        Parametrized over the isolated DC to assert that no DC is privileged: isolating DC1
        - the main DC of the equivalent two DC cluster, and the DC the others discovered
        through - is just as survivable as isolating DC3.
        """
        mdc = MdcCluster(self, ignite_version, dcs=DCS_3, srv_per_dc=2, runners_per_dc=1,
                         network_timeout=20_000, tcp_connect_timeout=10_000)

        majority = tuple(dc for dc in mdc.dcs if dc != isolated_dc)

        cut = isolation_pairs(isolated_dc, mdc.dcs)

        with cross_dc_network(self.logger, mdc, delay_ms=cross_dc_latency_ms) as net:
            mdc.start_servers()

            total_keys = self._generate_data(mdc)

            mdc.verify_cache_distribution(CACHE_NAME, copies_per_dc=1)

            # Both cuts land in the same SSH round-trip: rolling them out one after the
            # other would briefly present the cluster with a two segment topology it would
            # legitimately react to, which is not the scenario under test.
            net.enable_network_partitions(*cut)

            sleep(SPLIT_SETTLE_SECS)

            mdc.verify_segments(majority, isolated_dc)

            # One copy of every partition lives in every DC, so both segments read everything.
            for dc in mdc.dcs:
                mdc.check_data(dc, CACHE_NAME, 0, total_keys)

            # Two DCs out of three are a majority - the segment keeps writing...
            for idx, dc in enumerate(majority):
                mdc.check_put_admissibility(dc, CACHE_NAME, True, key_offset=_probe_offset(idx))

            # ...while the isolated DC sees one DC out of three and is rejected by the validator.
            mdc.check_put_admissibility(isolated_dc, CACHE_NAME, False, key_offset=_probe_offset(2))

            net.disable_network_partitions(*cut)

            # The isolated DC may be the one the others discovered through, in which case the
            # shared ip finder holds only its own addresses and a restart would seed it off
            # itself. Point discovery at every DC so it rejoins the majority ring either way.
            mdc.sync_service_discovery()

            # Split-brain does not self-heal: the read-only segment rejoins via restart.
            mdc.restart(isolated_dc)

            self._verify_recovered(mdc, total_keys, probe_idx=3)

            mdc.stop_servers()

    @cluster(num_nodes=9)
    @ignite_versions(str(DEV_BRANCH))
    @parametrize(cross_dc_latency_ms=100)
    def test_three_way_split_blocks_all_writes(self, ignite_version, cross_dc_latency_ms):
        """
        Every cross-DC link drops at once, leaving three single-DC segments. No segment
        holds a majority, so - unlike the two DC case, where one half always survives as
        writable - the whole cluster goes read-only until the links come back.
        """
        mdc = MdcCluster(self, ignite_version, dcs=DCS_3, srv_per_dc=2, runners_per_dc=1,
                         network_timeout=20_000, tcp_connect_timeout=10_000)

        mesh = all_pairs(mdc.dcs)

        with cross_dc_network(self.logger, mdc, delay_ms=cross_dc_latency_ms) as net:
            mdc.start_servers()

            total_keys = self._generate_data(mdc)

            mdc.verify_cache_distribution(CACHE_NAME, copies_per_dc=1)

            net.enable_network_partitions(*mesh)

            sleep(SPLIT_SETTLE_SECS)

            # Every DC ended up on its own, each a healthy single-DC segment.
            mdc.verify_split_brain()

            for idx, dc in enumerate(mdc.dcs):
                mdc.check_data(dc, CACHE_NAME, 0, total_keys)

                mdc.check_put_admissibility(dc, CACHE_NAME, False, key_offset=_probe_offset(idx))

            net.disable_network_partitions(*mesh)

            # The first DC keeps the ring, every other segment rejoins it via restart.
            for dc in mdc.dcs[1:]:
                mdc.restart(dc)

            self._verify_recovered(mdc, total_keys, probe_idx=3)

            mdc.stop_servers()

    @cluster(num_nodes=9)
    @ignite_versions(str(DEV_BRANCH))
    def test_writes_survive_single_dc_loss(self, ignite_version):
        """
        Data centers go down one by one, no network impairments involved. Losing the first
        one leaves a majority and writes continue; losing the second one drops the survivor
        into a minority and it goes read-only; both returning restores writes everywhere.
        """
        mdc = MdcCluster(self, ignite_version, dcs=DCS_3, srv_per_dc=2, runners_per_dc=1)

        with cross_dc_network(self.logger, mdc):
            mdc.start_servers()

            total_keys = self._generate_data(mdc)

            mdc.verify_cache_distribution(CACHE_NAME, copies_per_dc=1)

            mdc.servers[DC_3].stop()

            # Two DCs out of three remain visible - still a majority, still writable.
            mdc.verify_segment_healthy((DC_1, DC_2))

            mdc.check_data(DC_1, CACHE_NAME, 0, total_keys)

            mdc.check_put_admissibility(DC_1, CACHE_NAME, True, key_offset=_probe_offset(0))
            mdc.check_put_admissibility(DC_2, CACHE_NAME, True, key_offset=_probe_offset(1))

            mdc.servers[DC_2].stop()

            # One DC out of three is a minority: everything is still readable...
            mdc.check_data(DC_1, CACHE_NAME, 0, total_keys)

            # ...but the validator rejects every write.
            mdc.check_put_admissibility(DC_1, CACHE_NAME, False, key_offset=_probe_offset(2))

            # DC1 never left, so the shared ip finder still points at a live ring.
            for dc in (DC_2, DC_3):
                mdc.servers[dc].start(clean=False)

            for dc in (DC_2, DC_3):
                mdc.servers[dc].await_rebalance()

            self._verify_recovered(mdc, total_keys, probe_idx=3)

            mdc.stop_servers()

    @staticmethod
    def _generate_data(mdc: MdcCluster) -> int:
        """
        Populates the cache from every DC in turn, each with its own key range.

        :return: The size of the whole data set, i.e. the exclusive upper key bound.
        """
        for idx, dc in enumerate(mdc.dcs):
            mdc.generate_data(dc, CACHE_NAME, idx * KEYS_PER_DC, (idx + 1) * KEYS_PER_DC)

        return len(mdc.dcs) * KEYS_PER_DC

    @staticmethod
    def _verify_recovered(mdc: MdcCluster, total_keys: int, probe_idx: int):
        """
        Verifies that the cluster is whole again: one ACTIVE cluster spanning every DC,
        writes accepted everywhere, the whole data set intact and evenly spread, and no
        segment left a suspicious trace in its logs.
        """
        mdc.verify_whole_cluster_healthy()

        for idx, dc in enumerate(mdc.dcs):
            mdc.check_put_admissibility(dc, CACHE_NAME, True, key_offset=_probe_offset(probe_idx + idx))

        mdc.check_data(mdc.dcs[0], CACHE_NAME, 0, total_keys)

        mdc.verify_cache_distribution(CACHE_NAME, copies_per_dc=1)

        mdc.control().idle_verify(CACHE_NAME)

        mdc.verify_servers_log_clean()
