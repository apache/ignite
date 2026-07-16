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
MDC cluster resilience to network and data center failures.

Covers the full split-brain lifecycle (partition -> active half + read-only half ->
heal -> read-only half rejoins via restart), the loss and return of the main DC,
and short network blips that must NOT split the cluster.
"""
from time import sleep

from ducktape.mark import parametrize

from ignitetest.services.mdc.mdc_cluster import MdcCluster, cross_dc_network, DC_1, DC_2
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH

CACHE_NAME = "mdc-resilience"

BACKUPS = 1

# Time for discovery to detect the partition and for both half-rings to complete PME.
SPLIT_SETTLE_SECS = 10

# A blip must stay well below the failure detection timeout (10s by default).
BLIP_SECS = 1
BLIP_SETTLE_SECS = 5
BLIPS = 10

BG_MAX_STALL_MS = 500


class MdcPartitionResilienceTest(IgniteTest):
    """
    Tests for cluster network partition resilience in MultiDC.
    """
    @cluster(num_nodes=12)
    @ignite_versions(str(DEV_BRANCH))
    @parametrize(cross_dc_latency_ms=100)
    def test_mdc_cluster_partition_resilience(self, ignite_version, cross_dc_latency_ms):
        """
        The canonical split-brain lifecycle: partition -> split into two healthy half-rings
        (DC1 active, DC2 read-only) -> all data readable everywhere -> heal -> DC2 rejoins
        via restart -> writes restored everywhere, distribution and consistency verified.
        """
        mdc = MdcCluster(self, ignite_version, srv_per_dc=5, runners_per_dc=1,
                         network_timeout=20_000, tcp_connect_timeout=10_000)

        with cross_dc_network(self.logger, mdc, delay_ms=cross_dc_latency_ms) as net:
            mdc.start_servers()

            mdc.generate_data(DC_1, CACHE_NAME, 0, 100, backups=BACKUPS)
            mdc.generate_data(DC_2, CACHE_NAME, 100, 200, backups=BACKUPS)

            mdc.verify_cache_distribution(CACHE_NAME, copies_per_dc=1)

            net.enable_network_partition(DC_1, DC_2)

            sleep(SPLIT_SETTLE_SECS)

            mdc.verify_split_brain()

            # All data written before the split is readable in both halves.
            mdc.check_data(DC_1, CACHE_NAME, 0, 200)
            mdc.check_data(DC_2, CACHE_NAME, 0, 200)

            # The half-ring holding the main DC accepts writes, the other one is read-only.
            mdc.check_put_admissibility(DC_1, CACHE_NAME, True)
            mdc.check_put_admissibility(DC_2, CACHE_NAME, False)

            net.disable_network_partition(DC_1, DC_2)

            # Split-brain does not self-heal: the read-only half rejoins via restart.
            mdc.restart(DC_2)

            mdc.check_put_admissibility(DC_1, CACHE_NAME, True, key_offset=2_000_000)
            mdc.check_put_admissibility(DC_2, CACHE_NAME, True, key_offset=3_000_000)

            mdc.verify_cache_distribution(CACHE_NAME, copies_per_dc=1)

            mdc.control(DC_1).idle_verify(CACHE_NAME)

            mdc.verify_servers_log_clean()

            mdc.stop_servers()

    @cluster(num_nodes=6)
    @ignite_versions(str(DEV_BRANCH))
    @parametrize(cross_dc_latency_ms=100)
    def test_main_dc_loss_and_return(self, ignite_version, cross_dc_latency_ms):
        """
        The inverse of the canonical scenario: the MAIN data center goes down entirely.
        The surviving DC must stay readable but reject writes (the topology validator does
        not see the main DC), and writes must resume in both DCs once the main DC returns.
        No network impairments are involved - the main DC is stopped, not partitioned.
        """
        mdc = MdcCluster(self, ignite_version, srv_per_dc=2, runners_per_dc=1)

        with cross_dc_network(self.logger, mdc, delay_ms=cross_dc_latency_ms):
            mdc.start_servers()

            mdc.generate_data(DC_1, CACHE_NAME, 0, 100, backups=BACKUPS)
            mdc.generate_data(DC_2, CACHE_NAME, 100, 200, backups=BACKUPS)

            mdc.verify_cache_distribution(CACHE_NAME, copies_per_dc=1)

            mdc.servers[DC_1].stop()

            state = mdc.control(DC_2).cluster_state()

            assert "ACTIVE" == state.state, f"Surviving DC should remain ACTIVE [actual={state.state}]"

            # One copy of every partition lives in DC2, so all data stays readable...
            mdc.check_data(DC_2, CACHE_NAME, 0, 200)

            # ...but the surviving DC is read-only while the main DC is invisible.
            mdc.check_put_admissibility(DC_2, CACHE_NAME, False)

            # The shared ip finder memoized only DC1's (first started) addresses, so a
            # restarted DC1 would seed off itself and form a separate cluster. Point
            # discovery at both DCs so it rejoins through the surviving DC2.
            mdc.sync_service_discovery()

            mdc.servers[DC_1].start(clean=False)

            mdc.servers[DC_1].await_rebalance()

            mdc.check_put_admissibility(DC_1, CACHE_NAME, True, key_offset=2_000_000)
            mdc.check_put_admissibility(DC_2, CACHE_NAME, True, key_offset=3_000_000)

            mdc.verify_cache_distribution(CACHE_NAME, copies_per_dc=1)

            mdc.control(DC_1).idle_verify(CACHE_NAME)

            mdc.verify_servers_log_clean()

            mdc.stop_servers()

    @cluster(num_nodes=10)
    @ignite_versions(str(DEV_BRANCH))
    @parametrize(cross_dc_latency_ms=100)
    def test_short_partition_blips_do_not_split(self, ignite_version, cross_dc_latency_ms):
        """
        A flapping WAN link: several short (below the failure detection timeout) full
        cross-DC connectivity drops. The cluster must NOT split: after the blips it is
        still one ACTIVE cluster, all data is intact and both DCs accept writes.
        """
        mdc = MdcCluster(self, ignite_version, srv_per_dc=3, runners_per_dc=1, loaders_per_dc=1)

        with cross_dc_network(self.logger, mdc, delay_ms=cross_dc_latency_ms) as net:
            mdc.start_servers()

            mdc.generate_data(DC_1, CACHE_NAME, 0, 100, backups=BACKUPS)
            mdc.generate_data(DC_2, CACHE_NAME, 100, 200, backups=BACKUPS)

            for dc in (DC_1, DC_2):
                mdc.start_loader(dc, {"mode": "GET", "cacheName": CACHE_NAME,
                                      "keyFrom": 0, "keyTo": 200,
                                      "tolerateErrors": True, "opPauseMs": 5,
                                      "resultPrefix": f"bg{dc}"})

            for i in range(BLIPS):
                self.logger.info(f"Network blip {i + 1}/{BLIPS}")

                net.enable_network_partition(DC_1, DC_2)

                sleep(BLIP_SECS)

                net.disable_network_partition(DC_1, DC_2)

                sleep(BLIP_SETTLE_SECS)

            mdc.verify_whole_cluster_healthy()

            for dc in (DC_1, DC_2):
                svc = mdc.stop_loader(dc)

                ops = mdc.result_int(svc, f"bg{dc}OpsCnt")
                errs = mdc.result_int(svc, f"bg{dc}ErrCnt")
                max_stall = mdc.result_int(svc, f"bg{dc}MaxStallMs")

                self.logger.info(f"Background GET load [dc={dc}, ops={ops}, errs={errs}, maxStallMs={max_stall}]")

                assert ops > 0, f"Background get load performed no operations [dc={dc}]"
                assert errs == 0, \
                    f"Background get load errors exceed the boundary tolerance [dc={dc}, ops={ops}, errs={errs}]"
                assert max_stall < BG_MAX_STALL_MS, \
                    f"Background get load stalled for too long [dc={dc}, maxStallMs={max_stall}]"

            mdc.check_data(DC_1, CACHE_NAME, 0, 200)
            mdc.check_data(DC_2, CACHE_NAME, 0, 200)

            mdc.check_put_admissibility(DC_1, CACHE_NAME, True, key_offset=2_000_000)
            mdc.check_put_admissibility(DC_2, CACHE_NAME, True, key_offset=3_000_000)

            mdc.control(DC_1).idle_verify(CACHE_NAME)

            mdc.verify_servers_log_clean()

            mdc.stop_servers()
