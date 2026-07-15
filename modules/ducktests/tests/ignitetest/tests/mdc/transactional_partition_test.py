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
MDC transactional load through a cross-DC network partition.

A TRANSACTIONAL cache spans both data centers: with backups=1 and the
MdcAffinityBackupFilter every partition owns exactly one copy per DC, so every
implicit-transaction write (a plain put on a transactional cache) must reach a
node in the other DC. A continuous single-threaded insert load runs from the main
DC; the instant the DCs are partitioned the next commit cannot reach all partition
copies and fails with a cache exception. The load cuts itself off on that first
exception and records how many inserts had succeeded.

After the split settles the test asserts that the cluster really split-brained,
that the load stopped because of the partition (not because it ran out of work),
and - the point of the scenario - that the aborted implicit transactions left
nothing hanging on either half-ring and no suspicious entries in the server logs.

Data accessibility during the split is deliberately NOT checked: a transactional
cache needs all partition copies available, which a split-brained half-ring cannot
offer.
"""
from time import sleep

from ducktape.mark import parametrize

from ignitetest.services.mdc.mdc_cluster import MdcCluster, cross_dc_network, DC_1, DC_2
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH

CACHE_NAME = "mdc-tx-load"

BACKUPS = 1

# Fresh, disjoint key range for the continuous insert load: the load advances the key
# on every success, so [LOAD_KEY_FROM, LOAD_KEY_FROM + successfulInserts) gets inserted.
LOAD_KEY_FROM_DC_1 = 1_000_000
LOAD_KEY_TO_DC_1 = 10_000_000

LOAD_KEY_FROM_DC_2 = 10_000_000
LOAD_KEY_TO_DC_2 = 20_000_000

# Let the load accumulate successful inserts before the DCs are cut apart.
LOAD_WARMUP_SECS = 15

# Time for discovery to detect the partition and for both half-rings to complete PME
# and account for the split.
SPLIT_SETTLE_SECS = 15


class MdcTransactionalPartitionTest(IgniteTest):
    """
    Transactional load resilience to a cross-DC network partition.
    """
    @cluster(num_nodes=8)
    @ignite_versions(str(DEV_BRANCH))
    @parametrize(cross_dc_latency_ms=100)
    def test_transactional_load_cut_on_partition(self, ignite_version, cross_dc_latency_ms):
        """
        Continuous implicit-transaction insert load from the main DC is cut off by the first
        cache exception the cross-DC partition triggers; afterwards no transaction is left
        hanging on either half-ring and the server logs are clean.
        """
        mdc = MdcCluster(self, ignite_version, srv_per_dc=2, runners_per_dc=1, loaders_per_dc=1,
                         network_timeout=20_000, tcp_connect_timeout=10_000)

        with cross_dc_network(self.logger, mdc, delay_ms=cross_dc_latency_ms) as net:
            mdc.start_servers()

            # Continuous single-threaded implicit-transaction insert load from the main DC.
            # It stops on the very first exception (stopOnError) instead of failing the app,
            # recording how many inserts had succeeded up to that point.
            for dc, offset_from, to in [(DC_1, LOAD_KEY_FROM_DC_1, LOAD_KEY_TO_DC_1),
                                        (DC_2, LOAD_KEY_FROM_DC_2, LOAD_KEY_TO_DC_2)]:
                mdc.start_loader(dc, {
                    "mode": "TX_PUT",
                    "cacheName": CACHE_NAME,
                    "keyFrom": offset_from,
                    "keyTo": to,
                    "stopOnError": True,
                    "resultPrefix": f"txLoad{dc}",
                    "createCache": True,
                    "backups": BACKUPS,
                    "atomicity": "TRANSACTIONAL",
                    "mainDc": DC_1,
                })

            sleep(LOAD_WARMUP_SECS)

            net.enable_network_partition(DC_1, DC_2)

            # The load hits its first exception here and cuts itself off; give discovery and
            # PME time to detect the split and settle into two independent half-rings.
            sleep(SPLIT_SETTLE_SECS)

            mdc.verify_split_brain()

            # The load has already finished on its own - this just collects its results.
            for dc in [DC_1, DC_2]:
                svc = mdc.stop_loader(dc)

                inserts = mdc.result_int(svc, f"txLoad{dc}OpsCnt")
                errors = mdc.result_int(svc, f"txLoad{dc}ErrCnt")

                self.logger.info(f"Transactional load cut by the partition "
                                 f"[txLoad{dc}OpsCnt={inserts}, txLoad{dc}ErrCnt={errors}]")

                assert inserts > 0, "The transactional load performed no successful inserts"

            # The point of the scenario: the aborted implicit transactions leave nothing
            # hanging on either half-ring...
            mdc.verify_no_hanging_txs(DC_1)
            mdc.verify_no_hanging_txs(DC_2)

            # ...and neither half-ring logged a hung PME, a long running transaction or a
            # lost partition.
            mdc.verify_servers_log_clean()

            net.disable_network_partition(DC_1, DC_2)

            mdc.restart(DC_2)

            mdc.check_put_admissibility(DC_1, CACHE_NAME, True, key_offset=100_000_000)
            mdc.check_put_admissibility(DC_2, CACHE_NAME, True, key_offset=101_000_000)

            mdc.verify_cache_distribution(CACHE_NAME, copies_per_dc=1)

            mdc.control(DC_1).idle_verify(CACHE_NAME)

            mdc.verify_servers_log_clean()

            mdc.stop_servers()
