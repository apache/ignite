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

# Small seed so the cross-DC cache distribution can be verified before the split.
SEED_KEYS = 100

# Fresh, disjoint key range for the continuous insert load: the load advances the key
# on every success, so [LOAD_KEY_FROM, LOAD_KEY_FROM + successfulInserts) gets inserted.
LOAD_KEY_FROM = 1_000_000
LOAD_KEY_TO = 100_000_000

# Let the load accumulate successful inserts before the DCs are cut apart.
LOAD_WARMUP_SECS = 5

# Time for discovery to detect the partition and for both half-rings to complete PME
# and account for the split.
SPLIT_SETTLE_SECS = 15


class MdcTransactionalPartitionTest(IgniteTest):
    """
    Transactional load resilience to a cross-DC network partition.
    """
    @cluster(num_nodes=6)
    @ignite_versions(str(DEV_BRANCH))
    @parametrize(cross_dc_latency_ms=20)
    def test_transactional_load_cut_on_partition(self, ignite_version, cross_dc_latency_ms):
        """
        Continuous implicit-transaction insert load from the main DC is cut off by the first
        cache exception the cross-DC partition triggers; afterwards no transaction is left
        hanging on either half-ring and the server logs are clean.
        """
        mdc = MdcCluster(self, ignite_version, srv_per_dc=2,
                         runners_per_dc={DC_1: 1}, loaders_per_dc={DC_1: 1})

        with cross_dc_network(self.logger, mdc, delay_ms=cross_dc_latency_ms) as net:
            mdc.start_servers()

            # Create the TRANSACTIONAL cache and seed it, so every partition owns one copy
            # per DC - the reason an implicit-transaction write must touch both DCs.
            mdc.generate_data(DC_1, CACHE_NAME, 0, SEED_KEYS, backups=BACKUPS, atomicity="TRANSACTIONAL")

            mdc.verify_cache_distribution(CACHE_NAME, copies_per_dc=1)

            # Continuous single-threaded implicit-transaction insert load from the main DC.
            # It stops on the very first exception (stopOnError) instead of failing the app,
            # recording how many inserts had succeeded up to that point.
            mdc.start_loader(DC_1, {
                "mode": "PUT",
                "cacheName": CACHE_NAME,
                "keyFrom": LOAD_KEY_FROM,
                "keyTo": LOAD_KEY_TO,
                "stopOnError": True,
                "resultPrefix": "txLoad"
            })

            sleep(LOAD_WARMUP_SECS)

            net.enable_network_partition(DC_1, DC_2)

            # The load hits its first exception here and cuts itself off; give discovery and
            # PME time to detect the split and settle into two independent half-rings.
            sleep(SPLIT_SETTLE_SECS)

            mdc.verify_split_brain()

            # The load has already finished on its own - this just collects its results.
            svc = mdc.stop_loader(DC_1)

            inserts = mdc.result_int(svc, "txLoadOpsCnt")
            stopped_on_error = svc.extract_result("txLoadStoppedOnError")

            self.logger.info(f"Transactional load cut by the partition "
                             f"[insertsBeforeSplit={inserts}, stoppedOnError={stopped_on_error}]")

            assert inserts > 0, "The transactional load performed no successful inserts before the split"

            assert stopped_on_error == "true", \
                "The transactional load must be cut off by the partition's first cache exception " \
                f"[stoppedOnError={stopped_on_error}, inserts={inserts}]"

            # The point of the scenario: the aborted implicit transactions leave nothing
            # hanging on either half-ring...
            mdc.verify_no_hanging_txs(DC_1)
            mdc.verify_no_hanging_txs(DC_2)

            # ...and neither half-ring logged a hung PME, a long running transaction or a
            # lost partition.
            mdc.verify_servers_log_clean()

            mdc.stop_servers()
