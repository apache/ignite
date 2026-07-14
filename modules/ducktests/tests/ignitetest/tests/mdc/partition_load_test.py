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
Continuous cache/tx/SQL load through a network partition.

The availability claims verified here:

* GET load (cache API) runs in BOTH data centers from before the partition, through the
  partition and its healing, in a single application run - so any exception crossing a
  phase boundary fails the test on loader stop. Per-window throughput sampling and the
  max-stall metric additionally show that availability never dropped for long.
* PUT / transactional / SQL DML load succeeds in the main-DC half in every phase, is
  rejected (and only rejected) in the read-only half during the partition, and resumes
  everywhere after healing.
* Everything that was acknowledged as written is readable afterwards from the OPPOSITE
  data center ("as much as we succeeded to put, we get back").
* Negative invariants: no long running transactions, no PME freeze, no lost partitions,
  no hanging transactions, and idle_verify finds no conflicts after the recovery.
"""
from time import sleep

from ducktape.mark import matrix

from ignitetest.services.mdc.mdc_cluster import MdcCluster, cross_dc_network, DC_1, DC_2
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH

ATOMIC_CACHE = "mdc-atomic"
TX_CACHE = "mdc-tx"
SQL_CACHE = "mdc-sql"

# Pre-loaded data set, written half from each DC.
GEN_SIZE = 1_000

# Non-intersecting key offsets for the write bursts of each phase.
OFFSET_BEFORE_DC1 = 1_000_000
OFFSET_BEFORE_DC2 = 1_500_000
OFFSET_DURING_DC1 = 2_000_000
OFFSET_AFTER_DC1 = 3_000_000
OFFSET_AFTER_DC2 = 3_500_000

# Probe keys of the expected-to-be-rejected bursts (never intersect readable data).
OFFSET_REJECTED_PROBES = 9_000_000

PUT_BURST = 200
TX_BURST = 60
SQL_BURST = 100
GET_BURST = 500
REJECTED_BURST = 30

SPLIT_SETTLE_SECS = 10

# Boundary tolerance for the background load: a transient error window is possible at
# the instant the partition is enabled/healed, but must stay marginal.
BG_MAX_ERR_RATIO = 0.01
BG_MAX_STALL_MS = 60_000


class MdcPartitionLoadTest(IgniteTest):
    """
    Tests for continuous load through an MDC network partition.
    """
    @cluster(num_nodes=10)
    @ignite_versions(str(DEV_BRANCH))
    @matrix(cross_dc_latency_ms=[20])
    def test_load_through_partition(self, ignite_version, cross_dc_latency_ms):
        """
        Background gets in both DCs across the whole scenario; put/tx/sql bursts before,
        during and after the partition; cross-DC readback of every acknowledged write.
        """
        mdc = MdcCluster(self, ignite_version, srv_per_dc=3, runners_per_dc=1, loaders_per_dc=1)

        with cross_dc_network(self.logger, mdc, delay_ms=cross_dc_latency_ms) as net:
            mdc.start_servers()

            mdc.generate_data(DC_1, ATOMIC_CACHE, 0, GEN_SIZE // 2, backups=1)
            mdc.generate_data(DC_2, ATOMIC_CACHE, GEN_SIZE // 2, GEN_SIZE, backups=1)

            # Background gets: a single run per DC spanning every phase. Any exception
            # surfaces on stop_loader(); transient boundary errors are counted, not thrown.
            for dc in (DC_1, DC_2):
                mdc.start_loader(dc, {"mode": "GET", "cacheName": ATOMIC_CACHE,
                                      "keyFrom": 0, "keyTo": GEN_SIZE,
                                      "tolerateErrors": True, "opPauseMs": 5,
                                      "resultPrefix": f"bg{dc}"})

            # written: (cache, kind, written_from_dc, offset, count)
            written = []

            # ---- Phase 1: before the partition, both DCs accept writes.
            written += self._admissible_write_bursts(mdc, DC_1, "before", OFFSET_BEFORE_DC1)
            written.append(self._admissible_put_burst(mdc, DC_2, "before", OFFSET_BEFORE_DC2))

            before_sql = next(entry for entry in written if entry[0] == SQL_CACHE)

            net.enable_network_partition(DC_1, DC_2)

            sleep(SPLIT_SETTLE_SECS)

            mdc.verify_split_brain()

            # ---- Phase 2: during the partition.
            # The main-DC half keeps accepting the full write load.
            written += self._admissible_write_bursts(mdc, DC_1, "during", OFFSET_DURING_DC1)

            # The read-only half serves reads cleanly - cache API and SQL...
            svc = mdc.run_load(DC_2, "GET", ATOMIC_CACHE, "duringDC2Get",
                               keyFrom=0, keyTo=GEN_SIZE, iterations=GET_BURST)

            assert mdc.result_int(svc, "duringDC2GetErrCnt") == 0, "Reads in the read-only DC must be clean"

            svc = mdc.run_load(DC_2, "SQL_SELECT", SQL_CACHE, "duringDC2Sql",
                               keyFrom=before_sql[3], keyTo=before_sql[3] + before_sql[4],
                               iterations=before_sql[4])

            assert mdc.result_int(svc, "duringDC2SqlErrCnt") == 0, "SQL reads in the read-only DC must be clean"

            # ...and rejects every write - plain, transactional and SQL DML.
            mdc.run_load(DC_2, "PUT", ATOMIC_CACHE, "duringDC2Put", keyFrom=OFFSET_REJECTED_PROBES,
                         iterations=REJECTED_BURST, expectAdmissible=False)

            mdc.run_load(DC_2, "TX_PUT", TX_CACHE, "duringDC2Tx", keyFrom=OFFSET_REJECTED_PROBES,
                         iterations=REJECTED_BURST, expectAdmissible=False)

            mdc.run_load(DC_2, "SQL_PUT", SQL_CACHE, "duringDC2SqlPut", keyFrom=OFFSET_REJECTED_PROBES,
                         iterations=REJECTED_BURST, expectAdmissible=False)

            # ---- Phase 3: heal, rejoin the read-only half, writes resume everywhere.
            net.disable_network_partition(DC_1, DC_2)

            mdc.restart(DC_2)

            written += self._admissible_write_bursts(mdc, DC_1, "after", OFFSET_AFTER_DC1)
            written.append(self._admissible_put_burst(mdc, DC_2, "after", OFFSET_AFTER_DC2))

            # ---- Background loads: no exception on stop, availability never dropped.
            for dc in (DC_1, DC_2):
                svc = mdc.stop_loader(dc)

                ops = mdc.result_int(svc, f"bg{dc}OpsCnt")
                errs = mdc.result_int(svc, f"bg{dc}ErrCnt")
                max_stall = mdc.result_int(svc, f"bg{dc}MaxStallMs")
                min_window = mdc.result_int(svc, f"bg{dc}MinWindowOps")

                self.logger.info(f"Background GET load [dc={dc}, ops={ops}, errs={errs}, "
                                 f"maxStallMs={max_stall}, minWindowOps={min_window}]")

                assert ops > 0, f"Background get load performed no operations [dc={dc}]"

                assert errs == 0, f"Background get load errors exceed the boundary tolerance [dc={dc}, ops={ops}, errs={errs}]"

                assert max_stall < BG_MAX_STALL_MS, f"Background get load stalled for too long [dc={dc}, maxStallMs={max_stall}]"

            # ---- Readback: every acknowledged write is readable from the OPPOSITE DC.
            for idx, (cache, kind, src_dc, offset, cnt) in enumerate(written):
                assert cnt > 0, f"Write burst acknowledged nothing [cache={cache}, offset={offset}]"

                dst_dc = DC_2 if src_dc == DC_1 else DC_1

                if kind == "sql":
                    svc = mdc.run_load(dst_dc, "SQL_SELECT", cache, f"rb{idx}{dst_dc}",
                                       keyFrom=offset, keyTo=offset + cnt, iterations=cnt)

                    assert mdc.result_int(svc, f"rb{idx}{dst_dc}ErrCnt") == 0, \
                        f"SQL readback failed [cache={cache}, offset={offset}, cnt={cnt}, dc={dst_dc}]"
                else:
                    mdc.check_data(dst_dc, cache, offset, offset + cnt)

            # ---- Negative invariants.
            mdc.verify_no_hanging_txs()

            mdc.control(DC_1).idle_verify(",".join([ATOMIC_CACHE, TX_CACHE, SQL_CACHE]))

            mdc.verify_servers_log_clean()

            mdc.stop_servers()

    @cluster(num_nodes=8)
    @ignite_versions(str(DEV_BRANCH))
    @matrix(cross_dc_latency_ms=[20], cross_dc_loss=[0.1])
    def test_degraded_link_without_partition(self, ignite_version, cross_dc_latency_ms, cross_dc_loss):
        """
        A slow, lossy WAN that stays connected ("slow WAN" as opposed to "broken WAN"):
        the cluster must NOT split, and cache, transactional and SQL load must complete
        cleanly in both DCs (slower, but without a single error) - TCP absorbs the loss.
        """
        mdc = MdcCluster(self, ignite_version, srv_per_dc=3, runners_per_dc=1)

        with cross_dc_network(self.logger, mdc, delay_ms=cross_dc_latency_ms, loss=cross_dc_loss):
            mdc.start_servers()

            mdc.generate_data(DC_1, ATOMIC_CACHE, 0, GEN_SIZE // 2, backups=1)
            mdc.generate_data(DC_2, ATOMIC_CACHE, GEN_SIZE // 2, GEN_SIZE, backups=1)

            for dc, offset in ((DC_1, OFFSET_BEFORE_DC1), (DC_2, OFFSET_BEFORE_DC2)):
                svc = mdc.run_load(dc, "GET", ATOMIC_CACHE, f"deg{dc}Get",
                                   keyFrom=0, keyTo=GEN_SIZE, iterations=GET_BURST)

                assert mdc.result_int(svc, f"deg{dc}GetErrCnt") == 0, f"Degraded-link reads must be clean [dc={dc}]"

                self._admissible_put_burst(mdc, dc, "deg", offset)

            self._admissible_burst(mdc, DC_1, "TX_PUT", TX_CACHE, "degDC1Tx", OFFSET_DURING_DC1, TX_BURST,
                                   atomicity="TRANSACTIONAL")

            mdc.verify_whole_cluster_healthy()

            mdc.verify_cache_distribution(ATOMIC_CACHE, copies_per_dc=1)

            mdc.control(DC_1).idle_verify(",".join([ATOMIC_CACHE, TX_CACHE]))

            mdc.verify_servers_log_clean()

            mdc.stop_servers()

    def _admissible_write_bursts(self, mdc: MdcCluster, dc: str, phase: str, offset: int):
        """
        Runs the full trio of write bursts (cache API put, transactional put, SQL DML)
        expected to fully succeed, and returns the written ranges for later readback.
        """
        return [
            self._admissible_burst(mdc, dc, "PUT", ATOMIC_CACHE, f"{phase}{dc}Put", offset, PUT_BURST),
            self._admissible_burst(mdc, dc, "TX_PUT", TX_CACHE, f"{phase}{dc}Tx", offset, TX_BURST,
                                   atomicity="TRANSACTIONAL"),
            self._admissible_burst(mdc, dc, "SQL_PUT", SQL_CACHE, f"{phase}{dc}Sql", offset, SQL_BURST),
        ]

    def _admissible_put_burst(self, mdc: MdcCluster, dc: str, phase: str, offset: int):
        """
        Runs a single cache API put burst expected to fully succeed.
        """
        return self._admissible_burst(mdc, dc, "PUT", ATOMIC_CACHE, f"{phase}{dc}Put", offset, PUT_BURST)

    @staticmethod
    def _admissible_burst(mdc: MdcCluster, dc: str, mode: str, cache: str, prefix: str,
                          offset: int, iterations: int, **cache_params):
        """
        Runs one write burst that must succeed completely: the load application fails fast
        on any exception, and the acknowledged count must match the requested iterations.

        :return: (cache, kind, dc, offset, count) tuple for the readback stage.
        """
        svc = mdc.run_load(dc, mode, cache, prefix, keyFrom=offset, iterations=iterations,
                           createCache=True, backups=1, mainDc=DC_1, **cache_params)

        ops = mdc.result_int(svc, f"{prefix}OpsCnt")

        assert ops == iterations, \
            f"Write burst is incomplete [dc={dc}, mode={mode}, cache={cache}, exp={iterations}, ops={ops}]"

        kind = "sql" if mode.startswith("SQL") else "data"

        return cache, kind, dc, offset, ops
