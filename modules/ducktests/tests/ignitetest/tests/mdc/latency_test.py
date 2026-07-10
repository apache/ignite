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
Crude cross-DC latency effects.

With a large netem delay between the DCs the average synchronous operation latency
(measured inside the load application as wall time / operations) becomes a reliable
proxy for "did the operation cross the DC boundary":

* GET with ``readFromBackup=true`` (MDC-aware local reads): every partition has a copy
  in the client's DC, so reads are served locally and the average stays far below the
  configured delay - i.e. read latency is NOT affected by the cross-DC link.
* GET with ``readFromBackup=false``: roughly half the keys have a remote primary, each
  such read pays the full cross-DC round trip, so the average grows well above the
  local case.
* PUT with ``FULL_SYNC``: unavoidably pays the cross-DC price - the write is acknowledged
  only after the backup in the other DC responds, so the average exceeds the one-way delay.

All thresholds are deliberately loose (2x+ margins): this is a demonstration of the
effect, not a benchmark.
"""
from ducktape.mark import matrix

from ignitetest.services.mdc.mdc_cluster import MdcCluster, cross_dc_network, DC_1
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH

CACHE_LOCAL_READS = "mdc-lat-rfb"
CACHE_REMOTE_READS = "mdc-lat-norfb"

KEYS = 2048

LOCAL_GET_ITERS = 300
REMOTE_GET_ITERS = 60
PUT_ITERS = 30

PUT_OFFSET = 1_000_000


class MdcLatencyTest(IgniteTest):
    """
    Tests for cross-DC latency effects on cache reads and writes.
    """
    @cluster(num_nodes=6)
    @ignite_versions(str(DEV_BRANCH))
    @matrix(cross_dc_latency_ms=[50])
    def test_cross_dc_latency_effects(self, ignite_version, cross_dc_latency_ms):
        """
        Local reads are unaffected by the cross-DC delay, non-local reads and FULL_SYNC
        writes pay it.
        """
        mdc = MdcCluster(self, ignite_version, srv_per_dc=2, runners_per_dc={DC_1: 1})

        with cross_dc_network(self.logger, mdc, delay_ms=cross_dc_latency_ms):
            mdc.start_servers()

            # Both caches: backups=1 (a copy of every partition in each DC), FULL_SYNC.
            # They differ only in readFromBackup - the switch enabling MDC-local reads.
            mdc.generate_data(DC_1, CACHE_LOCAL_READS, 0, KEYS, backups=1)
            mdc.generate_data(DC_1, CACHE_REMOTE_READS, 0, KEYS, backups=1, readFromBackup=False)

            svc = mdc.run_load(DC_1, "GET", CACHE_LOCAL_READS, "latLocalGet",
                               keyFrom=0, keyTo=KEYS, iterations=LOCAL_GET_ITERS)

            avg_local_get = mdc.result_float(svc, "latLocalGetAvgOpMs")
            max_local_get = mdc.result_float(svc, "latLocalGetMaxOpMs")

            svc = mdc.run_load(DC_1, "GET", CACHE_REMOTE_READS, "latRemoteGet",
                               keyFrom=0, keyTo=KEYS, iterations=REMOTE_GET_ITERS)

            avg_remote_get = mdc.result_float(svc, "latRemoteGetAvgOpMs")
            max_remote_get = mdc.result_float(svc, "latRemoteGetMaxOpMs")

            svc = mdc.run_load(DC_1, "PUT", CACHE_LOCAL_READS, "latPut",
                               keyFrom=PUT_OFFSET, iterations=PUT_ITERS)

            avg_put = mdc.result_float(svc, "latPutAvgOpMs")
            min_put = mdc.result_float(svc, "latPutMinOpMs")

            self.logger.info(f"Cross-DC latency effects [delayMs={cross_dc_latency_ms}, "
                             f"avgLocalGetMs={avg_local_get}, avgRemoteGetMs={avg_remote_get}, "
                             f"avgPutMs={avg_put}]")

            # Local reads: served by the same-DC copy, so the cross-DC delay never applies.
            assert avg_local_get < cross_dc_latency_ms * 0.1 and max_local_get < cross_dc_latency_ms, \
                f"MDC-aware local reads should not pay the cross-DC latency " \
                f"[avgMs={avg_local_get}, maxMs={max_local_get}, delayMs={cross_dc_latency_ms}]"

            assert avg_remote_get > cross_dc_latency_ms * 0.9 and max_remote_get > cross_dc_latency_ms, \
                f"Reads without readFromBackup should pay the cross-DC latency " \
                f"[avgMs={avg_remote_get}, maxMs={max_remote_get}, delayMs={cross_dc_latency_ms}]"

            # FULL_SYNC writes: the backup in the other DC must acknowledge each update,
            # so a synchronous put cannot possibly complete under the one-way delay.
            assert avg_put > 2 * cross_dc_latency_ms and min_put > 2 * cross_dc_latency_ms, \
                f"FULL_SYNC writes should pay the cross-DC latency " \
                f"[avgMs={avg_put}, minMs={min_put}, delayMs={cross_dc_latency_ms}]"

            mdc.stop_servers()
