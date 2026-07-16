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
Thin client data-center-aware routing.

Every thin client is configured with the addresses of ALL server nodes of BOTH DCs.
A client pinned to a data center must route partition-aware reads to nodes of its own DC:
with a large cross-DC netem delay its average read latency stays small.

The partition part verifies the thin client experience of a split-brain: a client
pinned to the main-DC half keeps writing; a client pinned to the read-only half still
reads cleanly (falling back to the reachable nodes from its address list) but gets
every write rejected; and writes resume after the heal and rejoin.

The thin client services are registered into their DC's network group, so both the
netem delay and the iptables partition apply to their traffic as well.
"""
from time import sleep

from ducktape.mark import parametrize

from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.mdc.mdc_cluster import MdcCluster, dc_jvm_opts, DC_1, DC_2, cross_dc_network, THIN_LOAD_APP
from ignitetest.services.utils.ignite_configuration import IgniteThinClientConfiguration
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH

CACHE_NAME = "mdc-thin"

KEYS = 100

PINNED_GET_ITERS = 200
PUT_ITERS = 30

OFFSET_DURING = 1_000_000
OFFSET_REJECTED_PROBES = 9_000_000
OFFSET_AFTER = 2_000_000

SPLIT_SETTLE_SECS = 15


class MdcThinClientTest(IgniteTest):
    """
    Tests for thin client DC-aware routing and behavior through a partition.
    """
    @cluster(num_nodes=7)
    @ignite_versions(str(DEV_BRANCH))
    @parametrize(cross_dc_latency_ms=100)
    def test_thin_client_dc_aware_routing_and_partition(self, ignite_version, cross_dc_latency_ms):
        """
        DC-pinned thin client reads locally (latency far below the cross-DC delay);
        through a partition the pinned clients behave like their half-ring: main half writes, read-only half reads
        but rejects writes, and writes resume after the heal.
        """
        mdc = MdcCluster(self, ignite_version, srv_per_dc=2, runners_per_dc={DC_1: 1},
                         client_connector=True)

        # All thin clients get the full address list of both DCs: DC preference must come
        # from routing, not from the address list.
        cli_dc1 = self._thin_client(mdc, dc_jvm_opts(DC_1))
        cli_dc2 = self._thin_client(mdc, dc_jvm_opts(DC_2))

        # Register the thin client hosts into the network groups, so netem and the partition apply to them.
        mdc.register(DC_1, cli_dc1)
        mdc.register(DC_2, cli_dc2)

        with cross_dc_network(self.logger, mdc, delay_ms=cross_dc_latency_ms) as net:
            mdc.start_servers()

            mdc.generate_data(DC_1, CACHE_NAME, 0, KEYS, backups=1)

            svc = mdc.run_service(cli_dc1, {"mode": "GET", "cacheName": CACHE_NAME, "keyFrom": 0,
                                            "keyTo": KEYS, "iterations": PINNED_GET_ITERS,
                                            "resultPrefix": "pinnedGet"})

            avg_cli_dc_1 = mdc.result_float(svc, "pinnedGetAvgOpMs")
            err_cnt_cli_dc_1 = mdc.result_int(svc, "pinnedGetErrCnt")

            self.logger.info(f"Thin client routing latency [delayMs={cross_dc_latency_ms}, getAvgOpMs={avg_cli_dc_1}, "
                             f"getErrCnt={err_cnt_cli_dc_1}]")

            assert avg_cli_dc_1 < cross_dc_latency_ms, \
                f"DC-pinned thin client reads should be served locally " \
                f"[avgMs={avg_cli_dc_1}, delayMs={cross_dc_latency_ms}]"

            assert err_cnt_cli_dc_1 == 0, (f"Expected 0 errors for DC-pinned thin client reads, "
                                           f"but found {err_cnt_cli_dc_1}")

            net.enable_network_partition(DC_1, DC_2)

            sleep(SPLIT_SETTLE_SECS)

            mdc.verify_split_brain()

            # The client pinned to the main half keeps writing...
            mdc.run_service(cli_dc1, {"mode": "PUT", "cacheName": CACHE_NAME, "keyFrom": OFFSET_DURING,
                                      "keyTo": OFFSET_DURING + PUT_ITERS, "iterations": PUT_ITERS,
                                      "resultPrefix": "duringPut"})

            # ...while the client pinned to the read-only half still reads cleanly
            # (its DC1 addresses are unreachable, so it falls back to DC2 nodes)...
            svc = mdc.run_service(cli_dc2, {"mode": "GET", "cacheName": CACHE_NAME, "keyFrom": 0,
                                            "keyTo": KEYS, "iterations": PINNED_GET_ITERS,
                                            "resultPrefix": "roGet"})

            assert mdc.result_int(svc, "roGetErrCnt") == 0, \
                "Thin client reads in the read-only DC must be clean"

            # ...and has every write rejected by the topology validator.
            mdc.run_service(cli_dc2, {"mode": "PUT", "cacheName": CACHE_NAME,
                                      "keyFrom": OFFSET_REJECTED_PROBES,
                                      "keyTo": OFFSET_REJECTED_PROBES + PUT_ITERS,
                                      "iterations": PUT_ITERS, "expectAdmissible": False,
                                      "resultPrefix": "roPut"})

            net.disable_network_partition(DC_1, DC_2)

            mdc.restart(DC_2)

            mdc.run_service(cli_dc2, {"mode": "PUT", "cacheName": CACHE_NAME, "keyFrom": OFFSET_AFTER,
                                      "keyTo": OFFSET_AFTER + PUT_ITERS, "iterations": PUT_ITERS,
                                      "resultPrefix": "afterPut"})

            mdc.check_data(DC_1, CACHE_NAME, OFFSET_DURING, OFFSET_DURING + PUT_ITERS)
            mdc.check_data(DC_1, CACHE_NAME, OFFSET_AFTER, OFFSET_AFTER + PUT_ITERS)

            mdc.stop_servers()

    def _thin_client(self, mdc: MdcCluster, jvm_opts) -> IgniteApplicationService:
        """
        Builds a single-node thin client application service with the addresses of all
        server nodes of both DCs.
        """
        return IgniteApplicationService(
            self.test_context,
            IgniteThinClientConfiguration(addresses=mdc.thin_client_addresses(),
                                          version=mdc.ignite_config.version),
            java_class_name=THIN_LOAD_APP,
            num_nodes=1,
            jvm_opts=jvm_opts)
