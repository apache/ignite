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
MDC with three data centers: only a segment that sees a majority of them accepts writes.
"""
from time import sleep

from ducktape.mark import parametrize

from ignitetest.services.mdc.mdc_cluster import MdcCluster, cross_dc_network, DCS_3, DC_1
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH

CACHE_NAME = "mdc-majority"

KEYS_PER_DC = 100

# Time for discovery to detect the partition and for every segment to complete PME.
SPLIT_SETTLE_SECS = 20


class MdcMajorityPartitionTest(IgniteTest):
    """
    Tests majority based MDC topology validation across three data centers.
    """
    @cluster(num_nodes=9)
    @ignite_versions(str(DEV_BRANCH))
    # DC1 is the main DC of a two DC cluster; with three DCs it must have no such role.
    @parametrize(cross_dc_latency_ms=100, isolated_dc=DC_1)
    def test_minority_dc_isolation(self, ignite_version, cross_dc_latency_ms, isolated_dc):
        """
        Cuts one DC off from the other two: they keep accepting writes, the cut DC rejects them,
        and every DC still reads all keys. Then heals the network and restarts the cut DC.
        """
        mdc = MdcCluster(self, ignite_version, dcs=DCS_3, srv_per_dc=2, runners_per_dc=1,
                         network_timeout=20_000, discovery_network_timeout=20_000)

        majority = [dc for dc in mdc.dcs if dc != isolated_dc]

        total_keys = len(mdc.dcs) * KEYS_PER_DC

        with cross_dc_network(self.logger, mdc, delay_ms=cross_dc_latency_ms) as net:
            mdc.start_servers()

            for idx, dc in enumerate(mdc.dcs):
                mdc.generate_data(dc, CACHE_NAME, idx * KEYS_PER_DC, (idx + 1) * KEYS_PER_DC)

            mdc.verify_cache_distribution(CACHE_NAME, copies_per_dc=1)

            for dc in majority:
                net.enable_network_partition(isolated_dc, dc)

            sleep(SPLIT_SETTLE_SECS)

            mdc.verify_split_brain(majority, isolated_dc)

            # Every DC holds a copy of every partition, so both segments read all keys.
            for dc in mdc.dcs:
                mdc.check_data(dc, CACHE_NAME, 0, total_keys)

            for dc in majority:
                mdc.check_put_admissibility(dc, CACHE_NAME, True)

            mdc.check_put_admissibility(isolated_dc, CACHE_NAME, False)

            for dc in majority:
                net.disable_network_partition(isolated_dc, dc)

            # A split cluster does not merge back by itself: the cut DC rejoins by restarting.
            mdc.restart(isolated_dc)

            mdc.verify_whole_cluster_healthy()

            for dc in mdc.dcs:
                mdc.check_put_admissibility(dc, CACHE_NAME, True)

            mdc.check_data(DC_1, CACHE_NAME, 0, total_keys)

            mdc.verify_cache_distribution(CACHE_NAME, copies_per_dc=1)

            mdc.control().idle_verify(CACHE_NAME)

            mdc.verify_servers_log_clean()

            mdc.stop_servers()
