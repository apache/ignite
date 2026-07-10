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
from ducktape.mark import matrix

from ignitetest.services.mdc.mdc_cluster import MdcCluster, DC_1, DC_2, cross_dc_network
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH

CACHE_NAME = "mdc-dist"

DATA_SIZE = 1_000


class MdcDistributionTest(IgniteTest):
    """
    Tests for cross-DC partition copy placement by MdcAffinityBackupFilter.
    """
    @cluster(num_nodes=7)
    @ignite_versions(str(DEV_BRANCH))
    @matrix(backups=[1, 3], cross_dc_latency_ms=50)
    def test_copies_balanced_across_dcs(self, ignite_version, backups, cross_dc_latency_ms):
        """
        Every partition has exactly (backups + 1) / 2 OWNING copies in each DC,
        and the guarantee survives a full restart of one DC with rebalance.
        """
        mdc = MdcCluster(self, ignite_version, srv_per_dc=3, runners_per_dc={DC_1: 1, DC_2: 0})

        copies_per_dc = (backups + 1) // 2

        with cross_dc_network(self.logger, mdc, delay_ms=cross_dc_latency_ms):
            mdc.start_servers()

            mdc.generate_data(DC_1, CACHE_NAME, 0, DATA_SIZE, backups=backups)

            mdc.verify_cache_distribution(CACHE_NAME, copies_per_dc=copies_per_dc)

            mdc.restart(DC_2)

            mdc.verify_cache_distribution(CACHE_NAME, copies_per_dc=copies_per_dc)

            mdc.check_data(DC_1, CACHE_NAME, 0, DATA_SIZE)

            mdc.stop_servers()

    @cluster(num_nodes=7)
    @ignite_versions(str(DEV_BRANCH))
    @matrix(cross_dc_latency_ms=50)
    def test_copies_balanced_in_asymmetric_dcs(self, ignite_version, cross_dc_latency_ms):
        """
        DC sizes differ (2 vs 4 nodes), yet each DC still holds exactly one copy of
        every partition: balance is per-DC, not per-node.
        """
        mdc = MdcCluster(self, ignite_version, srv_per_dc={DC_1: 2, DC_2: 4},
                         runners_per_dc={DC_1: 1, DC_2: 0})

        with cross_dc_network(self.logger, mdc, delay_ms=cross_dc_latency_ms):
            mdc.start_servers()

            mdc.generate_data(DC_1, CACHE_NAME, 0, DATA_SIZE, backups=1)

            mdc.verify_cache_distribution(CACHE_NAME, copies_per_dc=1)

            mdc.check_data(DC_1, CACHE_NAME, 0, DATA_SIZE)

            mdc.stop_servers()

    @cluster(num_nodes=7)
    @ignite_versions(str(DEV_BRANCH))
    @matrix(cross_dc_latency_ms=50)
    def test_distribution_survives_node_loss(self, ignite_version, cross_dc_latency_ms):
        """
        With backups=3 (two copies per DC) losing one node of a DC leaves at least one
        copy of every partition in that DC, and after the baseline change and rebalance
        the filter restores the exact two-copies-per-DC placement on the surviving nodes.
        """
        mdc = MdcCluster(self, ignite_version, srv_per_dc=3, runners_per_dc={DC_1: 1, DC_2: 0})

        with cross_dc_network(self.logger, mdc, delay_ms=cross_dc_latency_ms):
            mdc.start_servers()

            mdc.generate_data(DC_1, CACHE_NAME, 0, DATA_SIZE, backups=3)

            mdc.verify_cache_distribution(CACHE_NAME, copies_per_dc=2)

            mdc.servers[DC_2].stop_node(mdc.servers[DC_2].nodes[-1])

            # Immediately after the loss the surviving copies must still cover both DCs
            # (at least one OWNING copy of every partition per DC).
            mdc.verify_cache_distribution(CACHE_NAME)

            mdc.check_data(DC_1, CACHE_NAME, 0, DATA_SIZE)

            # Adopt the new topology as baseline and let rebalance restore the full
            # two-copies-per-DC placement on the remaining 3 + 2 nodes.
            control = mdc.control(DC_1)

            control.set_baseline(control.cluster_state().topology_version)

            mdc.servers[DC_1].await_rebalance()
            mdc.servers[DC_2].await_rebalance()

            mdc.verify_cache_distribution(CACHE_NAME, copies_per_dc=2)

            mdc.check_data(DC_1, CACHE_NAME, 0, DATA_SIZE)

            mdc.stop_servers()
