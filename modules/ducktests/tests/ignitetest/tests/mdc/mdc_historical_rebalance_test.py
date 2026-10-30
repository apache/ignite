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
MDC historical rebalance test: a returning data center must restore its partitions
from local disk (only the delta written during its absence), not receive a full
rebalance from the surviving DC.
"""
import re
from time import sleep

from ignitetest.services.mdc.mdc_cluster import MdcCluster, cross_dc_network, DC_1, DC_2
from ignitetest.services.utils.ignite_aware import IgniteAwareService
from ignitetest.services.utils.ignite_configuration.data_storage import DataStorageConfiguration, \
    DataRegionConfiguration
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH

CACHE_NAME = "mdc-historical-rebalance"

BACKUPS = 1

BASE_ENTRIES = 1024
DELTA_ENTRIES = 256

JVM_OPTS = ["-Xmx384M", "-Xms384M", "-DIGNITE_PREFER_WAL_REBALANCE=true"]


def _persistence_config() -> DataStorageConfiguration:
    """
    Data storage configuration with persistence enabled on a small default data region.
    Short checkpoint frequency ensures data reaches disk before DC failure.
    """
    return DataStorageConfiguration(
        checkpoint_frequency=10,
        default=DataRegionConfiguration(persistence_enabled=True,
                                        initial_size=16 * 1024 * 1024,
                                        max_size=64 * 1024 * 1024)
    )


def _rebalance_metric(svc, metric: str) -> int:
    """
    Extracts a numeric field from the last 'Completed (final) rebalancing' log line
    of any node in the service.
    """
    for node in svc.nodes:
        cmd = f"grep 'Completed (final) rebalancing' {node.log_file} | tail -1"
        output = IgniteAwareService.exec_command(node, cmd)

        if output:
            m = re.search(rf"{metric}=(\d+)", output)

            if m:
                return int(m.group(1))

    raise AssertionError(f"Rebalance metric '{metric}' not found in {svc} logs")


class MdcHistoricalRebalanceTest(IgniteTest):
    """
    Verifies that a data center returning after a full stop (DC failure) restores
    its partitions from local disk and only receives the delta written during its
    absence, rather than a full rebalance from the surviving DC.
    """
    @cluster(num_nodes=4)
    @ignite_versions(str(DEV_BRANCH))
    def test_returning_dc_recovers_from_disk(self, ignite_version):
        """
        DC1 + DC2 with persistence enabled. Data is written, then DC2 goes down
        (full stop, not a network partition). While DC2 is away, DC1 accepts new
        writes (the delta). DC2 returns with clean=False (disk data preserved) and
        must recover: all base data from disk, only the delta via rebalance.
        """
        mdc = MdcCluster(self, ignite_version, srv_per_dc=1, runners_per_dc=1,
                         data_storage=_persistence_config(),
                         jvm_opts=JVM_OPTS)

        with cross_dc_network(self.logger, mdc):
            mdc.start_servers()

            # With persistence the cluster starts INACTIVE; activate it explicitly.
            mdc.control(DC_1).activate()

            # Phase 1: populate base data across both DCs.
            mdc.generate_data(DC_1, CACHE_NAME, 0, BASE_ENTRIES // 2, backups=BACKUPS)
            mdc.generate_data(DC_2, CACHE_NAME, BASE_ENTRIES // 2, BASE_ENTRIES, backups=BACKUPS)

            mdc.check_data(DC_1, CACHE_NAME, 0, BASE_ENTRIES)
            mdc.check_data(DC_2, CACHE_NAME, 0, BASE_ENTRIES)

            mdc.verify_cache_distribution(CACHE_NAME, copies_per_dc=1)

            # Allow checkpoint to flush data to disk before the DC failure.
            sleep(15)

            # Phase 2: DC2 goes down (full stop, simulating DC failure).
            mdc.servers[DC_2].stop()

            # DC1 must remain healthy and accept writes.
            mdc.check_put_admissibility(DC_1, CACHE_NAME, True)

            # Phase 3: write the delta on the surviving DC1.
            mdc.generate_data(DC_1, CACHE_NAME, BASE_ENTRIES, BASE_ENTRIES + DELTA_ENTRIES,
                              backups=BACKUPS)

            mdc.check_data(DC_1, CACHE_NAME, 0, BASE_ENTRIES + DELTA_ENTRIES)

            # Phase 4: DC2 returns (disk data preserved, clean=False).
            mdc.servers[DC_2].start(clean=False)
            mdc.servers[DC_2].await_rebalance()

            # Phase 5: verify historical rebalance actually restored data from disk.
            hist_entries = _rebalance_metric(mdc.servers[DC_2], "histEntries")

            self.logger.info(f"Historical rebalance: histEntries={hist_entries}")

            assert hist_entries > 0, \
                f"Expected data restored from local disk [histEntries={hist_entries}]"

            # Phase 6: verify full data integrity on both DCs.
            mdc.check_data(DC_1, CACHE_NAME, 0, BASE_ENTRIES + DELTA_ENTRIES)
            mdc.check_data(DC_2, CACHE_NAME, 0, BASE_ENTRIES + DELTA_ENTRIES)

            mdc.verify_cache_distribution(CACHE_NAME, copies_per_dc=1)

            mdc.verify_servers_log_clean()

            mdc.stop_servers()
