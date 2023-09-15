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
# limitations under the License
import time

from ducktape.mark import parametrize
from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.cdc.ignite_cdc import IgniteCdcUtility
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.tests.client_test import check_topology
from ignitetest.utils import cluster, ignite_versions, ignore_if
from ignitetest.utils.bean import Bean
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import LATEST, IgniteVersion, DEV_BRANCH, V_2_14_0


class CdcTest(IgniteTest):
    """
    CDC tests.
    """
    CACHE_NAME = "cdc-test-cache"
    JAVA_CLIENT_CLASS_NAME = "org.apache.ignite.internal.ducktest.tests.client_test.IgniteCachePutClient"

    @cluster(num_nodes=5)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @ignore_if(lambda version, _: version <= V_2_14_0)
    @parametrize(num_nodes=5, wal_force_archive_timeout=100, pacing=10, duration_sec=10)
    def test_cdc_start_stop(self, ignite_version, num_nodes, wal_force_archive_timeout, pacing, duration_sec):
        """
        Test for starting and stopping the ignite-cdc.sh
        """
        cdc_configuration = Bean("org.apache.ignite.cdc.CdcConfiguration",
                                 consumer=Bean("org.apache.ignite.internal.ducktest.tests.cdc.CountingCdcConsumer"))

        config = IgniteConfiguration(
            version=IgniteVersion(ignite_version),
            data_storage=DataStorageConfiguration(
                wal_force_archive_timeout=wal_force_archive_timeout,
                cdc_wal_path="cdc-custom-wal-dir",
                default=DataRegionConfiguration(
                    persistence_enabled=True,
                    cdc_enabled=True
                )
            ),
            ext_beans=[('bean.j2', cdc_configuration)]
        )

        ignite = IgniteService(self.test_context, config=config, num_nodes=num_nodes-2)
        ignite.start()

        control_sh = ControlUtility(cluster=ignite)
        control_sh.activate()

        ignite_cdc = IgniteCdcUtility(ignite)
        ignite_cdc.start()

        ignite.await_event("CountingCdcConsumer started", timeout_sec=60, from_the_beginning=True,
                           log_file="ignite-cdc.log")

        client_cfg = config._replace(client_mode=True)
        client = IgniteApplicationService(self.test_context, client_cfg,
                                          java_class_name=self.JAVA_CLIENT_CLASS_NAME,
                                          num_nodes=2,
                                          params={"cacheName": self.CACHE_NAME, "pacing": pacing})

        client.start()

        check_topology(control_sh, num_nodes)

        ignite.await_event("Consumed", timeout_sec=duration_sec, from_the_beginning=True,
                           log_file="ignite-cdc.log")

        time.sleep(duration_sec)

        client.stop()

        ignite_cdc.stop()

        ignite.await_event("Stopping Change Data Capture service instance",
                           timeout_sec=120, from_the_beginning=True,
                           log_file="ignite-cdc.log")

        ignite.await_event("CountingCdcConsumer stopped",
                           timeout_sec=1, from_the_beginning=True,
                           log_file="ignite-cdc.log")

        ignite.stop()
