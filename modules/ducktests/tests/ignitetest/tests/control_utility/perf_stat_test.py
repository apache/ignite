
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
This module contains performance statistics manipulation test through control utility.
"""

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion


class PerfStatTest(IgniteTest):
    @cluster(num_nodes=2)
    @ignite_versions(str(DEV_BRANCH))
    def test_perf_stat(self, ignite_version):
        """
        Test that performance statistics collecting can be started and stopped, current status can be requested
        and performance log files can be rotated both on server and client nodes.
        """
        server = IgniteService(
            self.test_context,
            IgniteConfiguration(version=IgniteVersion(ignite_version)),
            num_nodes=1
        )

        client = IgniteApplicationService(
            self.test_context,
            server.config._replace(client_mode=True),
            java_class_name="org.apache.ignite.internal.ducktest.tests.ContinuousDataLoadApplication",
            params={
                "cacheName": "test-cache",
                "range": 200_000,
                "warmUpRange":  100_000,
                "transactional": False
            }
        )

        server.start()

        control_sh = ControlUtility(server)

        assert not control_sh.is_performance_statistics_enabled()

        control_sh.start_performance_statistics()

        assert control_sh.is_performance_statistics_enabled()

        client.start()

        control_sh.rotate_performance_statistics()

        client.stop()

        assert control_sh.is_performance_statistics_enabled()

        control_sh.stop_performance_statistics()

        assert not control_sh.is_performance_statistics_enabled()

        server.stop()

        self.check_prf_files_are_created_on(server)

        self.check_prf_files_are_created_on(client)

    def check_prf_files_are_created_on(self, service):
        for n in service.nodes:
            files = n.account.sftp_client.listdir(service.perf_stat_dir)

            self.logger.debug(files)

            assert len(files) == 2

            assert all([filename.endswith(".prf") for filename in files])
