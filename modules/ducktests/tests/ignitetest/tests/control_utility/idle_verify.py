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
This module contains checks for idle_verify.
"""

from ignitetest.services.ignite import IgniteService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.tests.util import preload_data, DataGenerationParams
from ignitetest.utils import ignite_versions, cluster
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, LATEST, IgniteVersion

class IdleVerifyTest(IgniteTest):
    @cluster(num_nodes=3)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    # @ignite_versions(str(DEV_BRANCH))
    def test_dump_file_(self, ignite_version):
        ignite = IgniteService(self.test_context,
                               IgniteConfiguration(version=IgniteVersion(ignite_version)),
                               num_nodes=2)
        ignite.start()

        preload_data(
            self.test_context,
            ignite.config._replace(client_mode=True, discovery_spi=from_ignite_cluster(ignite)),
            data_gen_params=DataGenerationParams(backups=1, cache_count=2, entry_count=100,
                                                 entry_size=100, preloaders=1)
        )

        dump_filename = ControlUtility(ignite).idle_verify_dump(ignite.nodes[1])

        ignite.nodes[1].account.ssh(f"mv {dump_filename} {ignite.log_dir}")
