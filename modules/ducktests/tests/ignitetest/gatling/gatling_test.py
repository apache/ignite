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
This module contains basic ignite test.
"""
import os

from ducktape.mark.resource import cluster
from ducktape.tests.test import TestContext
from ignitetest.gatling.gatling_service import GatlingService
from ignitetest.services.ignite import IgniteService
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.utils import ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion


# pylint: disable=W0223
class GatlingTest(IgniteTest):
    """
    Basic Gatling test.
    """

    # Simulation class parameter.
    GLOBAL_SIMULATION_CLASS = "gatling_simulation_class"

    # Server nodes count parameter.
    GLOBAL_SERVER_NODES_COUNT = "server_nodes_count"

    # Gatling nodes count parameter.
    GLOBAL_GATLING_NODES_COUNT = "gatling_nodes_count"

    @cluster(num_nodes=12)
    @ignite_versions(str(DEV_BRANCH))
    def gatling_test(self, ignite_version):
        configuration = IgniteConfiguration(version=IgniteVersion(ignite_version))

        servers = IgniteService(self.test_context, configuration, startup_timeout_sec=180,
                                num_nodes=self._global_int(self.GLOBAL_SERVER_NODES_COUNT))

        servers.start()

        gatling = GatlingService(self.test_context, configuration, self._global_param(self.GLOBAL_SIMULATION_CLASS),
                                 num_nodes=self._global_int(self.GLOBAL_GATLING_NODES_COUNT))

        gatling.run()

        servers.stop()
