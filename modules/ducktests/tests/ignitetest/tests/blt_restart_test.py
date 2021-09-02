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
This module contains client tests
"""
import time

from ignitetest.services.ignite import IgniteService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.utils import ignite_versions, cluster
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import V_2_8_0, IgniteVersion, V_2_7_6


# pylint: disable=W0223
class BaselineTopologyTest(IgniteTest):
    @cluster(num_nodes=3)
    @ignite_versions(str(V_2_7_6))
    def testRestoreHistory(self, ignite_version):
        """
        Test that IgniteService correctly start and stop
        """
        ignite = IgniteService(self.test_context,
                               IgniteConfiguration(
                                   version=IgniteVersion(ignite_version),
                                   data_storage=DataStorageConfiguration(
                                       default=DataRegionConfiguration(persistent=True))
                               ),
                               num_nodes=3)
        print(self.test_context)

        ignite.start()

        control_utility = ControlUtility(ignite)

        control_utility.activate()

        cnt = 0

        while cnt < 10:
            self.logger.warn("Count: " + str(cnt))

            for node in ignite.nodes:
                self.logger.warn("restart node: " + node.name)
                ignite.stop_node(node, True)

                while ignite.alive(node):
                    self.logger.warn("sleep: " + node.name)
                    time.sleep(1)

                control_utility.remove_from_baseline([node])

                ignite.start_node(node)
                ignite.await_event_on_node("Topology snapshot", node, 120, from_the_beginning=True)

                control_utility.add_to_baseline([node])

            cnt=+1

        control_utility.idle_verify()

        ignite.stop()
