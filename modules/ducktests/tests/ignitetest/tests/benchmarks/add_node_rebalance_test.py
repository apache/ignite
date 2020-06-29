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
import time

from ducktape.mark import parametrize
from ducktape.mark.resource import cluster

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.tests.utils.ignite_test import IgniteTest
from ignitetest.version import DEV_BRANCH, IgniteVersion, V_2_8_0


class AddNodeRebalanceTest(IgniteTest):
    NUM_NODES = 4
    PRELOAD_TIMEOUT = 60
    DATA_AMOUNT = 1000000
    REBALANCE_TIMEOUT = 60

    """
    Test performs rebalance tests.
    """

    @staticmethod
    def properties(client_mode="false"):
        return """
            <property name="clientMode" value="{client_mode}"/>
        """.format(client_mode=client_mode)

    def __init__(self, test_context):
        super(AddNodeRebalanceTest, self).__init__(test_context=test_context)

    def setUp(self):
        pass

    def teardown(self):
        pass

    @cluster(num_nodes=NUM_NODES + 1)
    @parametrize(version=str(DEV_BRANCH))
    @parametrize(version=str(V_2_8_0))
    def test_add_node(self, version):
        """
        Test performs add node rebalance test which consists of following steps:
            * Start cluster.
            * Put data to it via IgniteClientApp.
            * Start one more node and awaits for rebalance to finish.
        """
        ignite_version = IgniteVersion(version)

        self.stage("Start Ignite nodes")

        ignites = IgniteService(self.test_context, num_nodes=AddNodeRebalanceTest.NUM_NODES - 1, version=ignite_version)

        ignites.start()

        self.stage("Starting DataGenerationApplication")

        # This client just put some data to the cache.
        IgniteApplicationService(self.test_context,
                                 java_class_name="org.apache.ignite.internal.ducktest.DataGenerationApplication",
                                 properties=self.properties(client_mode="true"),
                                 version=ignite_version,
                                 params="test-cache,%d" % self.DATA_AMOUNT,
                                 timeout_sec=self.PRELOAD_TIMEOUT).run()

        ignite = IgniteService(self.test_context, num_nodes=1, version=ignite_version)

        self.stage("Starting Ignite node")

        ignite.start()

        start = time.time()

        ignite.await_event("rebalanced=true, wasRebalanced=false",
                           timeout_sec=AddNodeRebalanceTest.REBALANCE_TIMEOUT,
                           from_the_beginning=True,
                           backoff_sec=1)

        data = {"Rebalanced in (sec)": time.time() - start}

        return data
