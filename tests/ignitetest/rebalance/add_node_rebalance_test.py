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

from ignitetest.tests.ignite_test import IgniteTest


class AddNodeRebalanceTest(IgniteTest):
    """
    Test performs rebalance tests.
    """
    def __init__(self, test_context):
        super(IgniteTest, self).__init__(test_context)

    def teardown(self):
        self.ignite.stop()

    def test_add_node(self):
        """
        Test performs add node rebalance test which consists of following steps:
            * Start cluster.
            * Put data to it via CacheDataProducer.
            * Start one more node.
            * Await for rebalance to finish.
        """
        for node in self.ignite.nodes:
            node.account.ssh("touch /opt/hello-from-test.txt")
