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

from ducktape.tests.test import Test


from ignitetest.services.ignite import IgniteService


class IgniteTest(Test):
    """
    Helper class that manages setting up a Ignite cluster. Use this if the
    default settings for Ignite are sufficient for your test; any customization
    needs to be done manually. Your run() method should call tearDown and
    setUp. The Ignite service are available as the fields IgniteTest.ignite.
    """
    def __init__(self, test_context):
        super(IgniteTest, self).__init__(test_context)

        self.ignite = IgniteService(test_context, self.num_brokers)

    def setUp(self):
        self.ignite.start()