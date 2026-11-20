# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest

from contract_paths import java_contract_source


class ContractPathsTest(unittest.TestCase):
    def test_only_supported_java_sources_are_parsed(self):
        core = "modules/core/src/main/java/org/apache/ignite/"
        self.assertTrue(java_contract_source(core + "IgniteCache.java"))
        self.assertTrue(java_contract_source(core + "internal/processors/odbc/SqlStateCode.java"))
        for path in ("rfcs/TEMPLATE.md", "modules/core/pom.xml", core + "internal/IgniteKernal.java",
                     core + "spi/discovery/tcp/messages/TcpDiscoveryPingRequest.java",
                     "modules/core/src/test/java/org/apache/ignite/CacheTest.java"):
            with self.subTest(path=path):
                self.assertFalse(java_contract_source(path))
