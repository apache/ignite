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

from contract_values import contract_values, supports_values


CORE = "modules/core/pom.xml"
CORE_POM = """<project xmlns="http://maven.apache.org/POM/4.0.0">
    <parent><groupId>org.apache.ignite</groupId><artifactId>ignite-parent-internal</artifactId></parent>
    <artifactId>ignite-core</artifactId>
    <version>${revision}</version>
    <dependencies><dependency><artifactId>cache-api</artifactId></dependency></dependencies>
    <!-- <artifactId>documentation example</artifactId> -->
</project>"""


class ContractValuesTest(unittest.TestCase):
    def test_coordinates_can_use_the_parent_group(self):
        """The core module declares its own artifactId and inherits its group's literal name."""
        expected = {"Maven groupId": "org.apache.ignite", "Maven artifactId": "ignite-core"}
        self.assertEqual(contract_values(CORE, CORE_POM), expected)
        edited = CORE_POM.replace("ignite-core", "ignite-runtime")
        self.assertEqual(contract_values(CORE, edited), {**expected, "Maven artifactId": "ignite-runtime"})

    def test_unrelated_changes_preserve_the_coordinates(self):
        """Updating a version, dependency, or comment leaves both published coordinates unchanged."""
        edited = (CORE_POM.replace("${revision}", "2.19.0").replace("cache-api", "cache-api-next")
                  .replace("documentation example", "other-example"))
        self.assertEqual(contract_values(CORE, edited), contract_values(CORE, CORE_POM))

    def test_explicit_group_overrides_the_parent(self):
        """An explicit group wins over its parent even when its value cannot be resolved."""
        edited = CORE_POM.replace("<artifactId>ignite-core", "<groupId>org.new</groupId><artifactId>ignite-core")
        self.assertEqual(contract_values(CORE, edited)["Maven groupId"], "org.new")
        self.assertEqual(contract_values(CORE, edited.replace("org.new", "${ignite.group}")),
                         {"Maven artifactId": "ignite-core"})

    def test_incomplete_documents_and_unrelated_paths_produce_no_values(self):
        """A missing model or an unrelated file provides no evidence of an artifact rename."""
        self.assertTrue(supports_values("modules/bom/pom.xml"))
        self.assertFalse(supports_values("examples/pom.xml"))
        self.assertEqual(contract_values("examples/pom.xml", CORE_POM), {})
        for source in ("<project", "<not-a-project><artifactId>x</artifactId></not-a-project>", "<project/>"):
            with self.subTest(source=source):
                self.assertEqual(contract_values(CORE, source), {})
