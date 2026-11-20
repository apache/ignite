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

from rfc_scope import CONFIG, METRICS, STORAGE, contract_review


class ContractPathsTest(unittest.TestCase):
    def test_internal_contract_definitions_get_specific_review_guidance(self):
        core = "modules/core/src/main/java/org/apache/ignite/internal/"
        for path, expected in (
            ("jdbc/thin/ConnectionPropertiesImpl.java", CONFIG),
            ("processors/metric/GridMetricManager.java", METRICS),
            ("processors/cache/persistence/metastorage/MetastoreDataPageIO.java", STORAGE),
        ):
            with self.subTest(path=path):
                self.assertEqual(contract_review(core + path), expected)

    def test_paths_with_external_contracts(self):
        """Edits to these APIs, wire formats, stored data and distribution files need review."""
        core = "modules/core/src/main/java/org/apache/ignite/"
        for path in (
            core + "IgniteCache.java",
            core + "IgniteSystemProperties.java",
            core + "spi/discovery/tcp/TcpDiscoverySpi.java",
            core + "mxbean/IgniteMXBean.java",
            core + "internal/management/DeactivateCommandArg.java",
            core + "internal/processors/rest/GridRestCommand.java",
            core + "internal/processors/odbc/SqlStateCode.java",
            core + "internal/processors/platform/client/cache/ClientCacheGetRequest.java",
            core + "internal/processors/cache/persistence/wal/serializer/RecordSerializer.java",
            core + "internal/processors/cache/persistence/snapshot/IncrementalSnapshotMetadata.java",
            "modules/thin-client/api/src/main/java/org/apache/ignite/client/IgniteClient.java",
            "modules/binary/impl/src/main/java/org/apache/ignite/internal/binary/BinaryWriterExImpl.java",
            "modules/calcite/src/main/codegen/includes/parserImpls.ftl",
            "modules/platforms/dotnet/Apache.Ignite.Core/IIgnite.cs",
            "modules/platforms/cpp/thin-client/include/ignite/thin/ignite_client.h",
            "modules/core/pom.xml",
            "assembly/release-apache-ignite.xml",
            "bin/ignite.sh",
        ):
            with self.subTest(path=path):
                self.assertIsNotNone(contract_review(path))

    def test_paths_outside_scope(self):
        """A PR that only touches these internal, test or documentation paths gets no scope signal."""
        for path in (
            "modules/core/src/main/java/org/apache/ignite/internal/IgniteKernal.java",
            "modules/core/src/main/java/org/apache/ignite/spi/discovery/tcp/messages/TcpDiscoveryPingRequest.java",
            "modules/core/src/main/java/org/apache/ignite/spi/communication/tcp/internal/ConnectionKey.java",
            "modules/core/src/test/java/org/apache/ignite/ClassPathContentLoggingTest.java",
            "modules/extdata/p2p/src/main/java/org/apache/ignite/tests/p2p/classic/SingleSplitTestTask.java",
            "modules/platforms/dotnet/Apache.Ignite.Core.Tests/Client/Cache/CacheTest.cs",
            "modules/platforms/dotnet/Apache.Ignite.Core/Impl/Ignite.cs",
            "modules/platforms/cpp/core/include/ignite/impl/ignite_impl.h",
            "modules/platforms/cpp/core-test/include/ignite/test_type.h",
            "examples/pom.xml",
            "docs/_docs/sql-reference/data-types.adoc",
            "rfcs/TEMPLATE.md",
        ):
            with self.subTest(path=path):
                self.assertIsNone(contract_review(path))
