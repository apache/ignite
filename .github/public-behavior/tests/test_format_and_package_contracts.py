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

from contract_diff import compare_sources


def changes(before, after):
    sources = {(side, path): src for side, snapshot in (("before", before), ("after", after))
               for path, src in snapshot.items()}
    return compare_sources([(path, path) for path in before], sources)


CORE = "modules/core/src/main/java/org/apache/ignite/internal/"
WAL = CORE + "pagemem/wal/record/WALRecord.java"
PAGE = CORE + "processors/cache/persistence/file/FilePageStore.java"
META = CORE + "processors/cache/persistence/snapshot/SnapshotMetadata.java"
BINARY = "modules/binary/api/src/main/java/org/apache/ignite/internal/binary/GridBinaryMarshaller.java"
ODBC = "modules/platforms/cpp/odbc/module.def"
NUGET = "modules/platforms/dotnet/Apache.Ignite.Core/Apache.Ignite.Core.csproj"
XSD = NUGET.rsplit("/", 1)[0] + "/IgniteConfigurationSection.xsd"
BOM = "modules/bom/pom.xml"


class FormatAndPackageContractsTest(unittest.TestCase):
    def test_persisted_numbers_and_wire_tags(self):
        """Changing a literal used in a stored file or binary object identifies that format."""
        before = {
            WAL: "package org.apache.ignite.internal.pagemem.wal.record; class WALRecord { enum RecordType {"
                 "PAGE_RECORD(1, PHYSICAL), RESERVED(9); RecordType(int i, Object... p) {} } }",
            PAGE: "package org.apache.ignite.internal.processors.cache.persistence.file; class FilePageStore {"
                  "private static final long SIGNATURE = 0xABCDL; static final int DFLT_BUFFER = 42; }",
            META: "package org.apache.ignite.internal.processors.cache.persistence.snapshot; class SnapshotMetadata {"
                  "private static final long serialVersionUID = 0L; }",
            BINARY: "package org.apache.ignite.internal.binary; class GridBinaryMarshaller {"
                    "public static final byte STRING=9, OBJ=(byte)103; }",
        }
        after = {WAL: before[WAL].replace("(1,", "(8,").replace("(9)", "(10)"),
                 PAGE: before[PAGE].replace("0xABCD", "0xABCE").replace("42", "53"),
                 META: before[META].replace("0L", "1L"), BINARY: before[BINARY].replace("=9,", "=17,")}
        self.assertEqual(changes(before, after), {
            WAL: ["Changed WAL record index for PAGE_RECORD from 1 to 8"],
            PAGE: ["Changed FilePageStore header SIGNATURE from 43981 to 43982"],
            META: ["Changed SnapshotMetadata serialization version from 0 to 1"],
            BINARY: ["Changed binary type code for STRING from 9 to 17"],
        })
        after = {WAL: before[WAL].replace("PAGE_RECORD", "PAGE"), META: before[META],
                 PAGE: before[PAGE].replace("0xABCDL", "43981L"), BINARY: before[BINARY].replace("(byte)103", "103")}
        self.assertEqual(changes(before, after), {})

    def test_sqlstate_mapping_resolves_both_constants(self):
        """JDBC returns a different SQLState for the same numeric Ignite error."""
        errors = CORE + "processors/cache/query/IgniteQueryErrorCode.java"
        states = CORE + "processors/odbc/SqlStateCode.java"
        before = {errors: """package org.apache.ignite.internal.processors.cache.query;
            class IgniteQueryErrorCode { public static final int DUPLICATE_KEY = 4001;
                static String codeToSqlState(int code) {
                    switch (code) { case DUPLICATE_KEY: return SqlStateCode.CONSTRAINT;
                        default: return SqlStateCode.INVALID; }
                } }""", states: """package org.apache.ignite.internal.processors.odbc;
            class SqlStateCode { public static final String CONSTRAINT="23000", INVALID="22000"; }"""}
        after = dict(before, **{errors: before[errors].replace("SqlStateCode.CONSTRAINT", "SqlStateCode.INVALID")})
        self.assertEqual(changes(before, after), {errors: ["Changed SQLState for error 4001 from 23000 to 22000"]})
        fallback = before[errors].replace("case DUPLICATE_KEY: return SqlStateCode.CONSTRAINT;", "")
        self.assertEqual(changes(before, {**before, errors: fallback}),
                         {errors: ["Changed SQLState for error 4001 from 23000 to 22000"]})
        after = {k: v.replace("CONSTRAINT", "CONSTRAINT_VIOLATION") for k, v in before.items()}
        self.assertEqual(changes(before, after), {})

    def test_published_manifest_entries(self):
        """A public package, exported ODBC name or managed dependency changes in its manifest."""
        before = {ODBC: "LIBRARY   ignite.odbc.dll\nEXPORTS\nSQLAllocConnect\nSQLDisconnect\n",
                  NUGET: '<Project Sdk="Microsoft.NET.Sdk"><PropertyGroup><PackageId>Apache.Ignite</PackageId>'
                         '<TargetFramework>netstandard2.0</TargetFramework></PropertyGroup></Project>',
                  "parent/pom.xml": "<project><properties><maven.compiler.release>17</maven.compiler.release>"
                                    "</properties></project>",
                  BOM: "<project><groupId>org.apache.ignite</groupId><dependencyManagement><dependencies><dependency>"
                       "<groupId>${project.groupId}</groupId><artifactId>ignite-core</artifactId></dependency>"
                       "</dependencies></dependencyManagement></project>"}
        after = {ODBC: before[ODBC].replace("SQLAllocConnect\n", ""), NUGET: before[NUGET].replace("Apache.Ignite", "Ignite"),
                 "parent/pom.xml": before["parent/pom.xml"].replace("17", "21"),
                 BOM: before[BOM].replace("ignite-core", "ignite-new")}
        self.assertEqual(changes(before, after), {
            ODBC: ["Removed Windows ODBC export: SQLAllocConnect"], NUGET: ["NuGet package ID changed from Apache.Ignite to Ignite."],
            BOM: ["Removed Maven BOM dependency: org.apache.ignite:ignite-core",
                  "Added Maven BOM dependency: org.apache.ignite:ignite-new"],
            "parent/pom.xml": ["Java release for compiled artifacts changed from 17 to 21."],
        })
        same = before[BOM].replace("</dependency>", "<type> jar </type><scope> compile </scope></dependency>")
        self.assertEqual(changes({BOM: before[BOM]}, {BOM: same}), {})
        imported = before[BOM].replace("</dependency>", "<type>pom</type><scope>import</scope></dependency>")
        self.assertEqual(changes({BOM: before[BOM]}, {BOM: imported}), {})
        self.assertEqual(changes({ODBC: before[ODBC]}, {ODBC: before[ODBC].replace("SQLDisconnect", "SQLDisconnect=impl")}), {})

    def test_dotnet_configuration_schema(self):
        """A configuration file accepts different enum values or requires a previously optional attribute."""
        schema = ('<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"><xs:simpleType name="cacheMode">'
                  '<xs:restriction base="xs:string"><xs:enumeration value="Atomic"/></xs:restriction></xs:simpleType>'
                  '<xs:element name="config"><xs:complexType><xs:attribute name="host" use="optional"/>'
                  '</xs:complexType></xs:element></xs:schema>')
        self.assertEqual(changes({XSD: schema}, {XSD: schema.replace("Atomic", "Local").replace("optional", "required")}),
                         {XSD: ["XML configuration /config/@host use changed from optional to required.",
                                "Removed XML configuration values for cacheMode: Atomic",
                                "Added XML configuration values for cacheMode: Local"]})

    def test_equivalent_values_in_shipped_manifests(self):
        """Configuration booleans and NuGet IDs have equivalent external spellings."""
        schema = ('<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"><xs:element name="config">'
                  '<xs:complexType><xs:attribute name="tryStop" type="xs:boolean" default="false"/>'
                  '</xs:complexType></xs:element></xs:schema>')
        project = '<Project Sdk="Microsoft.NET.Sdk"><PropertyGroup><PackageId>Apache.Ignite</PackageId></PropertyGroup></Project>'
        self.assertEqual(changes({XSD: schema, NUGET: project},
                                 {XSD: schema.replace('"false"', '"0"'), NUGET: project.replace("Apache.Ignite", "apache.ignite")}), {})
