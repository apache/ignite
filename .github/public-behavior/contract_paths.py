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

"""Repository paths where a change warrants review against Ignite's public contracts."""

import re
from fnmatch import fnmatchcase


JAVA_MODULES = {
    "binary/api", "binary/impl", "calcite", "commons", "compress", "control-utility", "core", "direct-io",
    "indexing", "jcl", "json", "jta", "kubernetes", "log4j2", "nio", "numa-allocator",
    "opencensus", "rest-http", "schedule", "slf4j", "spring", "thin-client/api",
    "thin-client/impl", "urideploy", "web", "zookeeper",
}

PUBLIC_API = ("Check that existing source still compiles and existing binaries still run. "
              "Check results, errors and calls to user overrides or callbacks.")
COMMAND = "Run existing commands with their current flags and check their output and exit codes."
PROTOCOL = "Check request, response and error handling between supported client and server versions."
CONFIG = "Check that existing configuration names, accepted values and defaults still work."
METRICS = "Check that monitoring tools can still read the documented metric names, types and values."
SQL = "Run existing queries and compare their results and error codes."
STORAGE = "Test recovery on a supported upgrade using data written by the previous version."
BINARY = "Check that supported clients can still decode values written in the previous binary format."
MAVEN = "Check that existing builds can still resolve the published Maven coordinates and dependencies."
DISTRIBUTION = "Check that existing installations still find their runtime files and dependencies in the distribution."

# Paths relative to org/apache/ignite/internal/. A package named "internal" can
# implement a command, an external protocol, or a format needed to recover data.
INTERNAL_PATHS = {
    COMMAND: ("management/*Command.java", "management/*CommandArg.java", "management/api/*.java",
              "commandline/ArgumentParser.java", "commandline/CommandHandler.java"),
    PROTOCOL: ("processors/rest/GridRestCommand.java", "processors/rest/GridRestResponse.java",
               "processors/rest/request/*.java", "processors/odbc/*.java", "processors/platform/client/*.java",
               "client/thin/ClientOperation.java", "client/thin/Protocol*.java"),
    CONFIG: ("jdbc/thin/ConnectionPropertiesImpl.java", "processors/configuration/distributed/*.java",
             "*/Distributed*Configuration.java"),
    METRICS: ("processors/metric/GridMetricManager.java",),
    SQL: ("sql/*.java", "processors/query/calcite/sql/*.java", "processors/query/calcite/exec/exp/IgniteSqlFunctions.java",
          "processors/cache/query/IgniteQueryErrorCode.java"),
    BINARY: ("binary/BinaryMetadata.java", "binary/GridBinaryMarshaller.java",
             "binary/BinaryReaderExImpl.java", "binary/BinaryWriterExImpl.java"),
    STORAGE: ("pagemem/wal/record/*.java", "processors/cache/persistence/wal/serializer/*.java",
              "processors/cache/persistence/tree/io/*.java", "processors/cache/persistence/metastorage/*IO.java",
              "processors/cache/persistence/snapshot/*SnapshotMetadata.java"),
}

OTHER_PATHS = {
    PUBLIC_API: (
        r"modules/platforms/cpp/(binary|common|core|thin-client)/include/ignite/(?!impl/|jni/).+\.(h|hpp)",
        r"modules/platforms/cpp/odbc/include/ignite/odbc\.h",
        r"modules/platforms/dotnet/Apache\.Ignite\.(Core|AspNet|EntityFramework|Linq|Log4Net|NLog)/"
        r"(?!Impl/).+\.(cs|csproj|xsd)",
    ),
    SQL: (r"modules/calcite/src/main/codegen/(config\.fmpp|includes/.+\.ftl)",),
    MAVEN: (r"(pom|parent/pom|parent-internal/pom)\.xml",),
    DISTRIBUTION: (r"assembly/(release|dependencies)-[^/]+\.xml",),
    COMMAND: (r"bin/(ignite|ignite-cdc|control)\.(sh|bat)",),
    CONFIG: (r"config/(wsl-)?default-config\.xml",),
}


def contract_review(path: str) -> str | None:
    """Return the compatibility check for a matched path, or None for an unflagged path."""
    module, source_root, java = path.partition("/src/main/java/org/apache/ignite/")
    if source_root and module.startswith("modules/") and java.endswith(".java"):
        if module.removeprefix("modules/") not in JAVA_MODULES:
            return None
        if java.startswith("internal/"):
            internal_path = java.removeprefix("internal/")
            for hint, patterns in INTERNAL_PATHS.items():
                if any(fnmatchcase(internal_path, pattern) for pattern in patterns):
                    return hint
            return None
        # Discovery and communication SPIs are public; their node messages are private.
        if "/internal/" in java or java.startswith(("spi/discovery/tcp/messages/", "spi/communication/tcp/messages/")):
            return None
        if java.startswith("configuration/") or java == "IgniteSystemProperties.java":
            return CONFIG
        return PUBLIC_API

    if path.startswith("modules/") and path.endswith("/pom.xml"):
        module = path.removeprefix("modules/").removesuffix("/pom.xml")
        return MAVEN if module == "bom" or module in JAVA_MODULES else None
    for hint, patterns in OTHER_PATHS.items():
        if any(re.fullmatch(pattern, path) for pattern in patterns):
            return hint
    return None
