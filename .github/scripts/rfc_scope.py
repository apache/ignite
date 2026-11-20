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

# Paths relative to org/apache/ignite/internal/. A package named "internal" can
# implement a command, an external protocol, or a format needed to recover data.
INTERNAL_PATHS = (
    "management/*Command.java", "management/*CommandArg.java", "management/api/*.java",
    "commandline/ArgumentParser.java", "commandline/CommandHandler.java",
    "processors/rest/GridRestCommand.java", "processors/rest/GridRestResponse.java",
    "processors/rest/request/*.java",
    "processors/configuration/distributed/*.java", "*/Distributed*Configuration.java",
    "sql/*.java", "processors/query/calcite/sql/*.java",
    "processors/query/calcite/exec/exp/IgniteSqlFunctions.java",
    "processors/cache/query/IgniteQueryErrorCode.java",
    "processors/odbc/*.java", "processors/platform/client/*.java",
    "client/thin/ClientOperation.java", "client/thin/Protocol*.java",
    "binary/BinaryMetadata.java", "binary/GridBinaryMarshaller.java",
    "binary/BinaryReaderExImpl.java", "binary/BinaryWriterExImpl.java",
    "pagemem/wal/record/*.java", "processors/cache/persistence/wal/serializer/*.java",
    "processors/cache/persistence/tree/io/*.java",
    "processors/cache/persistence/snapshot/*SnapshotMetadata.java",
)

OTHER_PATHS = (
    r"modules/platforms/cpp/(binary|common|core|thin-client)/include/ignite/(?!impl/|jni/).+\.(h|hpp)",
    r"modules/platforms/cpp/odbc/include/ignite/odbc\.h",
    r"modules/platforms/dotnet/Apache\.Ignite\.(Core|AspNet|EntityFramework|Linq|Log4Net|NLog)/"
    r"(?!Impl/).+\.(cs|csproj|xsd)",
    r"modules/calcite/src/main/codegen/(config\.fmpp|includes/.+\.ftl)",
    r"(pom|parent/pom|parent-internal/pom)\.xml",
    r"assembly/(release|dependencies)-[^/]+\.xml",
    r"bin/(ignite|ignite-cdc|control)\.(sh|bat)",
    r"config/(wsl-)?default-config\.xml",
)


def potential_contract(path: str) -> bool:
    """Return whether this repository-relative path needs a contract decision."""
    module, source_root, java = path.partition("/src/main/java/org/apache/ignite/")
    if source_root and module.startswith("modules/") and java.endswith(".java"):
        if module.removeprefix("modules/") not in JAVA_MODULES:
            return False
        if java.startswith("internal/"):
            internal_path = java.removeprefix("internal/")
            return any(fnmatchcase(internal_path, pattern) for pattern in INTERNAL_PATHS)
        # Discovery and communication SPIs are public; their node messages are private.
        return "/internal/" not in java and not java.startswith((
            "spi/discovery/tcp/messages/", "spi/communication/tcp/messages/",
        ))

    if path.startswith("modules/") and path.endswith("/pom.xml"):
        module = path.removeprefix("modules/").removesuffix("/pom.xml")
        return module == "bom" or module in JAVA_MODULES
    return any(re.fullmatch(pattern, path) for pattern in OTHER_PATHS)
