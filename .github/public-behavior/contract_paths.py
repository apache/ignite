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

"""Select source files for the static checks. A path match by itself is not a finding."""

from cli_sources import cli_package
from registered_contracts import supports_registered_contracts


JAVA_MODULES = {
    "binary/api", "binary/impl", "calcite", "commons", "compress", "control-utility", "core", "direct-io",
    "indexing", "jcl", "json", "jta", "kubernetes", "log4j2", "nio", "numa-allocator",
    "opencensus", "rest-http", "schedule", "slf4j", "spring", "thin-client/api",
    "thin-client/impl", "urideploy", "web", "zookeeper",
}

_CORE = "modules/core/src/main/java/org/apache/ignite/internal/"
_REST = "processors/rest/"
REST_SOURCES = {
    _CORE + _REST + "GridRestCommand.java",
    "modules/rest-http/src/main/java/org/apache/ignite/internal/" + _REST + "protocols/http/jetty/GridJettyRestProtocol.java",
    "modules/rest-http/src/main/java/org/apache/ignite/internal/" + _REST + "protocols/http/jetty/GridJettyRestHandler.java",
}
SQL_ERRORS = _CORE + "processors/cache/query/IgniteQueryErrorCode.java"
SQL_STATES = _CORE + "processors/odbc/SqlStateCode.java"

_INTERNAL_SOURCES = {
    "internal/processors/cache/query/IgniteQueryErrorCode.java",
    "internal/processors/odbc/SqlStateCode.java",
    "internal/processors/platform/client/ClientBitmaskFeature.java",
    "internal/client/thin/ClientOperation.java",
    "internal/processors/rest/protocols/http/jetty/IgniteRestExtension.java",
    "internal/commandline/ArgumentParser.java",
    "internal/commandline/CommandHandler.java",
    "internal/management/IgniteCommandRegistry.java",
    "internal/management/api/NoArg.java",
    "internal/binary/GridBinaryMarshaller.java",
    "internal/pagemem/wal/record/WALRecord.java",
    "internal/processors/cache/persistence/file/FilePageStore.java",
    "internal/processors/cache/persistence/file/FilePageStoreV2.java",
    "internal/processors/cache/persistence/snapshot/SnapshotMetadata.java",
    "internal/processors/cache/persistence/snapshot/IncrementalSnapshotMetadata.java",
}


def java_contract_source(path):
    if cli_package(path):
        return True
    module, source_root, java = path.partition("/src/main/java/org/apache/ignite/")
    if not source_root or not module.startswith("modules/") or not java.endswith(".java"):
        return False
    if module.removeprefix("modules/") not in JAVA_MODULES:
        return False
    if java in _INTERNAL_SOURCES or path in REST_SOURCES or supports_registered_contracts(path):
        return True
    return not (java.startswith(("internal/", "spi/discovery/tcp/messages/", "spi/communication/tcp/messages/"))
                or "/internal/" in java or java.endswith("package-info.java"))
