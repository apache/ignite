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

JAVA_MODULES = {
    "binary/api", "binary/impl", "calcite", "commons", "compress", "control-utility", "core", "direct-io",
    "indexing", "jcl", "json", "jta", "kubernetes", "log4j2", "nio", "numa-allocator",
    "opencensus", "rest-http", "schedule", "slf4j", "spring", "thin-client/api",
    "thin-client/impl", "urideploy", "web", "zookeeper",
}

WIRE_CONTRACTS = {
    "internal/processors/cache/query/IgniteQueryErrorCode.java",
    "internal/processors/odbc/SqlStateCode.java",
    "internal/processors/platform/client/ClientBitmaskFeature.java",
    "internal/client/thin/ClientOperation.java",
}


def java_contract_source(path):
    module, source_root, java = path.partition("/src/main/java/org/apache/ignite/")
    if not source_root or not module.startswith("modules/") or not java.endswith(".java"):
        return False
    if module.removeprefix("modules/") not in JAVA_MODULES:
        return False
    if java in WIRE_CONTRACTS:
        return True
    return not (java.startswith(("internal/", "spi/discovery/tcp/messages/", "spi/communication/tcp/messages/"))
                or "/internal/" in java or java.endswith("package-info.java"))
