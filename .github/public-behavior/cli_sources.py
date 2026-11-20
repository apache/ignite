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

"""Read the command and argument declarations for the affected control.sh command families."""

from pathlib import PurePosixPath
import re


MANAGEMENT = "modules/core/src/main/java/org/apache/ignite/internal/management"
ROOT_REGISTRY = MANAGEMENT + "/IgniteCommandRegistry.java"
NO_ARG = MANAGEMENT + "/api/NoArg.java"
COMMANDLINE = "modules/control-utility/src/main/java/org/apache/ignite/internal/commandline/"
CLI_RULE_SOURCES = {COMMANDLINE + "ArgumentParser.java",
                    COMMANDLINE + "argument/parser/CLIArgumentParser.java",
                    COMMANDLINE + "argument/parser/CLIArgument.java"}
CLI_RULE_SOURCES.update(MANAGEMENT + "/api/" + name + ".java" for name in (
    "Argument", "ArgumentGroup", "ArgumentGroupsHolder", "Positional", "CommandUtils",
    "Command", "CommandRegistryImpl", "CliSubcommandsWithPrefix", "CommandsRegistry",
))

# The tree belongs to a single Java package. One request supplies its commands, argument DTOs and base classes.
_PACKAGE_QUERY = """query($owner:String!, $repo:String!, $path:String!) {
  repository(owner:$owner, name:$repo) {
    object(expression:$path) { ... on Tree { entries { name type object { ... on Blob { text isTruncated } } } } }
  }
}"""


def cli_package(path):
    if not path.startswith(MANAGEMENT + "/"):
        return None
    relative = path[len(MANAGEMENT) + 1:]
    if relative.startswith("api/") or not re.fullmatch(r"(?:[a-z][a-z0-9]*/)*[A-Z][A-Za-z0-9]*(?:Command|Arg)\.java", relative):
        return None
    return str(PurePosixPath(path).parent)


def cli_packages(paths):
    packages = set()
    for path in paths:
        package = cli_package(path)
        if package:
            packages.add(package)
            # A nested subcommand may be registered in its enclosing package.
            for parent in PurePosixPath(package).parents:
                if not str(parent).startswith(MANAGEMENT + "/"):
                    break
                packages.add(str(parent))
    return packages


def cli_package_sources(repository, revision, package, api):
    owner, repo = repository.split("/")
    data = api("graphql", fields={"query": _PACKAGE_QUERY, "owner": owner, "repo": repo, "path": revision + ":" + package})
    tree = data["data"]["repository"]["object"]
    if not tree:
        return {}
    sources = {}
    for entry in tree["entries"]:
        path = package + "/" + entry["name"]
        if entry["type"] != "blob" or not (cli_package(path) == package or path == ROOT_REGISTRY):
            continue
        blob = entry["object"]
        if blob and isinstance(blob.get("text"), str) and not blob.get("isTruncated", True):
            sources[path] = blob["text"]
    return sources
