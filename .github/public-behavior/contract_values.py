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

"""Read literal coordinates of published Maven modules."""

import re
from xml.etree import ElementTree

from contract_paths import JAVA_MODULES


# modules/bom/pom.xml lists the published artifacts covered here.
_MODULES = (JAVA_MODULES - {"jcl", "numa-allocator"}) | {"bom"}
_POMS = {f"modules/{module}/pom.xml" for module in _MODULES}
_NAMESPACE = "{http://maven.apache.org/POM/4.0.0}"


def supports_values(path):
    return path in _POMS


def contract_values(path, source):
    if not supports_values(path):
        return {}
    try:
        project = ElementTree.fromstring(source)
    except (ElementTree.ParseError, ValueError):
        return {}
    if project.tag not in {"project", _NAMESPACE + "project"}:
        return {}
    namespace = _NAMESPACE if project.tag.startswith("{") else ""
    values = {}
    for field in ("groupId", "artifactId"):
        nodes = project.findall(namespace + field)
        if field == "groupId" and not nodes:
            nodes = project.findall(f"{namespace}parent/{namespace}groupId")
        if len(nodes) != 1 or len(nodes[0]):
            continue
        value = (nodes[0].text or "").strip()
        if re.fullmatch(r"[A-Za-z0-9_.-]+", value):
            values["Maven " + field] = value
    return values
