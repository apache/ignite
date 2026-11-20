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

"""Read values from published manifests and configuration schemas."""

import re
from xml.etree import ElementTree

from contract_paths import JAVA_MODULES


# modules/bom/pom.xml lists the published artifacts covered here.
_MODULES = (JAVA_MODULES - {"jcl", "numa-allocator"}) | {"bom"}
_POMS = {f"modules/{module}/pom.xml" for module in _MODULES}
_NAMESPACE = "{http://maven.apache.org/POM/4.0.0}"


_PLATFORMS = "modules/platforms/"
_ODBC = _PLATFORMS + "cpp/odbc/module.def"
_DOTNET = _PLATFORMS + "dotnet/Apache.Ignite."
_NUGET = {_DOTNET + name + "/Apache.Ignite." + name + ".csproj"
          for name in ("Core", "EntityFramework", "AspNet", "Linq", "NLog", "Log4Net", "Schema")}
_SCHEMAS = {_DOTNET + "Core/" + name + ".xsd" for name in ("IgniteClientConfigurationSection", "IgniteConfigurationSection")}
_XS = "{http://www.w3.org/2001/XMLSchema}"
_PARENT = "parent/pom.xml"
_BOM = "modules/bom/pom.xml"


def supports_values(path):
    return path in _POMS or path in _NUGET or path in _SCHEMAS or path in {_PARENT, _ODBC}


def _odbc_exports(source):
    lines = [line.partition(";")[0].strip() for line in source.splitlines()]
    lines = [line for line in lines if line]
    if len(lines) < 2 or not lines[0].startswith("LIBRARY ") or lines[1] != "EXPORTS":
        return None
    exports = set()
    for entry in lines[2:]:
        match = re.fullmatch(r"([a-zA-Z_]\w*)(?:\s*=\s*[a-zA-Z_]\w*)?", entry, re.ASCII)
        if not match:
            return None
        exports.add(match[1])
    return exports


def _schema_values(schema):
    if schema.tag != _XS + "schema":
        return {}
    values = {"XML configuration namespace": schema.get("targetNamespace")}
    for typ in schema.findall(_XS + "simpleType"):
        restriction = typ.find(_XS + "restriction")
        if restriction is None or not typ.get("name"):
            continue
        choices = {node.get("value") for node in restriction.findall(_XS + "enumeration")}
        if None not in choices and choices and restriction.get("base") == "xs:string":
            values["XML configuration values for " + typ.get("name")] = choices
    groups = {_XS + group for group in ("complexType", "all", "sequence", "choice")}

    # Only compare attributes in inline element declarations with the same external path.
    def attributes(node, path):
        for child in node:
            if child.tag == _XS + "element" and child.get("name"):
                attributes(child, path + "/" + child.get("name"))
            elif child.tag in groups and not child.get("name"):
                attributes(child, path)
            elif child.tag == _XS + "attribute" and child.get("name") and path:
                key = "XML configuration " + path + "/@" + child.get("name")
                values[key + " use"] = child.get("use", "optional")
                default = child.get("default")
                kind = child.get("type")
                if default is not None:
                    if kind == "xs:string":
                        values[key + " string default"] = default
                    elif kind == "xs:boolean":
                        value = {"0": "false", "false": "false", "1": "true", "true": "true"}.get(default.strip())
                        values[key + " boolean default"] = value
    attributes(schema, "")
    return {key: val for key, val in values.items() if val is not None}


def _nuget_values(project):
    if project.tag != "Project" or project.get("Sdk") != "Microsoft.NET.Sdk":
        return {}
    values = {}
    for field, label in (("PackageId", "NuGet package ID"), ("TargetFramework", ".NET target framework"),
                         ("AssemblyName", ".NET assembly name"), ("IsPackable", "packable")):
        all_nodes = project.findall(".//" + field)
        groups = project.findall("PropertyGroup")
        literal = [node for group in groups if not group.attrib for node in group.findall(field) if not node.attrib]
        if len(all_nodes) == len(literal) == 1 and re.fullmatch(r"[a-zA-Z0-9_.-]+", literal[0].text or ""):
            values[label] = literal[0].text
        elif field == "IsPackable" and all_nodes:
            return {}
    return values if values.pop("packable", "true").lower() == "true" else {}


def value_changes(label, before, after):
    if label in {"NuGet package ID", ".NET target framework"} and before.casefold() == after.casefold():
        return []
    if isinstance(before, set):
        return ([f"Removed {label}: {name}" for name in sorted(before - after)]
                + [f"Added {label}: {name}" for name in sorted(after - before)])
    return [f"{label} changed from {before} to {after}."] if before != after else []


def contract_values(path, source):
    if not supports_values(path):
        return {}
    if path == _ODBC:
        exports = _odbc_exports(source)
        return {} if exports is None else {"Windows ODBC export": exports}
    try:
        project = ElementTree.fromstring(source)
    except (ElementTree.ParseError, ValueError):
        return {}
    if path in _SCHEMAS:
        return _schema_values(project)
    if path in _NUGET:
        return _nuget_values(project)
    if project.tag not in {"project", _NAMESPACE + "project"}:
        return {}
    namespace = _NAMESPACE if project.tag.startswith("{") else ""
    if path == _PARENT:
        release = project.findtext(f"{namespace}properties/{namespace}maven.compiler.release", "")
        return {"Java release for compiled artifacts": release} if re.fullmatch(r"[1-9][0-9]*", release) else {}
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
    if path == _BOM:
        dependencies = project.find(f"{namespace}dependencyManagement/{namespace}dependencies")
        if dependencies is not None:
            managed = set()
            for dependency in dependencies.findall(namespace + "dependency"):
                if (dependency.findtext(namespace + "type", "jar") != "jar"
                        or dependency.find(namespace + "classifier") is not None
                        or dependency.findtext(namespace + "scope", "compile") != "compile"):
                    continue
                artifact = dependency.findtext(namespace + "artifactId", "")
                group = dependency.findtext(namespace + "groupId", "")
                if group == "${project.groupId}":
                    group = values.get("Maven groupId", "")
                if not all(re.fullmatch(r"[A-Za-z0-9_.-]+", part) for part in (group, artifact)):
                    break
                managed.add(group + ":" + artifact)
            else:
                values["Maven BOM dependency"] = managed
    return values
