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

"""Read the minimum Java version enforced by the distribution launchers."""

import re


_UNIX = "bin/include/functions.sh"
_WINDOWS = {"bin/ignite.bat", "bin/control.bat", "bin/index-reader.bat"}
_UNIX_GATE = re.compile(
    r'^ +javaMajorVersion "\$JAVA"\n\s*if \[ \$version -lt (?P<minimum>[1-9][0-9]*) \]; then\n'
    r'(?:[ \t]+echo [^\n]*\n)*[ \t]+exit 1\n[ \t]+fi\n}$', re.MULTILINE)
_WINDOWS_GATE = re.compile(
    r'^if %MAJOR_JAVA_VER% == 1 set MAJOR_JAVA_VER=%MINOR_JAVA_VER%\n\s*'
    r'if %MAJOR_JAVA_VER% LSS (?P<minimum>[1-9][0-9]*) \(\n'
    r'(?:[ \t]+echo [^\n]*\n)*[ \t]+goto error_finish\n\)$', re.MULTILINE | re.IGNORECASE)
_WINDOWS_ERROR_EXIT = re.compile(r'^:error_finish\n\s*(?:if not "%NO_PAUSE%" == "1" pause\n\s*)?goto :eof$',
                                re.MULTILINE | re.IGNORECASE)
# The same installation advice also appears in the branches for a missing java executable.
_JDK_ADVICE = re.compile(r'^[ \t]*echo "?[a-zA-Z0-9 _.,:/-]*\bJDK\b[a-zA-Z0-9 _.,:/-]*"?$',
                         re.MULTILINE | re.IGNORECASE)


def supports_launcher(path):
    return path == _UNIX or path in _WINDOWS


def launcher_changes(path, before, after):
    if not supports_launcher(path):
        return []
    gate = _UNIX_GATE if path == _UNIX else _WINDOWS_GATE
    versions = []
    scripts = []
    for source in (before, after):
        script = source.replace("\r\n", "\n")
        matches = list(gate.finditer(script))
        if len(matches) != 1:
            return []
        match = matches[0]
        if path == _UNIX:
            # The rejection must be in checkJava, which the Unix launchers call.
            prefix = script[:match.start()].rsplit("\n}", 1)[-1]
            if not re.search(r"^checkJava\(\) \{$", prefix, re.MULTILINE):
                return []
        elif not (re.search(r"^:checkJdkVersion$", script[:match.start()], re.MULTILINE | re.IGNORECASE)
                  and _WINDOWS_ERROR_EXIT.search(script, match.end())):
            return []
        versions.append(match["minimum"])
        start, end = match.span("minimum")
        scripts.append(_JDK_ADVICE.sub("", script[:start] + "<minimum>" + script[end:]))
    # If the control flow also changed, the numeric threshold alone cannot establish the effect.
    if scripts[0] == scripts[1] and versions[0] != versions[1]:
        return [f"Changed minimum Java version accepted by the launcher from {versions[0]} to {versions[1]}."]
    return []
