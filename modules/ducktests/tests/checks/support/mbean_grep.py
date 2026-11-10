# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
The MBean lookup of a JMX client, for checks of the patterns it is handed.

The client pipes the MBean list of a node through 'grep -E -o <pattern> | grep -E -v <negative pattern>'.
The functions here run a single MBean name through the same pipeline: once with Python's re, and once
with 'grep -E' itself where a grep is installed - the patterns are EREs, not Python regular expressions.
"""
import re
import shutil
import subprocess

GREP = shutil.which("grep")


def python_grep(pattern, negative_pattern, mbean_name):
    """
    :return: The part of the name the patterns leave, as 'grep -E -o | grep -E -v' would return it.
    """
    match = re.search(pattern, mbean_name)

    if not match or (negative_pattern and re.search(negative_pattern, match.group())):
        return None

    return match.group()


def real_grep(tmp_path, pattern, negative_pattern, mbean_name):
    """
    :return: The same as :func:`python_grep`, but produced by 'grep -E' itself. The patterns go
             through files, since a quote in a command line argument does not survive Windows.
    """
    def grep(flag, grep_pattern, text):
        pattern_file = tmp_path / f"pattern{flag}"
        pattern_file.write_bytes((grep_pattern + "\n").encode())

        return subprocess.run([GREP, "-E", flag, "-f", str(pattern_file)], input=text.encode(),
                              capture_output=True).stdout.decode()

    out = grep("-o", pattern, mbean_name + "\n")

    if out and negative_pattern:
        out = grep("-v", negative_pattern, out)

    return out.strip() or None
