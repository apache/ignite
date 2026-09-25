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
Checks the MBean name pattern of a metric registry.

The pattern is grepped against the MBean list of a node, so it is checked here the same way:
the match has to cover the WHOLE object name, since that name is what the following
'get -b <name>' is issued against. Every case runs through Python's re and, where a grep is
installed, through 'grep -E' itself - the pattern is an ERE, not a Python regular expression.
"""
import re
import shutil
import subprocess

import pytest

from ignitetest.services.utils.jmx_utils import metric_registry_pattern

# MBean names as a node prints them: the properties are ordered alphabetically, which puts
# the instance name between the group and the name.
PREFIX = "org.apache:clsLdr=3e2a56ab,group=%s,igniteInstanceName=ducker01,name=%s"
NO_GROUP_PREFIX = "org.apache:clsLdr=3e2a56ab,igniteInstanceName=ducker01,name=%s"

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


# (registry, MBean name, whether the registry's pattern must match that name)
CASES = [
    # A purely alphanumeric name is registered without quotes.
    ("cache.myCache", PREFIX % ("cache", "myCache"), True),
    ("cache.cache_1", PREFIX % ("cache", "cache_1"), True),
    # Anything else - a dash is enough - is registered quoted, closing quote included.
    ("cache.my-cache", PREFIX % ("cache", '"my-cache"'), True),
    ("cache.mdc-demo-backup-filter", PREFIX % ("cache", '"mdc-demo-backup-filter"'), True),
    # Only the FIRST dot splits the registry, so the rest of it is the MBean name.
    ("io.dataregion.default", PREFIX % ("io", '"dataregion.default"'), True),
    # 'cache' must not pick up 'cacheGroups': the two hold different metrics of the same cache.
    ("cache.my-cache", PREFIX % ("cacheGroups", '"my-cache"'), False),
    ("cacheGroups.my-cache", PREFIX % ("cacheGroups", '"my-cache"'), True),
    # 'name' sorts last, so the pattern ends at the end of the line and a longer name does not answer.
    ("cache.myCache", PREFIX % ("cache", "myCacheV2"), False),
    ("cache.my-cache", PREFIX % ("cache", '"my-cache-2"'), False),
    # The name is matched literally, ERE metacharacters included.
    ("cache.my.cache", PREFIX % ("cache", '"myXcache"'), False),
    ("cache.my.cache", PREFIX % ("cache", '"my.cache"'), True),
    ("cache.a(b)+c", PREFIX % ("cache", '"a(b)+c"'), True),
    ("cache.a(b)+c", PREFIX % ("cache", '"abbc"'), False),
    # Characters that are special only right after an opening '[' or '{' stay unescaped.
    ("cache.a]b}c", PREFIX % ("cache", '"a]b}c"'), True),
    ("cache.a[b]{2}", PREFIX % ("cache", '"a[b]{2}"'), True),
    ("cache.a[b]{2}", PREFIX % ("cache", '"abb"'), False),
    # The exporter escapes '*' inside the quotes.
    ("cache.a*b", PREFIX % ("cache", r'"a\*b"'), True),
    # A registry without a dot has no group, and must not pick up the system view of the same name.
    ("snapshot", NO_GROUP_PREFIX % "snapshot", True),
    ("snapshot", PREFIX % ("views", "snapshot"), False),
    ("cdc", PREFIX % ("cdc", "consumer"), False),
]


class CheckMetricRegistryPattern:
    """
    Checks that the pattern covers how the JMX exporter really names a registry.
    """
    @pytest.mark.parametrize(["registry", "mbean_name", "expected"], CASES)
    def check_the_pattern_matches_exactly_the_registry(self, registry, mbean_name, expected):
        assert python_grep(*metric_registry_pattern(registry), mbean_name) == (mbean_name if expected else None)

    @pytest.mark.skipif(GREP is None, reason="grep is not installed")
    @pytest.mark.parametrize(["registry", "mbean_name", "expected"], CASES)
    def check_grep_agrees(self, tmp_path, registry, mbean_name, expected):
        assert real_grep(tmp_path, *metric_registry_pattern(registry), mbean_name) == \
               (mbean_name if expected else None)

    @pytest.mark.parametrize("registry", ["cache.it's", 'cache.a"b', "cache.a\\b", "cache.a?b"])
    def check_a_name_the_jmxterm_commands_cannot_carry_is_rejected(self, registry):
        """The MBean name goes through the shell quoting of the grep and jmxterm commands."""
        with pytest.raises(ValueError):
            metric_registry_pattern(registry)
