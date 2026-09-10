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
'get -b <name>' is issued against.
"""
import re

import pytest

from ignitetest.services.utils.jmx_utils import metric_registry_pattern

# An MBean name as a node prints it: the properties are ordered alphabetically, which puts
# the instance name between the group and the name.
PREFIX = "org.apache:clsLdr=3e2a56ab,group=%s,igniteInstanceName=ducker01,name=%s"


def matched(pattern, mbean_name):
    """
    :return: The part of the name the pattern matches, as 'grep -E -o' would return it.
    """
    match = re.search(pattern, mbean_name)

    return match.group() if match else None


class CheckMetricRegistryPattern:
    """
    Checks that the pattern covers how the JMX exporter really names a registry.
    """
    @pytest.mark.parametrize("cache_name", ["myCache", "cache_1"])
    def check_an_alphanumeric_name_is_matched_unquoted(self, cache_name):
        """A purely alphanumeric registry name is registered without quotes."""
        mbean_name = PREFIX % ("cache", cache_name)

        assert matched(metric_registry_pattern("cache", cache_name), mbean_name) == mbean_name

    @pytest.mark.parametrize("cache_name", ["my-cache", "mdc-demo-backup-filter"])
    def check_a_non_alphanumeric_name_is_matched_quoted(self, cache_name):
        """Anything else - a dash is enough - is registered quoted, closing quote included."""
        mbean_name = PREFIX % ("cache", f'"{cache_name}"')

        assert matched(metric_registry_pattern("cache", cache_name), mbean_name) == mbean_name

    def check_a_multi_part_registry_keeps_its_tail_as_the_name(self):
        """Only the FIRST dot splits the registry, so the rest of it is the MBean name."""
        mbean_name = PREFIX % ("io", '"dataregion.default"')

        assert matched(metric_registry_pattern("io", "dataregion.default"), mbean_name) == mbean_name

    def check_a_group_is_not_matched_by_its_prefix(self):
        """
        'cache' must not pick up 'cacheGroups': the two hold different metrics of the same
        cache, and the cache group one would answer with a plausible looking wrong bean.
        """
        cache_name = "my-cache"

        group_mbean_name = PREFIX % ("cacheGroups", f'"{cache_name}"')

        assert matched(metric_registry_pattern("cache", cache_name), group_mbean_name) is None

        assert matched(metric_registry_pattern("cacheGroups", cache_name), group_mbean_name) == group_mbean_name

    @pytest.mark.parametrize(["asked", "registered"],
                             [("myCache", "myCacheV2"), ("my-cache", '"my-cache-2"')])
    def check_a_name_is_not_matched_by_its_prefix(self, asked, registered):
        """
        A registry whose name merely STARTS the asked-for one must not answer: 'name' sorts
        last of an MBean name's properties, so the pattern can - and has to - end at the end
        of the line.
        """
        assert matched(metric_registry_pattern("cache", asked), PREFIX % ("cache", registered)) is None
