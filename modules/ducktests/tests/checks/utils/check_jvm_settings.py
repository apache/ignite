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
Checks JVM settings.
"""

import pytest

from ignitetest.services.utils.jvm_utils import create_jvm_settings, merge_jvm_settings, validate_gc_settings, \
    DEFAULT_HEAP, GC_PROFILES, GC_G1, GC_SERIAL, MultipleGcSelectedError


class CheckJVMSettings:
    """
    Checks behavior of various tools.
    """

    def check_list(self):
        """
        Checks list representation of JVM settings.
        """
        jvm_settings = create_jvm_settings()

        assert "-Xms" + DEFAULT_HEAP in jvm_settings
        assert "-Xmx" + DEFAULT_HEAP in jvm_settings

        jvm_settings = merge_jvm_settings(jvm_settings, additionals="-Xms981M -Xmx981M")

        assert "-Xms981M" in jvm_settings
        assert "-Xmx981M" in jvm_settings
        assert "-Xms" + DEFAULT_HEAP not in jvm_settings
        assert "-Xmx" + DEFAULT_HEAP not in jvm_settings

        jvm_settings = merge_jvm_settings(jvm_settings, additionals="-XX:ParallelGCThreads=1024")
        jvm_settings = merge_jvm_settings(jvm_settings, additionals="-xx:ParallelGCThreads=512")

        assert "-XX:ParallelGCThreads=1024" in jvm_settings
        assert "-XX:ParallelGCThreads=512" not in jvm_settings

    @pytest.mark.parametrize(
        'settings,additionals,expected',
        [
            [['-Xmx10G, -Xms1G'], ['-Xmx5G', '-Xms512m'], {'-Xmx5G': 1, '-Xms512m': 1}],
            [['-Xmx5G', '-Xms512m'], ['-Xmx10G', '-Xms1G'], {'-Xmx10G': 1, '-Xms1G': 1}],
            [['-Xmx10G, -Xms1G'], ['-Xmx5G', '-Xms512m'], {'-Xmx5G': 1, '-Xms512m': 1}],
            [
                ['-Xmx5G', '-Xms512m', '-XX:ParallelGCThreads=1024'],
                ['-Xmx10G', '-Xms1G', '-XX:ParallelGCThreads=512'],
                {'-Xmx10G': 1, '-Xms1G': 1, '-XX:ParallelGCThreads=512': 1}
            ],
            [['-Xmx5G', '-Xms512m', '-ea'], ['-Xmx10G', '-Xms1G', '-ea'], {'-Xmx10G': 1, '-Xms1G': 1, '-ea': 1}],
        ]
    )
    def check_merge_jvm_settings(self, settings, additionals, expected):
        """
        Tests different variants of merge jvm settings.
        """
        res = {}
        for param in merge_jvm_settings(settings, additionals=additionals):
            if param in res:
                res[param] += 1
            else:
                res[param] = 1

        assert res == expected

    def check_default_gc(self):
        """
        Without an explicit collector, create_jvm_settings yields the default profile and nothing from
        any other one.
        """
        jvm_settings = create_jvm_settings()

        for opt in GC_PROFILES[GC_G1]:
            assert opt in jvm_settings

        assert "-XX:+UseStringDeduplication" not in create_jvm_settings(gc_settings=GC_PROFILES[GC_SERIAL])

    @pytest.mark.parametrize('gc_settings', [GC_PROFILES[GC_SERIAL], "-XX:+UseSerialGC"])
    def check_gc_settings_accepts_list_and_string(self, gc_settings):
        """
        A stray caller passing a string keeps working.
        """
        assert "-XX:+UseSerialGC" in create_jvm_settings(gc_settings=gc_settings)

    @pytest.mark.parametrize(
        'jvm_opts',
        [
            ["-XX:+UseG1GC", "-XX:+UseZGC"],
            ["-XX:+UseSerialGC", "-XX:+UseParallelGC", "-XX:+UseZGC"],
            "-XX:+UseG1GC -XX:+UseShenandoahGC",
        ]
    )
    def check_multiple_gc_selectors_raise(self, jvm_opts):
        """
        Two enabled collectors abort the JVM at startup; catch it in Python instead.
        """
        with pytest.raises(MultipleGcSelectedError):
            validate_gc_settings(jvm_opts)

        with pytest.raises(MultipleGcSelectedError):
            merge_jvm_settings([], jvm_opts)

    @pytest.mark.parametrize(
        'jvm_opts',
        [
            ["-XX:+UseG1GC"],
            ["-XX:+UseG1GC", "-XX:-UseZGC"],
            ["-XX:+UseG1GC", "-XX:-UseG1GC", "-XX:+UseZGC"],  # last occurrence per collector wins
            # neither of these is a collector selector, despite matching on a naive pattern
            ["-XX:+UseG1GC", "-XX:+DisableExplicitGC", "-XX:+UseStringDeduplication"],
        ]
    )
    def check_single_gc_selector_passes(self, jvm_opts):
        """
        One enabled collector, however it was arrived at, is fine.
        """
        assert validate_gc_settings(jvm_opts) == jvm_opts
