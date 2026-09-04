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
# limitations under the License

"""
This module resolves the garbage collector to use from Globals.

GC selection is mutually-exclusive group replacement: a collector and its tuning flags travel together
(see GC_PROFILES in jvm_utils). It therefore has to be chosen *before* the default option list is
assembled -- patching it afterwards via jvm_opts leaves two selectors in the command line, because
merge_jvm_settings overwrites per option, not per group.

This is the single resolution point for the 'gc' global. Keep it that way.
"""

from ignitetest.services.utils.jvm_utils import DEFAULT_GC, GC_PROFILES

GC_KEY_NAME = "gc"

SERVER_ROLE = "server"
CLIENT_ROLE = "client"


def is_gc_configured(_globals: dict):
    """
    :param _globals: Globals parameters
    :return: True if the run explicitly selects a garbage collector.
    """
    return GC_KEY_NAME in (_globals or {})


def resolve_gc_settings(_globals: dict, role: str):
    """
    Gets garbage collector options from Globals. Three shapes are accepted:

    {"gc": "ZGC"}                                            -- both roles
    {"gc": {"server": "ZGC"}}                                -- servers only, clients keep the default
    {"gc": {"server": "ZGC", "client": "SERIAL"}}            -- per role
    {"gc": {"server": ["-XX:+UseZGC", "-XX:SoftMaxHeapSize=2G"]}}   -- raw options, escape hatch

    Profile names are case-insensitive. A missing role, or a missing 'gc' key, yields the DEFAULT_GC
    profile. A list value is used verbatim and bypasses profile validation -- that is the point of it.

    :param _globals: Globals parameters
    :param role: SERVER_ROLE or CLIENT_ROLE
    :return: list of JVM options selecting and tuning the collector
    """
    configured = (_globals or {}).get(GC_KEY_NAME)

    if configured is None:
        return _profile(DEFAULT_GC)

    if isinstance(configured, dict):
        configured = configured.get(role)

        if configured is None:
            return _profile(DEFAULT_GC)

    if isinstance(configured, list):
        return list(configured)

    if isinstance(configured, str):
        name = configured.upper()

        if name not in GC_PROFILES:
            raise ValueError(f"Unknown garbage collector profile '{configured}' for role '{role}'. "
                             f"Valid profiles: {', '.join(sorted(GC_PROFILES))}. "
                             f"A list of raw JVM options is also accepted.")

        return _profile(name)

    raise ValueError(f"Unexpected value for the '{GC_KEY_NAME}' global: {configured!r}. Expected a profile "
                     f"name, a list of raw JVM options, or a mapping of role to either of those.")


def _profile(name):
    return list(GC_PROFILES[name])
