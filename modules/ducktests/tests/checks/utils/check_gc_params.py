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
Checks resolution of the 'gc' global into garbage collector options.
"""

import pytest

from ignitetest.services.utils.gc_params import CLIENT_ROLE, SERVER_ROLE, resolve_gc_settings
from ignitetest.services.utils.jvm_utils import GC_PROFILES, GC_G1, GC_SERIAL, GC_Z


class CheckGcParams:
    """
    Checks the 'gc' global parser.
    """

    @pytest.mark.parametrize('role', [SERVER_ROLE, CLIENT_ROLE])
    def check_default__when_gc_global_absent(self, role):
        """
        The no-op path: without the global both roles get G1.
        """
        assert resolve_gc_settings({}, role) == GC_PROFILES[GC_G1]
        assert resolve_gc_settings({"cluster_size": 3}, role) == GC_PROFILES[GC_G1]
        assert resolve_gc_settings(None, role) == GC_PROFILES[GC_G1]

    @pytest.mark.parametrize('role', [SERVER_ROLE, CLIENT_ROLE])
    def check_bare_string__applies_to_both_roles(self, role):
        """
        Bare string is sugar for {"server": X, "client": X}.
        """
        assert resolve_gc_settings({"gc": "ZGC"}, role) == GC_PROFILES[GC_Z]

    def check_per_role_mapping(self):
        """
        Each role resolves independently.
        """
        _globals = {"gc": {"server": "ZGC", "client": "SERIAL"}}

        assert resolve_gc_settings(_globals, SERVER_ROLE) == GC_PROFILES[GC_Z]
        assert resolve_gc_settings(_globals, CLIENT_ROLE) == GC_PROFILES[GC_SERIAL]

    def check_omitted_role__falls_back_to_default(self):
        """
        A role missing from the mapping keeps the default collector.
        """
        _globals = {"gc": {"server": "ZGC"}}

        assert resolve_gc_settings(_globals, SERVER_ROLE) == GC_PROFILES[GC_Z]
        assert resolve_gc_settings(_globals, CLIENT_ROLE) == GC_PROFILES[GC_G1]

    @pytest.mark.parametrize('name', ["zgc", "ZGC", "Zgc", "shenandoah", "Parallel"])
    def check_profile_names__are_case_insensitive(self, name):
        """
        Profile names are matched case-insensitively.
        """
        assert resolve_gc_settings({"gc": name}, SERVER_ROLE) == GC_PROFILES[name.upper()]

    def check_raw_list__is_passed_through_verbatim(self):
        """
        A list bypasses the registry entirely -- the documented escape hatch.
        """
        raw = ["-XX:+UseZGC", "-XX:SoftMaxHeapSize=2G"]

        assert resolve_gc_settings({"gc": raw}, SERVER_ROLE) == raw
        assert resolve_gc_settings({"gc": {"server": raw}}, SERVER_ROLE) == raw
        assert resolve_gc_settings({"gc": {"server": raw}}, CLIENT_ROLE) == GC_PROFILES[GC_G1]

    def check_raw_list__is_not_aliased_to_the_globals(self):
        """
        The caller must not be able to mutate the globals through the returned list.
        """
        raw = ["-XX:+UseZGC"]

        resolved = resolve_gc_settings({"gc": raw}, SERVER_ROLE)
        resolved.append("-XX:SoftMaxHeapSize=2G")

        assert raw == ["-XX:+UseZGC"]

    def check_profile__is_not_aliased_to_the_registry(self):
        """
        Mutating a resolved profile must not corrupt the registry for the next service.
        """
        resolved = resolve_gc_settings({"gc": "SERIAL"}, SERVER_ROLE)
        resolved.append("-XX:SoftMaxHeapSize=2G")

        assert resolve_gc_settings({"gc": "SERIAL"}, SERVER_ROLE) == GC_PROFILES[GC_SERIAL]

    def check_unknown_profile__raises_and_lists_valid_names(self):
        """
        An unknown name fails loudly instead of silently falling back to G1.
        """
        with pytest.raises(ValueError) as err:
            resolve_gc_settings({"gc": "CMS"}, SERVER_ROLE)

        for name in GC_PROFILES:
            assert name in str(err.value)

    def check_unexpected_value__raises(self):
        """
        Neither a profile name, nor raw options, nor a role mapping.
        """
        with pytest.raises(ValueError):
            resolve_gc_settings({"gc": 42}, SERVER_ROLE)

        with pytest.raises(ValueError):
            resolve_gc_settings({"gc": {"server": 42}}, SERVER_ROLE)

    @pytest.mark.parametrize('name', [name for name in GC_PROFILES if name != GC_G1])
    def check_non_g1_profiles__carry_no_g1_only_flags(self, name):
        """
        MaxGCPauseMillis flips ParallelGC into adaptive pause-goal sizing, and UseStringDeduplication is
        G1-only until JDK 18. Neither may leak into another collector's profile.
        """
        resolved = resolve_gc_settings({"gc": name}, SERVER_ROLE)

        assert not any("MaxGCPauseMillis" in opt for opt in resolved)
        assert not any("UseStringDeduplication" in opt for opt in resolved)

    @pytest.mark.parametrize('name', list(GC_PROFILES))
    def check_every_profile__selects_exactly_one_collector(self, name):
        """
        A profile is a mutually exclusive group -- exactly one selector, no spaces or quotes (the options
        are interpolated into a remote shell command).
        """
        resolved = resolve_gc_settings({"gc": name}, SERVER_ROLE)

        assert len([opt for opt in resolved if opt.startswith("-XX:+Use") and opt.endswith("GC")]) == 1
        assert not any(" " in opt or "'" in opt or '"' in opt for opt in resolved)
