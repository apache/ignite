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
Checks Spec class that describes config and command line to start Ignite-aware service.
"""
from unittest.mock import Mock, patch

import pytest

from ignitetest.services.utils.ignite_spec import IgniteApplicationSpec


def mock_service():
    """
    Create mock of service.
    """
    service = Mock()
    service.log_dir = ''
    service.persistent_root = ''
    service.context.globals = {"cluster_size": 1}

    return service


class CheckIgniteSpec:
    """
    Checks that the JVM options passed via constructor are not overriden by the default ones.
    """

    def check_exception__raised__if_both_jvm_and_full_jvm_opts_passed(self):
        service = mock_service()
        with pytest.raises(AssertionError):
            IgniteApplicationSpec(service, jvm_opts="-Xmx256m", full_jvm_opts="-ea")

    def check_default_options__are_used__if_none_jvm_or_full_jvm_opts_passed(self):
        def default_opts():
            return ['-XX:ErrorFile=default-filename', '-Xmx512m']
        service = mock_service()
        spec = IgniteApplicationSpec(service)
        with patch.object(spec, 'get_default_jvm_opts', new=default_opts):
            assert "-XX:ErrorFile=default-filename" in spec.final_jvm_opts()
            assert "-Xmx512m" in spec.final_jvm_opts()

    def check_default_options_from_root_class__are_used__if_none_jvm_or_full_jvm_opts_passed(self):
        # May fail if set of default options in the IgniteSpec will be changed. In this case change the test as well.
        # Live with it since there is no way to mock root abstract IgniteSpec class here.
        service = mock_service()
        spec = IgniteApplicationSpec(service)
        assert "-Dlog4j.configDebug=true" in spec.final_jvm_opts()

    def check_default_options__are_overriden__if_passed_as_jvm_opts_string(self):
        def default_opts():
            return ['-XX:ErrorFile=default-filename', '-Xmx512m', '-Xms256m']
        service = mock_service()
        spec = IgniteApplicationSpec(service, jvm_opts="-Xmx256m -Xms128m -XX:ErrorFile=test-specific-filename",
                                     full_jvm_opts=None)
        with patch.object(spec, 'get_default_jvm_opts', new=default_opts):
            assert "-XX:ErrorFile=test-specific-filename" in spec.final_jvm_opts()
            assert "-Xmx256m" in spec.final_jvm_opts()
            assert "-Xms128m" in spec.final_jvm_opts()
            assert "-XX:ErrorFile=default-filename" not in spec.final_jvm_opts()
            assert "-Xmx512m" not in spec.final_jvm_opts()
            assert "-Xms256m" not in spec.final_jvm_opts()

    def check_default_options__are_overriden__if_passed_as_jvm_opts_list(self):
        def default_opts():
            return ['-XX:ErrorFile=default-filename', '-Xss512m', '-Xmn512m']
        service = mock_service()
        spec = IgniteApplicationSpec(service, jvm_opts=["-Xss256m", "-Xmn256m", "-XX:ErrorFile=test-specific-filename"],
                                     full_jvm_opts=None)
        with patch.object(spec, 'get_default_jvm_opts', new=default_opts):
            assert "-XX:ErrorFile=test-specific-filename" in spec.final_jvm_opts()
            assert "-Xss256m" in spec.final_jvm_opts()
            assert "-Xmn256m" in spec.final_jvm_opts()
            assert "-XX:ErrorFile=default-filename" not in spec.final_jvm_opts()
            assert "-Xss512m" not in spec.final_jvm_opts()
            assert "-Xmn512m" not in spec.final_jvm_opts()

    def check_default_jvm_options__are_not_used__if_full_jvm_opts_is_passed_as_string(self):
        service = mock_service()
        spec = IgniteApplicationSpec(service, jvm_opts=None, full_jvm_opts="-Xmx256m -ea")
        assert "-Xmx256m" in spec.final_jvm_opts()
        assert "-ea" in spec.final_jvm_opts()
        assert len(spec.final_jvm_opts()) == 2

    def check_default_jvm_options__are_not_used__if_full_jvm_opts_is_passed_as_list(self):
        service = mock_service()
        spec = IgniteApplicationSpec(service, jvm_opts=None, full_jvm_opts=["-Xmx256m", "-ea"])
        assert "-Xmx256m" in spec.final_jvm_opts()
        assert "-ea" in spec.final_jvm_opts()
        assert len(spec.final_jvm_opts()) == 2

    def check_boolean_options__go_after_default_ones_and_overwrite_them__if_passed_via_jvm_opt(self):
        def default_opts():
            return ['-XX:+ExtensiveErrorReports']
        service = mock_service()
        spec = IgniteApplicationSpec(service, jvm_opts="-XX:-ExtensiveErrorReports")
        with patch.object(spec, 'get_default_jvm_opts', new=default_opts):
            assert "-XX:-ExtensiveErrorReports" in spec.final_jvm_opts()
            assert "-XX:+ExtensiveErrorReports" in spec.final_jvm_opts()
            assert spec.final_jvm_opts().index("-XX:-ExtensiveErrorReports") >\
                   spec.final_jvm_opts().index("-XX:+ExtensiveErrorReports")

    def check_colon_options__goes_after_default_ones_and_overwrite_them__if_passed_via_jvm_opt(self):
        def default_opts():
            return ['-Xshare:on']
        service = mock_service()
        spec = IgniteApplicationSpec(service, jvm_opts="-Xshare:off")
        with patch.object(spec, 'get_default_jvm_opts', new=default_opts):
            assert "-Xshare:on" in spec.final_jvm_opts()
            assert "-Xshare:off" in spec.final_jvm_opts()
            assert spec.final_jvm_opts().index("-Xshare:off") > \
                   spec.final_jvm_opts().index("-Xshare:on")
