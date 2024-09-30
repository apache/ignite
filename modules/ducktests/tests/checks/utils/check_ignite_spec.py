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
from unittest.mock import Mock

import pytest

from ignitetest.services.utils.ignite_spec import IgniteApplicationSpec
from ignitetest.utils.ignite_test import JFR_ENABLED


@pytest.fixture
def service():
    """
    Create mock of service.
    """
    service = Mock()
    service.log_dir = ''
    service.persistent_root = ''
    service.context.globals = {"cluster_size": 1}
    service.log_config_file = ''

    return service


"""
Checks that the JVM options passed via constructor are not overriden by the default ones.
"""


def check_default_options__are_used__if_jvm_opts_is_not_passed(service):
    spec = IgniteApplicationSpec(service)
    assert "-DIGNITE_NO_SHUTDOWN_HOOK=true" in spec.jvm_opts
    assert "-Dlog4j.configDebug=true" in spec.jvm_opts


def check_default_options__are_overriden__if_passed_as_jvm_opts_string(service):
    spec_with_default = IgniteApplicationSpec(service)
    spec_with_default_overriden = IgniteApplicationSpec(service, jvm_opts="-Dlog4j.configDebug=false")
    assert "-Dlog4j.configDebug=true" in spec_with_default.jvm_opts
    assert "-Dlog4j.configDebug=true" not in spec_with_default_overriden.jvm_opts

    assert "-Dlog4j.configDebug=false" not in spec_with_default.jvm_opts
    assert "-Dlog4j.configDebug=false" in spec_with_default_overriden.jvm_opts


def check_default_options__are_overriden__if_passed_as_jvm_opts_list(service):
    spec_with_default = IgniteApplicationSpec(service)
    spec_with_default_overriden = IgniteApplicationSpec(service, jvm_opts=["-Dlog4j.configDebug=false"])
    assert "-Dlog4j.configDebug=true" in spec_with_default.jvm_opts
    assert "-Dlog4j.configDebug=true" not in spec_with_default_overriden.jvm_opts

    assert "-Dlog4j.configDebug=false" not in spec_with_default.jvm_opts
    assert "-Dlog4j.configDebug=false" in spec_with_default_overriden.jvm_opts


def check_default_jvm_options__are_not_used__if_merge_with_default_is_false(service):
    spec = IgniteApplicationSpec(service, jvm_opts="-Xmx256m -ea", merge_with_default=False)
    assert "-Xmx256m" in spec.jvm_opts
    assert "-ea" in spec.jvm_opts
    assert len(spec.jvm_opts) == 2

    spec = IgniteApplicationSpec(service, merge_with_default=False)
    assert len(spec.jvm_opts) == 0


def check_boolean_options__go_after_default_ones_and_overwrite_them__if_passed_via_jvm_opt(service):
    service.context.globals[JFR_ENABLED] = True
    spec = IgniteApplicationSpec(service, jvm_opts="-XX:-FlightRecorder")
    assert "-XX:-FlightRecorder" in spec.jvm_opts
    assert "-XX:+FlightRecorder" in spec.jvm_opts
    assert spec.jvm_opts.index("-XX:-FlightRecorder") >\
           spec.jvm_opts.index("-XX:+FlightRecorder")


def check_colon_options__go_after_default_ones_and_overwrite_them__if_passed_via_jvm_opt(service):
    service.log_dir = "/default-path"
    spec = IgniteApplicationSpec(service, jvm_opts=["-Xlog:gc:/some-non-default-path/gc.log"])
    assert "-Xlog:gc:/some-non-default-path/gc.log" in spec.jvm_opts
    assert "-Xlog:gc*=debug,gc+stats*=debug,gc+ergo*=debug:/default-path/gc.log:uptime,time,level,tags" \
           in spec.jvm_opts
    assert spec.jvm_opts.index("-Xlog:gc:/some-non-default-path/gc.log") > \
           spec.jvm_opts.index(
               "-Xlog:gc*=debug,gc+stats*=debug,gc+ergo*=debug:/default-path/gc.log:uptime,time,level,tags")
