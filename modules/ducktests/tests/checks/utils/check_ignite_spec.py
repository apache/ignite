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

from ignitetest.services.utils import IgniteServiceType
from ignitetest.services.utils.gc_params import CLIENT_ROLE, SERVER_ROLE
from ignitetest.services.utils.ignite_spec import IgniteApplicationSpec, IgniteNodeSpec, service_role
from ignitetest.services.utils.jvm_utils import GC_PROFILES, GC_G1, MultipleGcSelectedError
from ignitetest.utils.ignite_test import JFR_ENABLED


def mock_service(service_type=IgniteServiceType.NODE, client_mode=False):
    """
    Create mock of service.
    """
    service = Mock()
    service.log_dir = ''
    service.persistent_root = ''
    service.context.globals = {"cluster_size": 1}
    service.log_config_file = ''
    service.config.service_type = service_type
    service.config.client_mode = client_mode

    return service


@pytest.fixture
def service():
    """
    Mock of a server node service.
    """
    return mock_service()


@pytest.fixture
def client_service():
    """
    Mock of a client node service.
    """
    return mock_service(client_mode=True)


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


"""
Checks GC selection via the 'gc' global.
"""


@pytest.mark.parametrize(
    'service_type,client_mode,expected',
    [
        [IgniteServiceType.NODE, False, SERVER_ROLE],
        [IgniteServiceType.NODE, True, CLIENT_ROLE],       # IgniteService can run in client mode
        [IgniteServiceType.THIN_CLIENT, None, CLIENT_ROLE],  # client_mode absent on thin configs
        [IgniteServiceType.THIN_JDBC, None, CLIENT_ROLE],
        [IgniteServiceType.NONE, None, CLIENT_ROLE],
    ]
)
def check_service_role__is_determined_semantically(service_type, client_mode, expected):
    assert service_role(mock_service(service_type, client_mode)) == expected


def check_g1__is_used__if_gc_global_is_absent(service):
    spec = IgniteNodeSpec(service)
    for opt in GC_PROFILES[GC_G1]:
        assert opt in spec.jvm_opts


def check_gc_global__applies_to_both_roles__if_passed_as_bare_string(service, client_service):
    service.context.globals["gc"] = "ZGC"
    client_service.context.globals["gc"] = "ZGC"

    for spec in (IgniteNodeSpec(service), IgniteNodeSpec(client_service)):
        assert "-XX:+UseZGC" in spec.jvm_opts
        assert "-XX:+UseG1GC" not in spec.jvm_opts


def check_gc_global__applies_per_role__if_passed_as_mapping(service, client_service):
    gc_global = {"server": "ZGC", "client": "SERIAL"}
    service.context.globals["gc"] = gc_global
    client_service.context.globals["gc"] = gc_global

    server_spec = IgniteNodeSpec(service)
    client_spec = IgniteNodeSpec(client_service)

    assert "-XX:+UseZGC" in server_spec.jvm_opts
    assert "-XX:+UseSerialGC" in client_spec.jvm_opts

    for spec in (server_spec, client_spec):
        assert "-XX:+UseG1GC" not in spec.jvm_opts
        assert "-XX:+UseStringDeduplication" not in spec.jvm_opts
        assert not any("MaxGCPauseMillis" in opt for opt in spec.jvm_opts)


def check_string_deduplication__is_not_applied__under_non_g1_collectors(service):
    assert "-XX:+UseStringDeduplication" in IgniteNodeSpec(service).jvm_opts

    service.context.globals["gc"] = "PARALLEL"
    assert "-XX:+UseStringDeduplication" not in IgniteNodeSpec(service).jvm_opts


def check_conflicting_gc_selectors__raise__if_gc_is_also_passed_via_jvm_opts(service):
    service.context.globals["gc"] = "ZGC"

    with pytest.raises(MultipleGcSelectedError) as err:
        IgniteNodeSpec(service, jvm_opts=["-XX:+UseSerialGC"])

    assert "-XX:+UseZGC" in str(err.value)
    assert "-XX:+UseSerialGC" in str(err.value)


def check_conflicting_gc_selectors__raise__if_both_are_passed_via_jvm_opts(service):
    with pytest.raises(MultipleGcSelectedError):
        IgniteNodeSpec(service, jvm_opts=["-XX:+UseSerialGC", "-XX:+UseParallelGC"], merge_with_default=False)


def check_disabled_gc_selector__does_not_conflict(service):
    """
    -XX:-UseG1GC turns G1 off, so the collector picked afterwards is the only one enabled.
    """
    spec = IgniteNodeSpec(service, jvm_opts=["-XX:-UseG1GC", "-XX:+UseSerialGC"])

    assert "-XX:+UseSerialGC" in spec.jvm_opts


def check_gc_global__does_not_apply__if_merge_with_default_is_false(service):
    service.context.globals["gc"] = "ZGC"

    spec = IgniteNodeSpec(service, jvm_opts="-XX:+UseSerialGC", merge_with_default=False)

    assert spec.jvm_opts == ["-XX:+UseSerialGC"]
    service.logger.warning.assert_called_once()


"""
Checks that a spec keeps the caller's delta separate from its own resolution, so another service can
inherit the delta without inheriting role-dependent options. See the CDC path.
"""


def check_user_jvm_opts__holds_the_delta__not_the_resolution(service):
    spec = IgniteNodeSpec(service, jvm_opts=["-Xmx8G", "-DFOO=bar"])

    assert spec.user_jvm_opts == ["-Xmx8G", "-DFOO=bar"]
    assert spec.merge_with_default is True

    assert "-Xmx8G" in spec.jvm_opts
    assert "-XX:+UseG1GC" in spec.jvm_opts  # resolution is richer than the delta


def check_user_jvm_opts__splits_a_string_delta(service):
    assert IgniteNodeSpec(service, jvm_opts="-Xmx8G -ea").user_jvm_opts == ["-Xmx8G", "-ea"]


@pytest.mark.parametrize('merge_with_default', [True, False])
def check_rebuild_as__reproduces_the_original_resolution(service, merge_with_default):
    spec = IgniteNodeSpec(service, jvm_opts=["-Xmx8G"], merge_with_default=merge_with_default)

    assert spec.rebuild_as(IgniteNodeSpec).jvm_opts == spec.jvm_opts


def check_rebuild_as__does_not_leak_gc_across_roles(client_service):
    """
    The CDC regression: a client-role service inheriting a server cluster's delta must keep its own
    collector, and must end up with exactly one selector.
    """
    server = mock_service()
    server.context.globals["gc"] = {"server": "ZGC", "client": "SERIAL"}
    client_service.context.globals["gc"] = {"server": "ZGC", "client": "SERIAL"}

    server_spec = IgniteNodeSpec(server, jvm_opts=["-Xmx8G"])

    # What ignite_to_kafka_cdc_helper does: hand the destination cluster's delta to a client service.
    inherited = IgniteNodeSpec(client_service, jvm_opts=server_spec.user_jvm_opts)

    assert "-XX:+UseZGC" in server_spec.jvm_opts
    assert "-XX:+UseSerialGC" in inherited.jvm_opts
    assert "-XX:+UseZGC" not in inherited.jvm_opts
    assert "-Xmx8G" in inherited.jvm_opts  # the delta itself is still inherited

    for spec in (server_spec, inherited):
        assert len([opt for opt in spec.jvm_opts if opt.startswith("-XX:+Use") and opt.endswith("GC")]) == 1


"""
Checks assembly of spec-specific defaults.
"""


def check_application_spec__has_matched_heap_bounds(service):
    spec = IgniteApplicationSpec(service)

    assert "-Xmx1G" in spec.jvm_opts
    assert "-Xms1G" in spec.jvm_opts
    assert len([opt for opt in spec.jvm_opts if opt.startswith("-Xmx")]) == 1
    assert len([opt for opt in spec.jvm_opts if opt.startswith("-Xms")]) == 1


def check_heap_override__from_jvm_opts__still_wins(service):
    spec = IgniteApplicationSpec(service, jvm_opts=["-Xmx4G", "-Xms4G"])

    assert "-Xmx4G" in spec.jvm_opts
    assert "-Xms4G" in spec.jvm_opts
    assert "-Xmx1G" not in spec.jvm_opts
    assert "-Xms1G" not in spec.jvm_opts
