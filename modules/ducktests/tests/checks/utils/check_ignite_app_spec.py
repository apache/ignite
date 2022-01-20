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
Checks Spec class that describes config and command line to start Ignite-aware application.
"""
from unittest.mock import Mock

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


class CheckIgniteApplicationSpec:
    """
    Checks that the passed heap memory option is not overriden by the default one.
    """

    def check_jvm_memory_option__is_not_overriden__if_passed_as_jvm_opts_string(self):
        service = mock_service()
        spec = IgniteApplicationSpec(service, jvm_opts="-Xmx256m -ea", full_jvm_opts=None)
        assert "-Xmx256m" in spec.jvm_opts

    def check_jvm_memory_option__is_not_overriden__if_passed_as_jvm_opts_list(self):
        service = mock_service()
        spec = IgniteApplicationSpec(service, jvm_opts=["-Xmx256m", "ea"], full_jvm_opts=None)
        assert "-Xmx256m" in spec.jvm_opts

    def check_jvm_memory_option__is_not_overriden__if_passed_as_full_jvm_opts_string(self):
        service = mock_service()
        spec = IgniteApplicationSpec(service, jvm_opts=None, full_jvm_opts="-Xmx256m -ea")
        assert "-Xmx256m" in spec.jvm_opts

    def check_jvm_memory_option__is_not_overriden__if_passed_as_full_jvm_opts_list(self):
        service = mock_service()
        spec = IgniteApplicationSpec(service, jvm_opts=None, full_jvm_opts=["-Xmx256m", "ea"])
        assert "-Xmx256m" in spec.jvm_opts

    def check_default_jvm_memory_option__is_used__if_none_passed_from_outside(self):
        service = mock_service()
        spec = IgniteApplicationSpec(service, jvm_opts=None, full_jvm_opts=None)
        assert "-Xmx1G" in spec.jvm_opts

    def check_jvm_memory_option__has_higher_priority__if_both_jvm_opts_and_full_jvm_opts_passed(self):
        service = mock_service()
        spec = IgniteApplicationSpec(service, jvm_opts="-Xmx512m", full_jvm_opts="-Xmx256m")
        assert "-Xmx512m" in spec.jvm_opts
