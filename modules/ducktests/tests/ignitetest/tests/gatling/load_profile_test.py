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
This module contains test for load profile execution via gatling service.
"""
from ducktape.mark import defaults, parametrize

from ignitetest.services.utils import IgniteServiceType
from ignitetest.utils import ignite_versions, cluster
from ignitetest.utils.base_load_profile_test import BaseLoadProfileTest
from ignitetest.utils.version import DEV_BRANCH


class LoadProfileTest(BaseLoadProfileTest):
    """
    Executes load profile.
    """

    def get_server_config(self, ignite_version):
        return super().get_server_config(ignite_version)

    def get_client_config(self, ignite, client_type):
        return super().get_client_config(ignite, client_type)

    @cluster(num_nodes=6)
    @ignite_versions(str(DEV_BRANCH))
    @defaults(client_type=[IgniteServiceType.NODE, IgniteServiceType.THIN_CLIENT])
    @parametrize(profile="org.apache.ignite.internal.ducktest.gatling.profile.SampleProfile",
                 rps=20,
                 duration=10,
                 client_nodes=2)
    def load_profile_test(self, profile, ignite_version=str(DEV_BRANCH),
                          client_type=IgniteServiceType.NODE, rps=20, duration=10, client_nodes=2):
        self.execute_load_profile(profile, ignite_version, client_type, rps, duration, client_nodes)
