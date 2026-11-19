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

from ignitetest.services.ignite import IgniteService
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.utils import ignite_versions, cluster
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion

DC_1_NAME = "DC1"
DC_2_NAME = "DC2"

class MultiDCTest(IgniteTest):
    @cluster(num_nodes=3)
    @ignite_versions(str(DEV_BRANCH))
    def test_coordinator_change(self, ignite_version):
        ign_cfg = IgniteConfiguration(version=IgniteVersion(ignite_version))

        jvm_opts_dc_1 = [f"-DIGNITE_DATA_CENTER_ID={DC_1_NAME}"]
        jvm_opts_dc_2 = [f"-DIGNITE_DATA_CENTER_ID={DC_2_NAME}"]

        self.svc_dc_1_crd = IgniteService(self.test_context, ign_cfg, num_nodes=1, jvm_opts=jvm_opts_dc_1)
        self.svc_dc_1_crd_sub = IgniteService(self.test_context, ign_cfg, num_nodes=1, jvm_opts=jvm_opts_dc_1)
        self.svc_dc_2 = IgniteService(self.test_context, ign_cfg, num_nodes=1, jvm_opts=jvm_opts_dc_2)

        for svc in [self.svc_dc_1_crd, self.svc_dc_2]:
            svc.start()

        self.svc_dc_1_crd_sub.start()

        self.is_coordinator(self.svc_dc_1_crd, 0)

        self.svc_dc_1_crd.stop()

        self.is_coordinator(self.svc_dc_2, 0) # self.svc_dc_1_crd_sub is not a coordinator

        self.svc_dc_1_crd_sub.stop()
        self.svc_dc_2.stop()

    @staticmethod
    def is_coordinator(svc, node_idx):
        node = svc.nodes[node_idx]

        assert node.discovery_info().is_coordinator, f"{svc.who_am_i(node)} is not a coordinator"
