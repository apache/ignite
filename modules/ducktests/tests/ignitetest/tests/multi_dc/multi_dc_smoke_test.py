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
This module contains smoke tests that checks that DCServiceManager work
"""
import socket

from ducktape.mark import defaults
from ignitetest.services.ignite import IgniteService
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.utils import ignite_versions, cluster
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion

from isetest.services.utils.multi_dc.dc_service_manager import DCService, CrossDCConfigStore, \
    CrossDCNetworkEmulatorConfig
from isetest.tests.multi_dc.utils import parse_iptables_marks


class SmokeDCServiceManagerTest(IgniteTest):
    @cluster(num_nodes=4)
    @ignite_versions(str(DEV_BRANCH))
    @defaults(check_broken=[True, False])
    def test_mdc_cluster_start_stop(self, ignite_version, check_broken):
        """
        Test that DCServiceManager correctly start and stop for 2 DCs
        """
        cross_dc_config_store = CrossDCConfigStore()

        dc1_dc2_config = CrossDCNetworkEmulatorConfig(delay_ms=20)

        cross_dc_config_store.set_cross_dc_config(1, 2, config=dc1_dc2_config)

        ignite_cfg = IgniteConfiguration(version=IgniteVersion(ignite_version))

        svc_1 = IgniteService(self.test_context, ignite_cfg, num_nodes=1)
        svc_2 = IgniteService(self.test_context, ignite_cfg, num_nodes=1)
        svc_3 = IgniteService(self.test_context, ignite_cfg, num_nodes=1)
        svc_4 = IgniteService(self.test_context, ignite_cfg, num_nodes=1)

        with DCService.scope(self.test_context, cross_dc_config_store) as mgr:
            mgr.start_service(svc_name="node1", dc_idx=1, svc=svc_1)
            mgr.start_service(svc_name="node2", dc_idx=2, svc=svc_2)
            mgr.start_service(svc_name="node3", dc_idx=1, svc=svc_3)
            mgr.start_service(svc_name="node4", dc_idx=1, svc=svc_4)

            # client start

            mgr.stop_service(svc_name="node3")
            mgr.stop_service(svc_name="node4")

            mgr.start_service(svc_name="node3", dc_idx=1, svc=svc_3)
            mgr.start_service(svc_name="node4", dc_idx=1, svc=svc_4)

            mgr.stop_service(svc_name="node1")

            # check topology

            # check coordinator

            # check errors on client and server

            # client stop

            mgr.stop_service(svc_name="node2")
            mgr.stop_service(svc_name="node3")
            mgr.stop_service(svc_name="node4")
