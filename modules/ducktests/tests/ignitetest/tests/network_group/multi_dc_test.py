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

from ducktape.mark import matrix

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.network_group.configuration import CrossNetworkGroupConfiguration, NetworkGroupStore
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_services
from ignitetest.tests.network_group import NetworkGroupAbstractTest
from ignitetest.utils import ignite_versions, cluster
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion

JAVA_CLIENT_CLASS_NAME = "org.apache.ignite.internal.ducktest.tests.multi_dc.BinaryMetadataUpdatesApplication"

DC_1_NAME = "DC1"
DC_2_NAME = "DC2"

class MultiDCTest(NetworkGroupAbstractTest):
    @cluster(num_nodes=5)
    @ignite_versions(str(DEV_BRANCH))
    @matrix(delay=["5ms"], loss=[0.01])
    def test_binary_meta_update_on_crd_change(self, ignite_version, delay, loss):
        self.configure_network_and_run(ignite_version=ignite_version, delay=delay, loss=loss)

    def _configure_network_group_store(self, **kwargs) -> NetworkGroupStore:
        store = super()._configure_network_group_store(**kwargs)

        dc1_dc2_cfg = CrossNetworkGroupConfiguration(delay=kwargs.get('delay'), loss=kwargs.get('loss'))
        store.set_config(DC_1_NAME, DC_2_NAME, dc1_dc2_cfg)

        return store

    def _configure_services(self, **kwargs):
        self.ign_cfg = IgniteConfiguration(
            version=IgniteVersion(kwargs.get('ignite_version')),
            peer_class_loading_enabled=False
        )

        self.svc_dc_1 = IgniteService(self.test_context, self.ign_cfg, num_nodes=3)
        self.svc_dc_2 = IgniteService(self.test_context, self.ign_cfg, num_nodes=1)

    def _configure_network_group_registry(self, **kwargs):
        return {
            DC_1_NAME: [self.svc_dc_1],
            DC_2_NAME: [self.svc_dc_2]
        }

    def _run(self, **kwargs):
        for svc in [self.svc_dc_1, self.svc_dc_2]:
            svc.start()

        client_cfg = self.ign_cfg._replace(
            client_mode=True,
            discovery_spi=from_ignite_services([self.svc_dc_1, self.svc_dc_2])
        )

        cli = IgniteApplicationService(self.test_context, client_cfg,
                                       java_class_name=JAVA_CLIENT_CLASS_NAME, num_nodes=1)
        cli.start()

        self.restart_service_node(self.svc_dc_1, 1)
        self.restart_service_node(self.svc_dc_1, 2)

        self.is_coordinator(self.svc_dc_1, 0)

        self.svc_dc_1.stop_node(self.svc_dc_1.nodes[0]) # Stop current coordinator

        total_alive = sum(len(ignite.alive_nodes) for ignite in [self.svc_dc_1, self.svc_dc_2])

        assert total_alive == 3, f"All nodes should be alive [expected=3, actual={total_alive}]"

        self.is_coordinator(self.svc_dc_2, 0)

        cli.stop()

        put_cnt = int(cli.extract_result("putCnt"))
        rps = int(cli.extract_result("rps"))

        self.logger.debug(f"Resulting load: {rps} RPS, {put_cnt} objects stored")

        assert put_cnt > 0
        assert rps > 0

        self.svc_dc_1.stop()
        self.svc_dc_2.stop()

    @staticmethod
    def restart_service_node(svc, node_idx):
        svc.stop_node(svc.nodes[node_idx])
        svc.start_node(svc.nodes[node_idx])

    @staticmethod
    def is_coordinator(svc, node_idx):
        node = svc.nodes[node_idx]

        assert node.discovery_info().is_coordinator, f"{svc.who_am_i(node)} is not a coordinator"