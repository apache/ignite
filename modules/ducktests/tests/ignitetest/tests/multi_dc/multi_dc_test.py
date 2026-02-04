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
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.services.utils.ignite_configuration.cache import CacheConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_services
from ignitetest.services.utils.multi_dc.dc_service_manager import CrossDCConfigStore, CrossDCNetworkEmulatorConfig, \
    DCService
from ignitetest.utils import ignite_versions, cluster
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion

TEST_CACHE_NAME = "replicated-cache"
JAVA_CLIENT_CLASS_NAME = "org.apache.ignite.internal.ducktest.tests.multi_dc.BinaryMetadataUpdatesApplication"


class MultiDCTest(IgniteTest):
    @cluster(num_nodes=5)
    @ignite_versions(str(DEV_BRANCH))
    @matrix(delay_ms=[10, 20, 50, 100, 500], pacing_ms=[10])
    def test_binary_meta_update_on_crd_change(self, ignite_version, delay_ms, pacing_ms):
        """
        Test that DCServiceManager correctly start and stop for 2 DCs
        """
        cross_dc_cfg_store = self._get_cross_dc_cfg_store(delay_ms=delay_ms)

        with DCService.scope(self.test_context, cross_dc_cfg_store) as mgr:
            ignite_cfg = self._get_ignite_cfg(ignite_version=ignite_version)

            svc_1 = IgniteService(self.test_context, ignite_cfg, num_nodes=1)
            svc_2 = IgniteService(self.test_context, ignite_cfg, num_nodes=1)
            svc_3 = IgniteService(self.test_context, ignite_cfg, num_nodes=1)
            svc_4 = IgniteService(self.test_context, ignite_cfg, num_nodes=1)

            mgr.start_service(svc_name="node_1", dc_idx=1, svc=svc_1)
            mgr.start_service(svc_name="node_2", dc_idx=2, svc=svc_2)
            mgr.start_service(svc_name="node_3", dc_idx=1, svc=svc_3)
            mgr.start_service(svc_name="node_4", dc_idx=1, svc=svc_4)

            client_cfg = ignite_cfg._replace(
                client_mode=True,
                discovery_spi=from_ignite_services([svc_1, svc_2, svc_3, svc_4])
            )

            cli = self._get_cache_metadata_update_app(client_cfg, pacing_ms)

            cli.start()

            mgr.stop_service(svc_name="node_3")
            mgr.start_service(svc_name="node_3", dc_idx=1, svc=svc_3)

            mgr.stop_service(svc_name="node_1")

            total_alive = sum(len(ignite.alive_nodes) for ignite in [svc_1, svc_2, svc_3, svc_4])

            assert total_alive == 3, f"All nodes should be alive [expected=3, actual={total_alive}]"

            self._check_coordinator(svc_2)

            cli.stop()

            assert int(cli.extract_result("putCnt")) > 0

            mgr.stop_service(svc_name="node_2")
            mgr.stop_service(svc_name="node_3")
            mgr.stop_service(svc_name="node_4")

    def _get_cache_metadata_update_app(self, client_config, pacing_ms):
        app_params = {
            "cacheName": TEST_CACHE_NAME,
            "pacing": pacing_ms
        }

        return IgniteApplicationService(self.test_context, client_config, java_class_name=JAVA_CLIENT_CLASS_NAME,
                                        num_nodes=1, params=app_params)

    @staticmethod
    def _check_coordinator(crd_svc):
        crd = crd_svc.nodes[0]

        crd_discovery_info = crd.discovery_info()

        order = crd_discovery_info.order()
        int_order = crd_discovery_info.int_order()
        crd = crd_discovery_info.coordinator()

        assert order == 1, f"Expected coordinator order is wrong [exp=1, actual={order}]"
        assert int_order == 2, f"Expected coordinator internal order is wrong [exp=2, actual={int_order}]"
        assert crd == "node-2", f"Expected coordinator is wrong [exp=node-2, actual={crd}]"

    @staticmethod
    def _get_ignite_cfg(ignite_version):
        """
        Ignite service configuration
        """
        cache_cfg = CacheConfiguration(name=TEST_CACHE_NAME, cache_mode='REPLICATED')

        return IgniteConfiguration(
                version=IgniteVersion(ignite_version),
                peer_class_loading_enabled=False,
                caches=[cache_cfg]
            )

    @staticmethod
    def _get_cross_dc_cfg_store(delay_ms):
        """
        Cross DC configuration store
        """
        cross_dc_cfg_store = CrossDCConfigStore()

        dc1_dc2_config = CrossDCNetworkEmulatorConfig(delay_ms=delay_ms)

        cross_dc_cfg_store.set_cross_dc_config(from_dc=1, to_dc=2, config=dc1_dc2_config)

        return cross_dc_cfg_store
