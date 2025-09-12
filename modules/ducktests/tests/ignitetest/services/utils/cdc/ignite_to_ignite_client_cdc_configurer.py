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
# limitations under the License

from typing import NamedTuple

from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.cdc.cdc_configurer import CdcConfigurer, CdcParams
from ignitetest.services.utils.ignite_aware import IgniteAwareService
from ignitetest.services.utils.ignite_configuration import IgniteThinClientConfiguration


class IgniteToIgniteClientCdcConfigurer(CdcConfigurer):
    """
    Configurer for the IgniteToIgniteClientCdcStreamer
    """
    def get_cdc_beans(self, src_cluster, dst_cluster, cdc_params, ctx):
        beans: list = super().get_cdc_beans(src_cluster, dst_cluster, cdc_params, ctx)

        addresses = [dst_cluster.nodes[0].account.hostname + ":" +
                     str(dst_cluster.config.client_connector_configuration.port)]

        target_cluster_client_config = IgniteThinClientConfiguration(
            addresses=addresses,
            version=dst_cluster.config.version)

        dummy_client = IgniteApplicationService(dst_cluster.context,
                                                target_cluster_client_config,
                                                java_class_name="")
        target_cluster_client_config = dummy_client.spec.extend_config(target_cluster_client_config)

        dummy_client.free()

        params = IgniteToIgniteClientCdcStreamerTemplateParams(
            dst_cluster,
            target_cluster_client_config,
            cdc=cdc_params
        )

        beans.append((
            "ignite_to_ignite_client_cdc_streamer.j2",
            params
        ))

        return beans


class IgniteToIgniteClientCdcStreamerTemplateParams(NamedTuple):
    target_cluster: IgniteAwareService
    target_cluster_client_config: IgniteThinClientConfiguration
    cdc: CdcParams
    name: str = "IgniteToIgniteClientCdcStreamerTemplateParams"
