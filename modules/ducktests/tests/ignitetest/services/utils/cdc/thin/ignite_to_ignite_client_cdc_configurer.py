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

from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.cdc.cdc_configurer import CdcConfigurer
from ignitetest.services.utils.cdc.cdc_spec import get_cdc_spec
from ignitetest.services.utils.cdc.thin.ignite_to_ignite_client_cdc_streamer_params import \
    IgniteToIgniteClientCdcStreamerParams
from ignitetest.services.utils.ignite_configuration import IgniteThinClientConfiguration


class CdcIgniteToIgniteClientConfigurer(CdcConfigurer):
    """
    Configurer for the IgniteToIgniteClientCdcStreamer
    """
    def configure_source_cluster(self, source_cluster, target_cluster, cdc_params):
        super().configure_source_cluster(source_cluster, target_cluster, cdc_params)

        source_cluster.spec = get_cdc_spec(source_cluster.spec.__class__, source_cluster)

    def get_cdc_beans(self, source_cluster, target_cluster, cdc_params):
        beans: list = super().get_cdc_beans(source_cluster, target_cluster, cdc_params)

        addresses = [target_cluster.nodes[0].account.hostname + ":" +
                     str(target_cluster.config.client_connector_configuration.port)]

        target_cluster_client_config = IgniteThinClientConfiguration(
            addresses=addresses,
            version=target_cluster.config.version)

        dummy_client = IgniteApplicationService(target_cluster.context,
                                                target_cluster_client_config,
                                                java_class_name="")
        target_cluster_client_config = dummy_client.spec.extend_config(target_cluster_client_config)

        dummy_client.free()

        params = IgniteToIgniteClientCdcStreamerParams(
            target_cluster,
            target_cluster_client_config,
            max_batch_size=cdc_params.cdc_max_batch_size,
            only_primary=cdc_params.cdc_only_primary,
            caches=cdc_params.cdc_caches
        )

        if self.class_name:
            params = params._replace(class_name=self.class_name)

        beans.append((
            "ignite_to_ignite_client_cdc_streamer.j2",
            params
        ))

        return beans


