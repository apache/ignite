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
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.services.utils.metrics.metrics import OPENCENSUS_TEMPLATE_FILE


class IgniteToIgniteCdcConfigurer(CdcConfigurer):
    """
    Configurer for the IgniteToIgniteCdcStreamer
    """
    def get_cdc_beans(self, src_cluster, dst_cluster, cdc_params, ctx):
        beans: list = super().get_cdc_beans(src_cluster, dst_cluster, cdc_params, ctx)

        target_cluster_client_config = dst_cluster.config._replace(
            client_mode=True,
            ssl_params=None,
            plugins=[],
            ext_beans=[],
            data_storage=None,
        )

        dummy_client = IgniteApplicationService(dst_cluster.context,
                                                target_cluster_client_config,
                                                java_class_name="")
        target_cluster_client_config = dummy_client.spec.extend_config(target_cluster_client_config)

        remove_bean_by_template_name(target_cluster_client_config.ext_beans, OPENCENSUS_TEMPLATE_FILE)

        target_cluster_client_config = target_cluster_client_config._replace(metric_exporters={})

        dummy_client.free()

        params = IgniteToIgniteCdcStreamerTemplateParams(
            dst_cluster,
            target_cluster_client_config,
            cdc=cdc_params
        )

        beans.append((
            "ignite_to_ignite_cdc_streamer.j2",
            params
        ))

        return beans


def remove_bean_by_template_name(beans, template_name):
    """
    Removes bean from the list.

    :param beans: List of beans. Bean is a tuple (template_name, params).
    :param template_name: Template file name.
    """
    bean = next((b for b in beans if b[0] == template_name), None)

    if bean:
        beans.remove(bean)


class IgniteToIgniteCdcStreamerTemplateParams(NamedTuple):
    target_cluster: IgniteAwareService
    target_cluster_client_config: IgniteConfiguration
    cdc: CdcParams
    name: str = "IgniteToIgniteCdcStreamerTemplateParams"
