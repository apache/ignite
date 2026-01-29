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
from time import sleep

from ducktape.mark import matrix

from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.services.utils.multi_dc.dc_service_manager import CrossDCConfigStore, CrossDCNetworkEmulatorConfig, \
    DCService
from ignitetest.utils import ignite_versions, cluster
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion

NUM_NODES = 12


class MultiDCTest(IgniteTest):

    @staticmethod
    def _get_cross_dc_cfg_store(delay_ms):
        """
        Cross DC configuration store
        """
        cross_dc_cfg_store = CrossDCConfigStore()

        dc1_dc2_config = CrossDCNetworkEmulatorConfig(delay_ms=delay_ms)

        cross_dc_cfg_store.set_cross_dc_config(from_dc=1, to_dc=2, config=dc1_dc2_config)

        return cross_dc_cfg_store
