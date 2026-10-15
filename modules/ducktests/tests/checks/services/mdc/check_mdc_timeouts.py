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
Checks the timeouts of an MDC cluster.
"""
from ignitetest.services.mdc.mdc_cluster import DC_1
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration


class CheckMdcTimeouts:
    """
    Checks that each timeout parameter reaches only its own setting.
    """
    def check_the_node_timeout_reaches_only_the_nodes(self, new_mdc):
        """The node timeout leaves discovery alone."""
        mdc = new_mdc(network_timeout=20_000)

        assert mdc.ignite_config.network_timeout == 20_000

        assert mdc.servers[DC_1].config.discovery_spi.network_timeout is None

    def check_the_discovery_timeout_reaches_servers_and_clients(self, new_mdc):
        """The discovery timeout leaves the node timeout alone."""
        mdc = new_mdc(discovery_network_timeout=20_000)

        assert mdc.ignite_config.network_timeout == IgniteConfiguration().network_timeout

        assert mdc.servers[DC_1].config.discovery_spi.network_timeout == 20_000

        assert mdc.runners[DC_1][0].config.discovery_spi.network_timeout == 20_000
