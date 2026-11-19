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
Checks the discovery addresses of an MDC cluster.
"""
from ignitetest.services.mdc.mdc_cluster import DCS_3, DC_2


def _hosts(services):
    return {node.account.externally_routable_ip for svc in services for node in svc.nodes}


class CheckMdcDiscovery:
    """
    Checks which nodes the servers and clients of an MDC cluster discover through.
    """
    def check_every_server_discovers_through_every_dc(self, new_mdc):
        """Every server joins through the servers of every DC, one port per host."""
        mdc = new_mdc(dcs=DCS_3, srv_per_dc=2)

        for svc in mdc.servers.values():
            assert set(svc.config.discovery_spi.ip_finder.addresses) == _hosts(mdc.servers.values())

            assert svc.config.discovery_spi.port_range == 0

    def check_a_client_discovers_through_its_own_dc(self, new_mdc):
        """A client joins only through the servers of its own DC."""
        mdc = new_mdc(dcs=DCS_3, srv_per_dc=2)

        spi = mdc.runners[DC_2][0].config.discovery_spi

        assert set(spi.ip_finder.addresses) == _hosts([mdc.servers[DC_2]])
