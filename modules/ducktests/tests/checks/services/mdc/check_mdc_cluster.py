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
Checks the MDC fixture without a cluster.
"""
import logging
from itertools import combinations, count
from types import SimpleNamespace

import pytest

from checks.support.ducktape_doubles import fake_nodes
from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.mdc import mdc_cluster
from ignitetest.services.mdc.mdc_cluster import MdcCluster, cross_dc_network, DCS_2, DCS_3, DC_1, DC_2
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration

_HOSTS = count(1)


class FakeIgniteService(IgniteService):
    """
    A server service on nodes of its own, built without ducktape.
    """
    def __init__(self, context, config, num_nodes=1, **kwargs):
        self.config = config
        self.nodes = fake_nodes(*(f"ducker{next(_HOSTS)}" for _ in range(num_nodes)))


class FakeAppService(IgniteApplicationService):
    """
    An application service on a node of its own, built without ducktape.
    """
    def __init__(self, context, config, **kwargs):
        self.config = config
        self.nodes = fake_nodes(f"ducker{next(_HOSTS)}")


@pytest.fixture
def new_mdc(monkeypatch):
    """
    :return: A factory of MdcCluster over fake services.
    """
    monkeypatch.setattr(mdc_cluster, "IgniteService", FakeIgniteService)
    monkeypatch.setattr(mdc_cluster, "IgniteApplicationService", FakeAppService)

    test = SimpleNamespace(test_context=SimpleNamespace(globals={}), logger=logging.getLogger(__name__))

    return lambda **kwargs: MdcCluster(test, "dev", **kwargs)


def _hosts(services):
    return {node.account.externally_routable_ip for svc in services for node in svc.nodes}


class CheckMdcDiscovery:
    """
    Checks which nodes the servers and clients of an MDC cluster discover through.
    """
    def check_every_server_discovers_through_every_dc(self, new_mdc):
        """Any DC can restart and rejoin through the others."""
        mdc = new_mdc(dcs=DCS_3, srv_per_dc=2)

        for svc in mdc.servers.values():
            assert set(svc.config.discovery_spi.ip_finder.addresses) == _hosts(mdc.servers.values())

            assert svc.config.discovery_spi.port_range == 0

    def check_a_client_discovers_through_its_own_dc(self, new_mdc):
        """A client started in a cut DC must not wait on the other DCs."""
        mdc = new_mdc(dcs=DCS_3, srv_per_dc=2)

        spi = mdc.runners[DC_2][0].config.discovery_spi

        assert set(spi.ip_finder.addresses) == _hosts([mdc.servers[DC_2]])


class CheckMdcTimeouts:
    """
    Checks that each timeout parameter reaches only its own setting.
    """
    def check_a_default_cluster_sets_no_timeout(self, new_mdc):
        """A default cluster keeps the framework's node timeout and Ignite's discovery one."""
        mdc = new_mdc()

        assert mdc.ignite_config.network_timeout == IgniteConfiguration().network_timeout

        assert mdc.servers[DC_1].config.discovery_spi.network_timeout is None

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


class CheckMdcCacheParams:
    """
    Checks the parameters an MDC cache is created with.
    """
    @pytest.mark.parametrize(["dcs", "expected"], [
        (DCS_2, {"dcsNum": 2, "mainDc": DC_1, "topologyValidator": True}),
        (DCS_3, {"dcsNum": 3, "datacenters": list(DCS_3), "topologyValidator": True})])
    def check_the_dc_count_picks_the_validator_mode(self, new_mdc, dcs, expected):
        """An even DC count is validated against the main DC, an odd one by majority."""
        assert new_mdc(dcs=dcs)._cache_params() == expected


class CheckMdcNetwork:
    """
    Checks the cross-DC impairments.
    """
    def check_the_delay_reaches_every_dc_pair(self):
        """Every pair of DCs gets the delay, in both directions."""
        mdc = SimpleNamespace(dcs=DCS_3, network_registry=lambda: {dc: [] for dc in DCS_3})

        store = cross_dc_network(None, mdc, delay_ms=100).network_group_store

        for dc_a, dc_b in combinations(DCS_3, 2):
            assert store.get_config(dc_a, dc_b).delay == "100ms"

            assert store.get_config(dc_b, dc_a).delay == "100ms"
