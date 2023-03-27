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

"""
Module contains classes and utility methods to create discovery configuration for ignite nodes.
"""

from abc import ABCMeta, abstractmethod

from ignitetest.services.utils.ignite_aware import IgniteAwareService
from ignitetest.services.zk.zookeeper import ZookeeperService


class DiscoverySpi(metaclass=ABCMeta):
    """
    Abstract class for DiscoverySpi.
    """
    @property
    @abstractmethod
    def type(self):
        """
        Type of DiscoverySPI.
        """

    @abstractmethod
    def prepare_on_start(self, **kwargs):
        """
        Call if update before start is needed.
        """


class ZookeeperDiscoverySpi(DiscoverySpi):
    """
    ZookeeperDiscoverySpi.
    """
    def __init__(self, zoo_service, root_path):
        self.connection_string = zoo_service.connection_string()
        self.port = zoo_service.settings.client_port
        self.root_path = root_path
        self.session_timeout = zoo_service.settings.min_session_timeout

    @property
    def type(self):
        return "ZOOKEEPER"

    def prepare_on_start(self, **kwargs):
        pass


class TcpDiscoveryIpFinder(metaclass=ABCMeta):
    """
    Abstract class for TcpDiscoveryIpFinder.
    """
    @property
    @abstractmethod
    def type(self):
        """
        Type of TcpDiscoveryIpFinder.
        """

    @abstractmethod
    def prepare_on_start(self, **kwargs):
        """
        Call if update before start is needed.
        """


class TcpDiscoveryVmIpFinder(TcpDiscoveryIpFinder):
    """
    IpFinder with static ips, obtained from cluster nodes.
    """
    def __init__(self, nodes=None):
        self.addresses = TcpDiscoveryVmIpFinder.__get_addresses(nodes) if nodes else None

    @property
    def type(self):
        return 'VM'

    def prepare_on_start(self, **kwargs):
        if not self.addresses:
            cluster = kwargs.get('cluster')
            self.addresses = TcpDiscoveryVmIpFinder.__get_addresses(cluster.nodes)

    @staticmethod
    def __get_addresses(nodes):
        return [node.account.externally_routable_ip for node in nodes]


class TcpDiscoverySpi(DiscoverySpi):
    """
    TcpDiscoverySpi.
    """
    def __init__(self, ip_finder=TcpDiscoveryVmIpFinder(), port=47500, port_range=100, local_address=None):
        self.ip_finder = ip_finder
        self.port = port
        self.port_range = port_range
        self.local_address = local_address

    @property
    def type(self):
        return 'TCP'

    def prepare_on_start(self, **kwargs):
        self.ip_finder.prepare_on_start(**kwargs)


def from_ignite_cluster(cluster, subset=None):
    """
    Form TcpDiscoverySpi from cluster or its subset.
    :param cluster: IgniteService cluster
    :param subset: slice object (optional).
    :return: TcpDiscoverySpi with static ip addresses.
    """
    assert isinstance(cluster, IgniteAwareService)

    if subset:
        assert isinstance(subset, slice)
        nodes = cluster.nodes[subset]
    else:
        nodes = cluster.nodes

    return TcpDiscoverySpi(ip_finder=TcpDiscoveryVmIpFinder(nodes))


def from_zookeeper_cluster(cluster, root_path="/apacheIgnite"):
    """
    Form ZookeeperDiscoverySpi from zookeeper service cluster.
    :param cluster: ZookeeperService cluster.
    :param root_path: root ZNode path.
    :return: ZookeeperDiscoverySpi.
    """
    assert isinstance(cluster, ZookeeperService)

    return ZookeeperDiscoverySpi(cluster, root_path=root_path)
