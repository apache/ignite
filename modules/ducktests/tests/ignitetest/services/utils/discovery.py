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


class ZookeeperDiscoverySpi(DiscoverySpi):
    """
    ZookeeperDiscoverySpi.
    """
    def __init__(self, connection_string, root_path):
        self.connection_string = connection_string
        self.root_path = root_path

    @property
    def type(self):
        return "ZOOKEEPER"


class TcpDiscoverySpi(DiscoverySpi):
    """
    TcpDiscoverySpi.
    """
    def __init__(self, ip_finder):
        self.ip_finder = ip_finder

    @property
    def type(self):
        return 'TCP'


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


class TcpDiscoveryVmIpFinder(TcpDiscoveryIpFinder):
    """
    IpFinder with static ips, obtained from cluster nodes.
    """
    def __init__(self, nodes):
        self.addresses = [node.account.externally_routable_ip for node in nodes]

    @property
    def type(self):
        return 'VM'


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

    return ZookeeperDiscoverySpi(cluster.connection_string(), root_path=root_path)
