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
This module contains JMX Console client and different utilities and mixins to retrieve ignite node parameters
and attributes.
"""

import re

from ignitetest.services.utils.decorators import memoize


def ignite_jmx_mixin(node, pids):
    """
    Dynamically mixin JMX attributes to Ignite service node.
    :param node: Ignite service node.
    :param pids: Ignite service node pids.
    """
    setattr(node, 'pids', pids)
    base_cls = node.__class__
    base_cls_name = node.__class__.__name__
    node.__class__ = type(base_cls_name, (base_cls, IgniteJmxMixin), {})


class JmxMBean(object):
    """
    Dynamically exposes JMX MBean attributes.
    """
    def __init__(self, client, name):
        self.client = client
        self.name = name

    def __getattr__(self, attr):
        """
        Retrieves through JMX client MBean attributes.
        :param attr: Attribute name.
        :return: Attribute value.
        """
        return self.client.mbean_attribute(self.name, attr)


class JmxClient(object):
    """JMX client, invokes jmxterm on node locally.
    """
    jmx_util_cmd = 'java -jar /opt/jmxterm.jar -v silent -n'

    def __init__(self, node):
        self.node = node
        self.pid = node.pids[0]

    @memoize
    def find_mbean(self, pattern, domain='org.apache'):
        """
        Find mbean by specified pattern and domain on node.
        :param pattern: MBean name pattern.
        :param domain: Domain of MBean
        :return: JmxMBean instance
        """
        cmd = "echo $'open %s\\n beans -d %s \\n close' | %s | grep -o '%s'" \
              % (self.pid, domain, self.jmx_util_cmd, pattern)

        name = next(self.__run_cmd(cmd)).strip()

        return JmxMBean(self, name)

    def mbean_attribute(self, mbean, attr):
        """
        Get MBean attribute.
        :param mbean: MBean name
        :param attr: Attribute name
        :return: Attribute value
        """
        cmd = "echo $'open %s\\n get -b %s %s \\n close' | %s | sed 's/%s = \\(.*\\);/\\1/'" \
              % (self.pid, mbean, attr, self.jmx_util_cmd, attr)

        return iter(s.strip() for s in self.__run_cmd(cmd))

    def __run_cmd(self, cmd):
        return self.node.account.ssh_capture(cmd, allow_fail=False, callback=str)


class DiscoveryInfo(object):
    """ Ignite service node discovery info, obtained from DiscoverySpi mbean.
    """
    def __init__(self, coordinator, local_raw):
        self._local_raw = local_raw
        self._coordinator = coordinator

    @property
    def node_id(self):
        """
        :return: Local node id.
        """
        return self.__find__("id=([^\\s]+),")

    @property
    def coordinator(self):
        """
        :return: Coordinator node id.
        """
        return self._coordinator

    @property
    def consistent_id(self):
        """
        :return: Node consistent id, if presents (only in TcpDiscovery).
        """
        return self.__find__("consistentId=([^\\s]+),")

    @property
    def is_client(self):
        """
        :return: True if node is client.
        """
        return self.__find__("isClient=([^\\s]+),") == "true"

    @property
    def order(self):
        """
        :return: Topology order.
        """
        return int(self.__find__("order=(\\d+),"))

    @property
    def int_order(self):
        """
        :return: Internal order (TcpDiscovery).
        """
        val = self.__find__("intOrder=(\\d+),")
        return int(val) if val else -1

    def __find__(self, pattern):
        res = re.search(pattern, self._local_raw)
        return res.group(1) if res else None


class IgniteJmxMixin(object):
    """
    Mixin to IgniteService node, exposing useful properties, obtained from JMX.
    """
    @memoize
    def jmx_client(self):
        """
        :return: JmxClient instance.
        """
        # noinspection PyTypeChecker
        return JmxClient(self)

    @memoize
    def node_id(self):
        """
        :return: Local node id.
        """
        return next(self.kernal_mbean().LocalNodeId).strip()

    def discovery_info(self):
        """
        :return: DiscoveryInfo instance.
        """
        disco_mbean = self.disco_mbean()
        crd = next(disco_mbean.Coordinator).strip()
        local = next(disco_mbean.LocalNodeFormatted).strip()

        return DiscoveryInfo(crd, local)

    def kernal_mbean(self):
        """
        :return: IgniteKernal MBean.
        """
        return self.jmx_client().find_mbean('.*group=Kernal,name=IgniteKernal')

    @memoize
    def disco_mbean(self):
        """
        :return: DiscoverySpi MBean.
        """
        disco_spi = next(self.kernal_mbean().DiscoverySpiFormatted).strip()

        if 'ZookeeperDiscoverySpi' in disco_spi:
            return self.jmx_client().find_mbean('.*group=SPIs,name=ZookeeperDiscoverySpi')

        return self.jmx_client().find_mbean('.*group=SPIs,name=TcpDiscoverySpi')
