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
import re

from ignitetest.services.utils.decorators import memoize


def ignite_jmx_mixin(node, pids):
    setattr(node, 'pids', pids)
    base_cls = node.__class__
    base_cls_name = node.__class__.__name__
    node.__class__ = type(base_cls_name, (base_cls, IgniteJmxMixin), {})


class JmxMBean(object):
    def __init__(self, client, name):
        self.client = client
        self.name = name

    def __getattr__(self, attr):
        return self.client.mbean_attribute(self.name, attr)


class JmxClient(object):
    jmx_util_cmd = 'java -jar /opt/jmxterm.jar -v silent -n'

    def __init__(self, node):
        self.node = node
        self.pid = node.pids[0]

    @memoize
    def find_mbean(self, pattern, domain='org.apache'):
        cmd = "echo $'open %s\\n beans -d %s \\n close' | %s | grep -o '%s'" \
              % (self.pid, domain, self.jmx_util_cmd, pattern)

        name = next(self.run_cmd(cmd)).strip()

        return JmxMBean(self, name)

    def mbean_attribute(self, mbean, attr):
        cmd = "echo $'open %s\\n get -b %s %s \\n close' | %s | sed 's/%s = \\(.*\\);/\\1/'" \
              % (self.pid, mbean, attr, self.jmx_util_cmd, attr)

        return iter(s.strip() for s in self.run_cmd(cmd))

    def run_cmd(self, cmd):
        return self.node.account.ssh_capture(cmd, allow_fail=False, callback=str)


class DiscoveryInfo(object):
    def __init__(self, coordinator, local_raw):
        self._local_raw = local_raw
        self._coordinator = coordinator

    @property
    def id(self):
        return self.__find__("id=([^\\s]+),")

    @property
    def coordinator(self):
        return self._coordinator

    @property
    def consistent_id(self):
        return self.__find__("consistentId=([^\\s]+),")

    @property
    def is_client(self):
        return self.__find__("isClient=([^\\s]+),") == "true"

    @property
    def order(self):
        return int(self.__find__("order=(\\d+),"))

    @property
    def int_order(self):
        val = self.__find__("intOrder=(\\d+),")
        return int(val) if val else -1

    def __find__(self, pattern):
        res = re.search(pattern, self._local_raw)
        return res.group(1) if res else None


class IgniteJmxMixin(object):
    @memoize
    def jmx_client(self):
        # noinspection PyTypeChecker
        return JmxClient(self)

    @memoize
    def id(self):
        return next(self.kernal_mbean().LocalNodeId).strip()

    def discovery_info(self):
        disco_mbean = self.disco_mbean()
        crd = next(disco_mbean.Coordinator).strip()
        local = next(disco_mbean.LocalNodeFormatted).strip()

        return DiscoveryInfo(crd, local)

    def kernal_mbean(self):
        return self.jmx_client().find_mbean('.*group=Kernal,name=IgniteKernal')

    @memoize
    def disco_mbean(self):
        disco_spi = next(self.kernal_mbean().DiscoverySpiFormatted).strip()

        if 'ZookeeperDiscoverySpi' in disco_spi:
            return self.jmx_client().find_mbean('.*group=SPIs,name=ZookeeperDiscoverySpi')
        else:
            return self.jmx_client().find_mbean('.*group=SPIs,name=TcpDiscoverySpi')
