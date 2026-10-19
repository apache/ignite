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
import os
import re

from ignitetest.services.utils.jvm_utils import java_version, java_major_version

_NO_DEFAULT = object()

# Not allowed in a metric registry name: the single quote would break the quoted grep argument, and the
# $'...' quoting of the jmxterm commands turns the exporter's \" and \? escapes, and a lone backslash, into
# something else.
_UNSUPPORTED_REGISTRY_CHARS = frozenset("'\"\\?")


def _object_name_value(value):
    """
    Spells a property value the way IgniteUtils.escapeObjectNameValue() does: as is when it is
    purely alphanumeric or underscore, otherwise quoted, with backslash, quote, '?' and '*' escaped.
    """
    if re.fullmatch(r'[A-Za-z0-9_]+', value):
        return value

    return '"' + re.sub(r'([\\"?*])', r'\\\1', value) + '"'


def _ere_escape(value):
    """
    Escapes a literal for a POSIX extended regular expression, which is what 'grep -E' reads -
    re.escape() escapes for the Python dialect instead.
    """
    return re.sub(r'([\[\\.^$*+?(){|])', r'\\\1', value)


def metric_registry_pattern(registry):
    """
    Builds the MBean name pattern of a single metric registry.

    The JMX metric exporter splits a registry name at its FIRST dot and makes the head the
    MBean group and the tail its name: ``cache.myCache`` becomes ``group=cache`` plus
    ``name=myCache``, ``io.dataregion.default`` becomes ``group=io`` plus
    ``name="dataregion.default"``. A registry without a dot, ``snapshot`` say, gets no group at all.

    Both values are spelled as the exporter spells them - quoted and escaped unless purely
    alphanumeric - and matched literally. The properties of an MBean name are ordered
    alphabetically, so the group always comes before the name but never right next to it. And
    ``name`` sorts last of them all, so the pattern ends at the end of the line: without that
    anchor ``myCache`` would also match the registry of ``myCacheV2``.

    A registry without a group needs the negative pattern as well - an ERE has no lookahead, and
    the system view of the same name (``group=views,...,name=snapshot``) matches otherwise.

    The MBean name of a registry carries no pid, but it does carry the class loader hash, so it
    may differ between two runs of a node.

    :param registry: Metric registry name. It must not contain a single or double quote, a backslash
           or a question mark - see :data:`_UNSUPPORTED_REGISTRY_CHARS`.
    :return: Tuple of the pattern and the negative pattern to pass to :meth:`JmxClient.find_mbean`.
    """
    if _UNSUPPORTED_REGISTRY_CHARS.intersection(registry):
        raise ValueError(f"A metric registry name with a quote, a backslash or '?' is not supported: {registry}")

    group, dot, name = registry.partition('.')

    if not dot:
        return rf'.*name={_ere_escape(_object_name_value(registry))}\s*$', 'group='

    return rf'.*group={_ere_escape(_object_name_value(group))},' \
           rf'.*name={_ere_escape(_object_name_value(name))}\s*$', None


def ignite_jmx_mixin(node, service):
    """
    Dynamically mixin JMX attributes to Ignite service node. Called on every start of the node,
    which is what hands a restarted node a JMX client of its new JVM.
    :param node: Ignite service node.
    :param service: Ignite service.
    """
    setattr(node, 'pids', service.pids(node, service.main_java_class))
    setattr(node, 'install_root', service.install_root)
    setattr(node, '_jmx_client', None)

    if not isinstance(node, IgniteJmxMixin):
        base_cls = node.__class__
        base_cls_name = node.__class__.__name__
        node.__class__ = type(base_cls_name, (base_cls, IgniteJmxMixin), {})


class JmxMBean:
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

    def value(self, attr, default=_NO_DEFAULT):
        """
        Reads a single valued attribute - which is what a metric read almost always wants,
        as opposed to the raw line iterator the attribute access itself returns.

        :param attr: Attribute name.
        :param default: Value to return if the attribute reads nothing; StopIteration is raised if not passed.
        :return: Attribute value, whitespace stripped.
        """
        try:
            return next(self.client.mbean_attribute(self.name, attr)).strip()
        except StopIteration:
            if default is _NO_DEFAULT:
                raise

            return default

    def bool_value(self, attr):
        """
        :param attr: Attribute name.
        :return: Attribute value as a boolean; anything but "true" is False.
        """
        return self.value(attr).lower() == "true"

    def run(self, operation, params):
        """"
        Runs through JMX client MBean operation.
        :param operation: Operation name.
        :param params: List of parameters.
        :return: Result of operation as string.
        """
        return self.client.mbean_run(self.name, operation, params)


class JmxClient:
    """JMX client, invokes jmxterm on node locally.
    """
    def __init__(self, node, pid=None):
        """
        :param node: Node to run jmxterm on.
        :param pid: JVM to connect to; the first of the node's pids if not passed.
        """
        self.node = node
        self.install_root = node.install_root
        self.pid = pid if pid is not None else node.pids[0]
        self.java_major = java_major_version(java_version(self.node))
        self._mbeans = {}

    @property
    def jmx_util_cmd(self):
        """
        :return: jmxterm prepared command line invocation.
        """
        extra_flag = "--add-exports jdk.jconsole/sun.tools.jconsole=ALL-UNNAMED" if self.java_major >= 15 else ""

        return os.path.join(f"java {extra_flag} -jar {self.install_root}/jmxterm.jar -v silent -n")

    def find_mbean(self, pattern, negative_pattern=None, domain='org.apache'):
        """
        Find mbean by specified pattern and domain on node. The lookup is cached by this client, which
        is safe as long as the client does not outlive the JVM it was built for.
        :param pattern: MBean name pattern.
        :param negative_pattern: if passed used to filter out some MBeans
        :param domain: Domain of MBean
        :return: JmxMBean instance
        """
        key = (pattern, negative_pattern, domain)

        if key not in self._mbeans:
            cmd = "echo $'open %s\\n beans -d %s \\n close' | %s | grep -E -o '%s'" \
                  % (self.pid, domain, self.jmx_util_cmd, pattern)

            if negative_pattern:
                cmd += " | grep -E -v '%s'" % negative_pattern

            self._mbeans[key] = JmxMBean(self, next(self.__run_cmd(cmd)).strip())

        return self._mbeans[key]

    def find_metric_registry(self, registry):
        """
        :param registry: Metric registry name, e.g. ``cache.myCache`` - see :func:`metric_registry_pattern`.
        :return: JmxMBean of the metric registry.
        """
        return self.find_mbean(*metric_registry_pattern(registry))

    def mbean_attribute(self, mbean, attr):
        """
        Get MBean attribute.
        :param mbean: MBean name
        :param attr: Attribute name
        :return: Attribute value
        """
        cmd = "echo $'open %s\\n get -b %s %s \\n close' | %s | sed 's/%s = \\(.*\\);/\\1/'" \
              % (self.pid, mbean.replace(' ', '\\ '), attr, self.jmx_util_cmd, attr)

        return iter(s.strip() for s in self.__run_cmd(cmd))

    def mbean_run(self, mbean, operation, params):
        """
        Run MBean operation.
        :param mbean: MBean name
        :param operation: Operation name
        :param params: List of parameters
        :return: Result of operation as string
        """
        cmd = "echo $'open %s\\n run -b %s %s %s\\n close' | %s" \
              % (self.pid, mbean.replace(' ', '\\ '), operation, ' '.join(str(p) for p in params), self.jmx_util_cmd)

        return iter(s.strip() for s in self.__run_cmd(cmd))

    def __run_cmd(self, cmd):
        return self.node.account.ssh_capture(cmd, allow_fail=False, callback=str, combine_stderr=False)


class DiscoveryInfo:
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
        val = self.__find__("order=(\\d+),")
        return int(val) if val else -1

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


class IgniteJmxMixin:
    """
    Mixin to IgniteService node, exposing useful properties, obtained from JMX.

    Everything here is read through the JMX client of the node's CURRENT JVM: a client holds the
    pid and caches the MBean names of the JVM it was built for, so ignite_jmx_mixin() drops it on
    every start of the node and the next access builds a new one.
    """
    def jmx_client(self):
        """
        :return: JmxClient instance of the running node.
        """
        if self._jmx_client is None:
            # noinspection PyTypeChecker
            self._jmx_client = JmxClient(self)

        return self._jmx_client

    def node_id(self):
        """
        :return: Local node id.
        """
        return self.kernal_mbean().value("LocalNodeId")

    def discovery_info(self):
        """
        :return: DiscoveryInfo instance.
        """
        disco_mbean = self.disco_mbean()

        return DiscoveryInfo(disco_mbean.value("Coordinator"), disco_mbean.value("LocalNodeFormatted"))

    def kernal_mbean(self):
        """
        :return: IgniteKernal MBean.
        """
        return self.jmx_client().find_mbean('.*group=Kernal.*name=IgniteKernal')

    def metric_registry_mbean(self, registry):
        """
        :param registry: Metric registry name, e.g. ``cache.myCache`` or ``cacheGroups.myGroup``.
        :return: MBean of the metric registry.
        """
        return self.jmx_client().find_metric_registry(registry)

    def disco_mbean(self):
        """
        :return: DiscoverySpi MBean.
        """
        disco_spi = self.kernal_mbean().value("DiscoverySpiFormatted")

        if 'ZookeeperDiscoverySpi' in disco_spi:
            return self.jmx_client().find_mbean('.*group=SPIs.*name=ZookeeperDiscoverySpi')

        return self.jmx_client().find_mbean('.*group=SPIs.*name=TcpDiscoverySpi')
