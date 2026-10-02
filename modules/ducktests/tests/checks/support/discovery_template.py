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
The discovery SPI bean of a node configuration, rendered for checks of what reaches the node.
"""
from jinja2 import Environment, FileSystemLoader

from ignitetest.services.utils.config_template import TEMPLATE_PATHES
from ignitetest.services.utils.ignite_configuration.discovery import TcpDiscoverySpi, TcpDiscoveryVmIpFinder

HOSTS = ["ducker02", "ducker03"]


def discovery_spi(**kwargs) -> TcpDiscoverySpi:
    """
    :return: A TcpDiscoverySpi over HOSTS. Its ip finder is its own: the constructor's default
             one is shared by every TcpDiscoverySpi built without one.
    """
    ip_finder = TcpDiscoveryVmIpFinder()
    ip_finder.addresses = list(HOSTS)

    return TcpDiscoverySpi(ip_finder=ip_finder, **kwargs)


def render_discovery_spi(spi: TcpDiscoverySpi) -> str:
    """
    :return: The TcpDiscoverySpi bean exactly as the node configuration template renders it.
    """
    env = Environment(loader=FileSystemLoader(searchpath=TEMPLATE_PATHES))

    template = env.from_string('{% import "discovery_macro.j2" as m %}{{ m.tcp_discovery_spi(spi) }}')

    return template.render(spi=spi)
