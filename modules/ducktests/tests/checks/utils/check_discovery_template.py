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
Checks how a TcpDiscoverySpi renders into the node's Spring XML.
"""
from jinja2 import Environment, FileSystemLoader

from ignitetest.services.utils.config_template import TEMPLATE_PATHES
from ignitetest.services.utils.ignite_configuration.discovery import TcpDiscoverySpi, TcpDiscoveryVmIpFinder


def _render(**spi_args) -> str:
    """
    :return: The TcpDiscoverySpi bean as the node configuration template renders it.
    """
    ip_finder = TcpDiscoveryVmIpFinder()
    ip_finder.addresses = ["ducker02"]

    template = Environment(loader=FileSystemLoader(searchpath=TEMPLATE_PATHES)).from_string(
        '{% import "discovery_macro.j2" as m %}{{ m.tcp_discovery_spi(spi) }}')

    return template.render(spi=TcpDiscoverySpi(ip_finder=ip_finder, **spi_args))


class CheckTcpDiscoverySpiTemplate:
    """
    Checks the network timeout of the TcpDiscoverySpi bean.
    """
    def check_network_timeout_is_left_to_ignite_by_default(self):
        """No timeout given, none rendered."""
        assert 'name="networkTimeout"' not in _render()

    def check_network_timeout_reaches_the_spi(self):
        """A given timeout is rendered as the SPI's networkTimeout."""
        assert '<property name="networkTimeout" value="20000"/>' in _render(network_timeout=20_000)
