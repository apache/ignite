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
from checks.support.discovery_template import discovery_spi, render_discovery_spi


class CheckTcpDiscoverySpiTemplate:
    """
    Checks the TcpDiscoverySpi bean of the node configuration.
    """
    def check_network_timeout_is_left_to_ignite_by_default(self):
        """Tests that never set the timeout keep Ignite's own default."""
        assert 'name="networkTimeout"' not in render_discovery_spi(discovery_spi())

    def check_network_timeout_reaches_the_spi(self):
        """
        IgniteConfiguration.networkTimeout does not reach discovery: the SPI has a timeout of
        its own, and it is the one a joining node waits for its join to complete.
        """
        xml = render_discovery_spi(discovery_spi(network_timeout=20_000))

        assert '<property name="networkTimeout" value="20000"/>' in xml
