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
Stand-ins for the ducktape objects a test is handed - a logger, nodes, services and the
registry they are collected in - for checks of framework code that only reads them.

Each double is as poor as the real thing is at the point of use, so that a check fails on
code reaching for more than a test actually offers it.
"""

from types import SimpleNamespace

from ignitetest.services.utils.path import IgnitePathAware


class FakeLogger:
    """
    Collects what the code under check would have logged.
    """
    def __init__(self):
        self.messages = []

    def info(self, msg):
        """Records an info message."""
        self.messages.append(msg)

    def warn(self, msg):
        """Records a warning."""
        self.messages.append(msg)

    debug = info
    error = warn


def fake_nodes(*hostnames):
    """
    :return: Nodes carrying the account attributes that ducktape's do.
    """
    return [SimpleNamespace(account=SimpleNamespace(hostname=host, externally_routable_ip=host))
            for host in hostnames]


class FakeService:
    """
    Stands in for a non-Ignite service of the test registry, e.g. a zookeeper one: it carries
    paths of its own, which code following the Ignite services must not hand out for Ignite
    nodes.
    """
    log_dir = "/mnt/service/zk-logs"
    config_file = "/mnt/service/zookeeper.properties"

    def __init__(self, *hostnames):
        self.nodes = fake_nodes(*hostnames)

    def who_am_i(self, node):
        """Names the node the way a ducktape service does."""
        return f"{self.__class__.__name__}-{node.account.hostname}"


class FakeIgniteService(IgnitePathAware):
    """
    Stands in for an Ignite service, with the real path layout behind it.
    """
    def __init__(self, *hostnames):
        self.nodes = fake_nodes(*hostnames)

    def who_am_i(self, node):
        """Names the node the way a ducktape service does."""
        return f"{self.__class__.__name__}-{node.account.hostname}"

    @property
    def product(self):
        return "ignite-dev"

    @property
    def globals(self):
        return {}


class FakeRegistry:
    """
    Stands in for ducktape's ServiceRegistry, which is what a test hands to the framework: it
    is iterable and nothing else, so code reading it may not index it or ask it for a length.
    """
    def __init__(self, *services):
        self._services = services

    def __iter__(self):
        return iter(self._services)
