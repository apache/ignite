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
This module contains ignite config classes and utilities.
"""
import os

from jinja2 import FileSystemLoader, Environment

IGNITE_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")
ZK_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "zk", "templates")
DEFAULT_IGNITE_CONF = "ignite.xml.j2"
DEFAULT_THIN_CLIENT_CONF = "thin_client_config.xml.j2"
DEFAULT_LOG4J2_CONF = "log4j2.xml.j2"

TEMPLATE_PATHES = [IGNITE_TEMPLATE_PATH, ZK_TEMPLATE_PATH]


class ConfigTemplate:
    """
    Basic configuration.
    """
    def __init__(self, path):
        env = Environment(loader=FileSystemLoader(searchpath=TEMPLATE_PATHES))
        env.filters["snake_to_camel"] = snake_to_camel

        self.template = env.get_template(path)
        self.default_params = {}

    def render(self, **kwargs):
        """
        Render configuration.
        """
        kwargs.update(self.default_params)
        unfiltered = self.template.render(**kwargs)

        return '\n'.join(filter(lambda line: line.strip(), unfiltered.split('\n')))


def snake_to_camel(snake_name):
    """
    Custom jinja2 filter to convert named from smake to camel format
    :param snake_name: name in snake format
    :return: name in camel format
    """
    components = snake_name.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])


class IgniteServerConfigTemplate(ConfigTemplate):
    """
    Ignite server node configuration.
    """
    def __init__(self, path=DEFAULT_IGNITE_CONF):
        super().__init__(path)


class IgniteClientConfigTemplate(ConfigTemplate):
    """
    Ignite client node configuration.
    """
    def __init__(self, path=DEFAULT_IGNITE_CONF):
        super().__init__(path)
        self.default_params.update(client_mode=True)


class IgniteThinClientConfigTemplate(ConfigTemplate):
    """
    Ignite client node configuration.
    """
    def __init__(self, path=DEFAULT_THIN_CLIENT_CONF):
        super().__init__(path)


class IgniteLoggerConfigTemplate(ConfigTemplate):
    """
    Ignite logger configuration.
    """
    def __init__(self):
        super().__init__(DEFAULT_LOG4J2_CONF)
