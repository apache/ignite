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

DEFAULT_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")
DEFAULT_IGNITE_CONF = os.path.join(DEFAULT_CONFIG_PATH, "ignite.xml.j2")
DEFAULT_THIN_CLIENT_CONF = os.path.join(DEFAULT_CONFIG_PATH, "thin_client_config.xml.j2")


class ConfigTemplate:
    """
    Basic configuration.
    """
    def __init__(self, path):
        tmpl_dir = os.path.dirname(path)
        tmpl_file = os.path.basename(path)

        tmpl_loader = FileSystemLoader(searchpath=[DEFAULT_CONFIG_PATH, tmpl_dir])
        env = Environment(loader=tmpl_loader)

        self.template = env.get_template(tmpl_file)
        self.default_params = {}

    def render(self, **kwargs):
        """
        Render configuration.
        """
        kwargs.update(self.default_params)
        unfiltered = self.template.render(**kwargs)

        return '\n'.join(filter(lambda line: line.strip(), unfiltered.split('\n')))


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
        super().__init__(os.path.join(DEFAULT_CONFIG_PATH, "log4j.xml.j2"))
