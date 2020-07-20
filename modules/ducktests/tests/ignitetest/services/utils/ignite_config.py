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
This module renders Ignite config and all related artifacts
"""

from jinja2 import FileSystemLoader, Environment

import os

DEFAULT_CONFIG_PATH = os.path.dirname(os.path.abspath(__file__)) + "/config"


class Config(object):
    def __init__(self, path):
        tmpl_dir = os.path.dirname(path)
        tmpl_file = os.path.basename(path)

        tmpl_loader = FileSystemLoader(searchpath=tmpl_dir)
        env = Environment(loader=tmpl_loader)

        self.template = env.get_template(tmpl_file)

    def render(self, **kwargs):
        return self.template.render(**kwargs)


class IgniteServerConfig(Config):
    def __init__(self, context):
        path = DEFAULT_CONFIG_PATH + "/ignite-server.xml.tmpl"
        if 'ignite_server_config_path' in context.globals:
            path = context.globals['ignite_server_config_path']
        super(IgniteServerConfig, self).__init__(path)


class IgniteClientConfig(Config):
    def __init__(self, context):
        path = DEFAULT_CONFIG_PATH + "/ignite-client.xml.tmpl"
        if 'ignite_client_config_path' in context.globals:
            path = context.globals['ignite_client_config_path']
        super(IgniteClientConfig, self).__init__(path)


class IgniteLoggerConfig(Config):
    def __init__(self):
        super(IgniteLoggerConfig, self).__init__(DEFAULT_CONFIG_PATH + "/log4j.xml.tmpl")

