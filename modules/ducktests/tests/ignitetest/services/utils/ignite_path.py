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
This module contains ignite path resolve utilities.
"""

import os

from ignitetest.tests.utils.version import get_version, IgniteVersion


class IgnitePath:
    """Path resolver for Ignite system tests which assumes the following layout:

       /opt/ignite-dev          # Current version of Ignite under test
       /opt/ignite-2.7.6        # Example of an older version of Ignite installed from tarball
       /opt/ignite-<version>    # Other previous versions of Ignite
       ...
   """
    SCRATCH_ROOT = "/mnt"
    IGNITE_INSTALL_ROOT = "/opt"

    def __init__(self, context):
        self.project = context.globals.get("project", "ignite")

    def home(self, node_or_version, project=None):
        """
        :param node_or_version: Ignite service node or IgniteVersion instance.
        :param project: Project name.
        :return: Home directory.
        """
        version = self.__version__(node_or_version)
        home_dir = project or self.project
        if version is not None:
            home_dir += "-%s" % str(version)

        return os.path.join(IgnitePath.IGNITE_INSTALL_ROOT, home_dir)

    def script(self, script_name, node_or_version, project=None):
        """
        :param script_name: Script name.
        :param node_or_version: Ignite service node or IgniteVersion instance.
        :param project: Project name.
        :return: Full path to script.
        """
        version = self.__version__(node_or_version)
        return os.path.join(self.home(version, project=project), "bin", script_name)

    @staticmethod
    def __version__(node_or_version):
        if isinstance(node_or_version, IgniteVersion):
            return node_or_version

        return get_version(node_or_version)
