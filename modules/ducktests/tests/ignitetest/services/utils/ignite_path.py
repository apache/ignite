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

import os

from ignitetest.version import get_version, IgniteVersion

"""
This module provides Ignite path methods
"""


class IgnitePath:
    SCRATCH_ROOT = "/mnt"
    IGNITE_INSTALL_ROOT = "/opt"

    """Path resolver for Ignite system tests which assumes the following layout:

        /opt/ignite-dev          # Current version of Ignite under test
        /opt/ignite-2.7.6        # Example of an older version of Ignite installed from tarball
        /opt/ignite-<version>    # Other previous versions of Ignite
        ...
    """

    def __init__(self, project="ignite"):
        self.project = project

    def home(self, node_or_version, project=None):
        version = self._version(node_or_version)
        home_dir = project or self.project
        if version is not None:
            home_dir += "-%s" % str(version)

        return os.path.join(IgnitePath.IGNITE_INSTALL_ROOT, home_dir)

    def script(self, script_name, node_or_version, project=None):
        version = self._version(node_or_version)
        return os.path.join(self.home(version, project=project), "bin", script_name)

    def _version(self, node_or_version):
        if isinstance(node_or_version, IgniteVersion):
            return node_or_version
        else:
            return get_version(node_or_version)
