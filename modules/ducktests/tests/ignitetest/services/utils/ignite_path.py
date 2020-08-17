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


class IgnitePath:
    """Path resolver for Ignite system tests which assumes the following layout:

       /opt/ignite-dev          # Current version of Ignite under test
       /opt/ignite-2.7.6        # Example of an older version of Ignite installed from tarball
       /opt/ignite-<version>    # Other previous versions of Ignite
       ...
   """
    SCRATCH_ROOT = "/mnt"
    IGNITE_INSTALL_ROOT = "/opt"

    def __init__(self, version, project="ignite"):
        self.version = version
        home_dir = "%s-%s" % (project, str(self.version))
        self.home = os.path.join(IgnitePath.IGNITE_INSTALL_ROOT, home_dir)

    def module(self, module_name):
        """
        :param module_name: name of Ignite optional lib
        :return: absolute path to the specified module
        """
        if self.version.is_dev:
            module_path = os.path.join("modules", module_name, "target")
        else:
            module_path = os.path.join("libs", "optional", "ignite-%s" % module_name)

        return os.path.join(self.home, module_path)

    def script(self, script_name):
        """
        :param script_name: name of Ignite script
        :return: absolute path to the specified script
        """
        return os.path.join(self.home, "bin", script_name)
