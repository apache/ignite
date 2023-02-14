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
Module contains ignite version utility class.
"""
import re
from distutils.version import LooseVersion

from ignitetest import __version__


class IgniteVersion(LooseVersion):
    """
    Container for Ignite versions which makes versions simple to compare.

    distutils.version.LooseVersion (and StrictVersion) has robust comparison and ordering logic.

    Example:

        v27 = IgniteVersion("2.7.0")
        v28 = IgniteVersion("2.8.1")
        assert v28 > v27  # assertion passes!
    """

    DEV_VERSION = "dev"
    DEFAULT_PROJECT = "ignite"

    def __init__(self, vstring=None):
        if vstring == self.DEV_VERSION:
            self.project = self.DEFAULT_PROJECT
            version = vstring
        else:
            match = re.match(r'([a-zA-Z]*)-*([\d(dev)]+.*)', vstring)
            self.project = self.DEFAULT_PROJECT if not match.group(1) else match.group(1)
            version = match.group(2)

        self.is_dev = version == self.DEV_VERSION

        if self.is_dev:
            version = __version__  # we may also parse pom file to gain correct version (in future)

        super().__init__(version)

    def __str__(self):
        return "%s-%s" % (self.project, "dev" if self.is_dev else super().__str__())

    def _cmp(self, other):
        if isinstance(other, str):
            other = IgniteVersion(other)

        # todo solve comparability issues and uncomment the following
        # if self.project != other.project:
        #     raise Exception("Incomperable versons v1=%s, v2=%s because of different projects" % (self, other))

        return super()._cmp(other)

    def __repr__(self):
        return "IgniteVersion ('%s')" % str(self)


DEV_BRANCH = IgniteVersion("dev")

# 2.7.x versions
V_2_7_6 = IgniteVersion("2.7.6")
LATEST_2_7 = V_2_7_6

# 2.8.x versions
V_2_8_0 = IgniteVersion("2.8.0")
V_2_8_1 = IgniteVersion("2.8.1")
LATEST_2_8 = V_2_8_1

# 2.9.x versions
V_2_9_0 = IgniteVersion("2.9.0")
V_2_9_1 = IgniteVersion("2.9.1")
LATEST_2_9 = V_2_9_1

# 2.10.x versions
V_2_10_0 = IgniteVersion("2.10.0")
LATEST_2_10 = V_2_10_0

# 2.11.x versions
V_2_11_0 = IgniteVersion("2.11.0")
V_2_11_1 = IgniteVersion("2.11.1")
LATEST_2_11 = V_2_11_1

# 2.12.x versions
V_2_12_0 = IgniteVersion("2.12.0")
LATEST_2_12 = V_2_12_0

# 2.13.x versions
V_2_13_0 = IgniteVersion("2.13.0")
LATEST_2_13 = V_2_13_0

# 2.14.x versions
V_2_14_0 = IgniteVersion("2.14.0")
LATEST_2_14 = V_2_14_0

# if you updated the LATEST version
# please check DEV version in 'tests/ignitetest/__init__.py'
LATEST = LATEST_2_14
OLDEST = V_2_7_6
