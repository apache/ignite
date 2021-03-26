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
Checks DEV_BRANCH version.
"""

from ignitetest.utils.version import IgniteVersion, DEV_BRANCH, LATEST
from ignitetest import __version__


# pylint: disable=no-self-use
class CheckDevVersion:
    """
    Checks developer version.
    """
    def check_dev_version(self):
        """"
        Check developer version.
        """
        dev = IgniteVersion('dev')

        assert DEV_BRANCH == dev
        assert DEV_BRANCH.version == dev.version

        index = __version__.find('-')

        if index > 0:
            ver = IgniteVersion(__version__[:index])

            assert dev > ver
            assert dev.version > ver.version

        assert dev.is_dev

        assert str(dev) == 'dev'

        assert dev > LATEST
        assert dev.version > LATEST.version
