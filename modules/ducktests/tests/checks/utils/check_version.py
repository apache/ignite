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
Checks Version.
"""

from ignitetest import __version__
from ignitetest.utils.version import IgniteVersion, DEV_BRANCH, LATEST


def check_dev_version():
    """"
    Check developer version.
    """
    dev = IgniteVersion('dev')
    ignite_dev = IgniteVersion('ignite-dev')
    fork_dev = IgniteVersion('fork-dev')

    assert DEV_BRANCH.is_dev
    assert dev.is_dev
    assert ignite_dev.is_dev
    assert fork_dev.is_dev

    assert DEV_BRANCH == dev == ignite_dev

    # todo solve comparability issues and uncomment the following
    # with pytest.raises(Exception):
    #     assert DEV_BRANCH != fork_dev  # incomparable

    assert DEV_BRANCH.version == dev.version == ignite_dev.version == fork_dev.version
    assert DEV_BRANCH.project == dev.project == ignite_dev.project == "ignite"
    assert fork_dev.project == "fork"

    index = __version__.find('-')

    if index > 0:
        ver = IgniteVersion(__version__[:index])

        assert dev > ver
        assert ignite_dev > ver
        assert dev.version > ver.version
        assert ignite_dev.version > ver.version

    assert dev > LATEST
    assert dev.version > LATEST.version
    assert ignite_dev > LATEST
    assert ignite_dev.version > LATEST.version
    assert DEV_BRANCH > LATEST
    assert DEV_BRANCH.version > LATEST.version

    # todo solve comparability issues and uncomment the following
    # with pytest.raises(Exception):
    #     assert fork_dev != LATEST  # incomparable

    assert fork_dev.version != LATEST.version

    assert str(dev) == str(ignite_dev) == str(DEV_BRANCH) == 'ignite-dev'
    assert str(fork_dev) == 'fork-dev'


def check_numeric_version():
    """
    Checks numeric version.
    """
    v_2_99_1 = IgniteVersion('2.99.1')
    ignite_v_2_99_1 = IgniteVersion('ignite-2.99.1')
    fork_v_2_99_1 = IgniteVersion('fork-2.99.1')

    assert not v_2_99_1.is_dev
    assert not ignite_v_2_99_1.is_dev
    assert not fork_v_2_99_1.is_dev
    assert not LATEST.is_dev

    assert v_2_99_1 == ignite_v_2_99_1

    # todo solve comparability issues and uncomment the following
    # with pytest.raises(Exception):
    #     assert v_2_99_1 != fork_v_2_99_1  # incomparable

    assert v_2_99_1.version == ignite_v_2_99_1.version == fork_v_2_99_1.version == [2, 99, 1]
    assert v_2_99_1.project == ignite_v_2_99_1.project == LATEST.project == "ignite"
    assert fork_v_2_99_1.project == "fork"

    assert str(v_2_99_1) == str(ignite_v_2_99_1) == 'ignite-2.99.1'
    assert str(fork_v_2_99_1) == 'fork-2.99.1'
