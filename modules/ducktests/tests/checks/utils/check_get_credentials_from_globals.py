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
Check that get_credentials_from_globals correctly parse Credentials from globals
"""

import pytest
from ignitetest.services.utils.auth import get_credentials_from_globals, DEFAULT_AUTH_USERNAME, DEFAULT_AUTH_PASSWORD

TEST_USER = "client"
TEST_PASSWORD = "qwe123"


class CheckCaseJks:
    """
    Check that get_credentials_from_globals correctly parse Credentials from globals
    """

    @staticmethod
    @pytest.mark.parametrize('test_globals, expected_username, expected_password',
                             [({"use_auth": "True",
                                TEST_USER: {
                                    "credentials": [TEST_USER, TEST_PASSWORD]}}, TEST_USER, TEST_PASSWORD),
                              ({"use_auth": "True"}, DEFAULT_AUTH_USERNAME, DEFAULT_AUTH_PASSWORD),
                              ({}, None, None)])
    def check_parse(test_globals, expected_username, expected_password):
        """
        Check function for pytest
        """

        assert (expected_username, expected_password) == get_credentials_from_globals(test_globals, TEST_USER)
