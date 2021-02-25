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


class TestParams:
    """
    Constant variables for test
    """

    test_globals = {
        "use_auth": "True",
        "client": {
            "credentials": ["client", "qwe123"]}}
    test_globals_default = {
        "use_auth": "True"}
    test_globals_no_auth = {}

    expected_username = 'client'
    expected_password = 'qwe123'
    expected_username_default = DEFAULT_AUTH_USERNAME
    expected_password_default = DEFAULT_AUTH_PASSWORD


class CheckCaseJks:
    """
    Check that get_credentials_from_globals correctly parse Credentials from globals
    """

    @staticmethod
    @pytest.mark.parametrize('test_globals, expected_username, expected_password',
                             [(TestParams.test_globals, TestParams.expected_username, TestParams.expected_password),
                              (TestParams.test_globals_default, TestParams.expected_username_default,
                               TestParams.expected_password_default),
                              (TestParams.test_globals_no_auth, None, None)])
    def check_parse(test_globals, expected_username, expected_password):
        """
        Check function for pytest
        """

        assert (expected_username, expected_password) == get_credentials_from_globals(test_globals, 'client')
