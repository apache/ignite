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
Check that get_credentials correctly parse Credentials from globals
"""

import pytest
from ignitetest.services.utils.auth import get_credentials, DEFAULT_AUTH_USERNAME, DEFAULT_AUTH_PASSWORD, \
    USERNAME_KEY, PASSWORD_KEY, AUTHENTICATION_KEY, ENABLED_KEY

TEST_USERNAME = "admin"
TEST_PASSWORD = "qwe123"


class CheckCaseJks:
    """
    Check that get_credentials correctly parse Credentials from globals
    Posible structure is:
    {"authentication": {
        "enabled": true,
        "username": "admin",
        "password": "qwe123"}}
    """

    @staticmethod
    @pytest.mark.parametrize('test_globals, expected_username, expected_password',
                             [({AUTHENTICATION_KEY: {
                                 ENABLED_KEY: True,
                                 USERNAME_KEY: TEST_USERNAME,
                                 PASSWORD_KEY: TEST_PASSWORD}}, TEST_USERNAME,
                               TEST_PASSWORD),
                                 ({AUTHENTICATION_KEY: {
                                     ENABLED_KEY: True}}, DEFAULT_AUTH_USERNAME, DEFAULT_AUTH_PASSWORD)])
    def check_parse(test_globals, expected_username, expected_password):
        """
        Check function for pytest
        """
        assert (expected_username, expected_password) == get_credentials(test_globals)
