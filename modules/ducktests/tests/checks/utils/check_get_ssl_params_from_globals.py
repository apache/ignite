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
Check that SslParams correctly parse SSL params from globals
"""

import pytest
from ignitetest.services.utils.ssl.ssl_params import get_ssl_params_from_globals, SslParams, DEFAULT_TRUSTSTORE, \
    DEFAULT_CLIENT_KEYSTORE, DEFAULT_PASSWORD

INSTALL_ROOT = '/opt/'
CERTIFICATE_DIR = '/opt/ignite-dev/modules/ducktests/tests/certs/'
TEST_KEYSTORE_JKS = "client1.jks"
TEST_TRUSTSTORE_JKS = "truststore.jks"
TEST_PASSWORD = "qwe123"
TEST_CERTIFICATE_DIR = "/opt/certs/"
TEST_USER = "client"


def _compare(expected, actual):
    """
    Compare SslParams object with dictionary
    """

    if expected is None and actual is None:
        return True
    if isinstance(expected, dict) and isinstance(actual, SslParams):
        return expected == actual.__dict__
    return False


class TestParams:
    """
    Globals for tests
    """

    test_globals_jks = {
        "install_root": INSTALL_ROOT,
        "use_ssl": "True",
        TEST_USER: {
            "ssl": {
                "key_store_jks": TEST_KEYSTORE_JKS,
                "key_store_password": TEST_PASSWORD,
                "trust_store_jks": TEST_TRUSTSTORE_JKS,
                "trust_store_password": TEST_PASSWORD}}}
    test_globals_path = {
        "install_root": INSTALL_ROOT,
        "use_ssl": "True",
        TEST_USER: {
            "ssl": {
                "key_store_path": TEST_CERTIFICATE_DIR + TEST_KEYSTORE_JKS,
                "key_store_password": TEST_PASSWORD,
                "trust_store_path": TEST_CERTIFICATE_DIR + TEST_TRUSTSTORE_JKS,
                "trust_store_password": TEST_PASSWORD}}}
    test_globals_default = {
        "install_root": INSTALL_ROOT,
        "use_ssl": "True"}
    test_globals_no_ssl = {
        "install_root": INSTALL_ROOT}

    expected_ssl_params_jks = {'key_store_path': CERTIFICATE_DIR + TEST_KEYSTORE_JKS,
                               'key_store_password': TEST_PASSWORD,
                               'trust_store_path': CERTIFICATE_DIR + TEST_TRUSTSTORE_JKS,
                               'trust_store_password': TEST_PASSWORD}
    expected_ssl_params_path = {'key_store_path': TEST_CERTIFICATE_DIR + TEST_KEYSTORE_JKS,
                                'key_store_password': TEST_PASSWORD,
                                'trust_store_path': TEST_CERTIFICATE_DIR + TEST_TRUSTSTORE_JKS,
                                'trust_store_password': TEST_PASSWORD}
    expected_ssl_params_default = {'key_store_path': CERTIFICATE_DIR + DEFAULT_CLIENT_KEYSTORE,
                                   'key_store_password': DEFAULT_PASSWORD,
                                   'trust_store_path': CERTIFICATE_DIR + DEFAULT_TRUSTSTORE,
                                   'trust_store_password': DEFAULT_PASSWORD}


class CheckCaseJks:
    """
    Check that SslParams correctly parse SSL params from globals
    """

    @staticmethod
    @pytest.mark.parametrize('test_globals, expected',
                             [(TestParams.test_globals_jks, TestParams.expected_ssl_params_jks),
                              (TestParams.test_globals_path, TestParams.expected_ssl_params_path),
                              (TestParams.test_globals_default, TestParams.expected_ssl_params_default),
                              (TestParams.test_globals_no_ssl, None)])
    def check_parse(test_globals, expected):
        """
        Check that SslParams correctly parse SSL params from globals
        """

        assert _compare(expected, get_ssl_params_from_globals(test_globals, TEST_USER))
