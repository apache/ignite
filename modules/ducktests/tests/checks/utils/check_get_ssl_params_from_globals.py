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
import os

import pytest
from ignitetest.services.utils.ssl.ssl_params import get_ssl_params, SslParams, DEFAULT_TRUSTSTORE, \
    DEFAULT_CLIENT_KEYSTORE, DEFAULT_PASSWORD, IGNITE_CLIENT_ALIAS, SSL_KEY, SSL_PARAMS_KEY, ENABLED_KEY

CERT_DIR1 = "/folder1"
CERT_DIR2 = "/folder2"
TEST_KEYSTORE_JKS = "client1.jks"
TEST_TRUSTSTORE_JKS = "truststore.jks"
TEST_PASSWORD = "qwe123"


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
    Possible structure is:
    {"ssl": {
        "enabled": true,
        "params": {
          "server": {
            "key_store_jks": "server.jks",
            "key_store_password": "123456",
            "trust_store_jks": "truststore.jks",
            "trust_store_password": "123456"
          },
          "client": {
            "key_store_jks": "client.jks",
            "key_store_password": "123456",
            "trust_store_jks": "truststore.jks",
            "trust_store_password": "123456"
          },
          "admin": {
            "key_store_jks": "admin.jks",
            "key_store_password": "123456",
            "trust_store_jks": "truststore.jks",
            "trust_store_password": "123456"
          }
        }
      }
    }
    """

    test_globals_jks = {
        SSL_KEY: {
            ENABLED_KEY: True,
            SSL_PARAMS_KEY: {
                IGNITE_CLIENT_ALIAS: {
                    "key_store_jks": TEST_KEYSTORE_JKS,
                    "key_store_password": TEST_PASSWORD,
                    "trust_store_jks": TEST_TRUSTSTORE_JKS,
                    "trust_store_password": TEST_PASSWORD}}}}
    test_globals_path = {
        SSL_KEY: {
            ENABLED_KEY: True,
            SSL_PARAMS_KEY: {
                IGNITE_CLIENT_ALIAS: {
                    "key_store_path": os.path.join(CERT_DIR2, TEST_KEYSTORE_JKS),
                    "key_store_password": TEST_PASSWORD,
                    "trust_store_path": os.path.join(CERT_DIR2, TEST_TRUSTSTORE_JKS),
                    "trust_store_password": TEST_PASSWORD}}}}
    test_globals_default = {SSL_KEY: {ENABLED_KEY: True}}
    test_globals_no_ssl = {}

    expected_ssl_params_jks = {'key_store_path': os.path.join(CERT_DIR1, TEST_KEYSTORE_JKS),
                               'key_store_password': TEST_PASSWORD,
                               'trust_store_path': os.path.join(CERT_DIR1, TEST_TRUSTSTORE_JKS),
                               'trust_store_password': TEST_PASSWORD,
                               'cipher_suites': None,
                               'trust_managers': None}
    expected_ssl_params_path = {'key_store_path': os.path.join(CERT_DIR2, TEST_KEYSTORE_JKS),
                                'key_store_password': TEST_PASSWORD,
                                'trust_store_path': os.path.join(CERT_DIR2, TEST_TRUSTSTORE_JKS),
                                'trust_store_password': TEST_PASSWORD,
                                'cipher_suites': None,
                                'trust_managers': None}
    expected_ssl_params_default = {'key_store_path': os.path.join(CERT_DIR1, DEFAULT_CLIENT_KEYSTORE),
                                   'key_store_password': DEFAULT_PASSWORD,
                                   'trust_store_path': os.path.join(CERT_DIR1, DEFAULT_TRUSTSTORE),
                                   'trust_store_password': DEFAULT_PASSWORD,
                                   'cipher_suites': None,
                                   'trust_managers': None}


class CheckCaseJks:
    """
    Check that SslParams correctly parse SSL params from globals
    """

    @staticmethod
    @pytest.mark.parametrize('test_globals, shared_root, expected',
                             [(TestParams.test_globals_jks, CERT_DIR1, TestParams.expected_ssl_params_jks),
                              (TestParams.test_globals_path, CERT_DIR2, TestParams.expected_ssl_params_path),
                              (TestParams.test_globals_default, CERT_DIR1, TestParams.expected_ssl_params_default)])
    def check_parse(test_globals, shared_root, expected):
        """
        Check that SslParams correctly parse SSL params from globals
        """
        assert _compare(expected, get_ssl_params(test_globals, shared_root, IGNITE_CLIENT_ALIAS))
