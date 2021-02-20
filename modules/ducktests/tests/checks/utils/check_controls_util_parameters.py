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
Checks Control Utility params parsing
"""

import pytest
from ignitetest.services.utils.auth import DEFAULT_AUTH_USERNAME, DEFAULT_AUTH_PASSWORD
from ignitetest.services.utils.ssl.ssl_context import SslContext
from ignitetest.services.utils.control_utility import ControlUtility

DEFAULT_ADMIN_KEYSTORE = 'admin.jks'
INSTALL_ROOT = '/opt'
CERTIFICATE_DIR = '/opt/ignite-dev/modules/ducktests/tests/certs/'


class Cluster:
    """
    Need this to initialise ControlUtility
    """

    def __init__(self, globals_dict):
        self.context = Context(globals_dict)
        self.instal_root = CERTIFICATE_DIR


class Context:
    """
    Need this to initialise ControlUtility
    """

    def __init__(self, globals_dict):
        self.logger = ''
        self.globals = globals_dict
        self.globals['install_root'] = INSTALL_ROOT


def compare_ssl(class1, class2):
    """
    Compare two SslContext objects
    """

    if class1 is None and class2 is None:
        return True
    if isinstance(class1, SslContext) and isinstance(class2, SslContext):
        return class1.__dict__ == class2.__dict__
    return False


class TestParams:
    """
    Globals and Kwargs for tests
    """

    test_ssl_jks = {'key_store_jks': 'admin1.jks', 'key_store_password': 'qwe123',
                    'trust_store_jks': 'truststore.jks', 'trust_store_password': 'qwe123'}
    test_ssl_path = {'key_store_path': '/opt/certs/admin1.jks', 'key_store_password': 'qwe123',
                     'trust_store_path': '/opt/certs/truststore.jks',
                     'trust_store_password': 'qwe123'}

    test_ssl_context_jks = SslContext(root_dir=INSTALL_ROOT,
                                      key_store_jks='admin1.jks', key_store_password='qwe123',
                                      trust_store_jks='truststore.jks', trust_store_password='qwe123')
    test_ssl_context_path = SslContext(key_store_path='/opt/certs/admin1.jks', key_store_password='qwe123',
                                       trust_store_path='/opt/certs/truststore.jks', trust_store_password='qwe123')
    test_ssl_context = SslContext(root_dir=INSTALL_ROOT, key_store_jks='admin1.jks')
    test_ssl_context_default = SslContext(root_dir=INSTALL_ROOT,
                                          key_store_jks='admin.jks', key_store_password='123456',
                                          trust_store_jks='truststore.jks', trust_store_password='123456')

    test_username = 'admin1'
    test_password = 'qwe123'
    test_username2 = 'admin1'


class CheckCaseGlobalsSsl:
    """
    Check that control_utulity.py correctly parse SSL params from globals
    """

    @staticmethod
    @pytest.mark.parametrize('test_globals, expected',
                             [({'use_ssl': True,
                                'admin': {'ssl': TestParams.test_ssl_jks}}, TestParams.test_ssl_context_jks),
                              ({'use_ssl': True,
                                'admin': {'ssl': TestParams.test_ssl_path}}, TestParams.test_ssl_context_path),
                              ({'use_ssl': True}, TestParams.test_ssl_context_default)])
    def check_parse(test_globals, expected):
        """
        Check that control_utulity.py correctly parse SSL params from globals
        """

        assert compare_ssl(ControlUtility(Cluster(test_globals)).ssl_context, expected)


class CheckCaseParamSsl:
    """
    Check that control_utulity.py correctly parse SSL params from parameter
    """

    @staticmethod
    @pytest.mark.parametrize('test_ssl_context, expected',
                             [(TestParams.test_ssl_context, TestParams.test_ssl_context)])
    def check_parse(test_ssl_context, expected):
        """
        Check that control_utulity.py correctly parse SSL params from parameter
        """

        assert compare_ssl(ControlUtility(Cluster({}), ssl_context=test_ssl_context).ssl_context, expected)


class CheckCaseParamAndGlobalsSsl:
    """
    Check that control_utulity.py correctly parse SSL
    """

    @staticmethod
    @pytest.mark.parametrize('test_globals, test_ssl_context, expected',
                             [({'use_ssl': True,
                                'admin': {'ssl': TestParams.test_ssl_jks}}, TestParams.test_ssl_context_default,
                               TestParams.test_ssl_context_jks)])
    def check_parse(test_globals, test_ssl_context, expected):
        """
        Check that control_utulity.py correctly parse SSL
        """

        assert compare_ssl(ControlUtility(cluster=Cluster(test_globals), ssl_context=test_ssl_context).ssl_context,
                           expected)


class CheckCaseGlobalsCredentials:
    """
    Check that control_utulity.py correctly parse Credentials from globals
    """

    @staticmethod
    @pytest.mark.parametrize('test_globals, expected_username, expected_password',
                             [({'use_auth': True,
                                'admin': {'credentials': [TestParams.test_username, TestParams.test_password]}},
                               TestParams.test_username, TestParams.test_password),
                              ({'use_auth': True}, DEFAULT_AUTH_USERNAME, DEFAULT_AUTH_PASSWORD)])
    def check_parse(test_globals, expected_username, expected_password):
        """
        Check that control_utulity.py correctly parse Credentials from globals
        """

        control_utility = ControlUtility(Cluster(test_globals))
        assert control_utility.username == expected_username
        assert control_utility.password == expected_password


class CheckCaseParamCredentials:
    """
    Check that control_utulity.py correctly parse SSL params from parameter
    """

    @staticmethod
    @pytest.mark.parametrize('test_username, test_password, expected_username, expected_password',
                             [(TestParams.test_username, TestParams.test_password,
                               TestParams.test_username, TestParams.test_password)])
    def check_parse(test_username, test_password, expected_username, expected_password):
        """
        Check that control_utulity.py correctly parse Credentials from parameter
        """

        control_utility = ControlUtility(cluster=Cluster({}), username=test_username, password=test_password)
        assert control_utility.username == expected_username
        assert control_utility.password == expected_password


class CheckCaseParamAndGlobalsCredentials:
    """
    Check that control_utulity.py correctly parse Credentials
    """

    @staticmethod
    @pytest.mark.parametrize('test_globals, test_username, test_password, expected_username, expected_password',
                             [({'use_ssl': True,
                                'admin': {'credentials': [TestParams.test_username2, TestParams.test_password]}},
                               TestParams.test_username, TestParams.test_password,
                               TestParams.test_username2, TestParams.test_password)])
    def check_parse(test_globals, test_username, test_password, expected_username, expected_password):
        """
        Check that control_utulity.py correctly parse Credentials
        """

        control_utility = ControlUtility(cluster=Cluster(test_globals), username=test_username, password=test_password)
        assert control_utility.username == expected_username
        assert control_utility.password == expected_password
