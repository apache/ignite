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
Checks Control Utility params pasrsing
"""

import pytest
from ignitetest.services.utils.control_utility import ControlUtility, Creds


class Cluster:
    """
    Need this to initialise ControlUtility
    """

    def __init__(self, globals_dict):
        self.context = Context(globals_dict)
        self.certificate_dir = '/opt/certs/'


class Context:
    """
    Need this to initialise ControlUtility
    """

    def __init__(self, globals_dict):
        self.logger = ''
        self.globals = globals_dict


class TestParams:
    """
    Globals and Kwargs for tests
    """

    test_ssl_jks = {'key_store_jks': 'admin1.jks', 'key_store_password': 'qwe123',
                    'trust_store_jks': 'truststore.jks', 'trust_store_password': 'qwe123'}
    test_ssl_path = {'key_store_path': '/opt/ignite/certs/admin1.jks', 'key_store_password': 'qwe123',
                     'trust_store_path': '/opt/ignite/certs/truststore.jks',
                     'trust_store_password': 'qwe123'}
    test_auth = {'login': 'admin1', 'password': 'qwe123'}

    creds_full_jks = Creds('/opt/certs/admin1.jks', 'qwe123',
                           '/opt/certs/truststore.jks', 'qwe123',
                           'admin1', 'qwe123')
    creds_full_path = Creds('/opt/ignite/certs/admin1.jks', 'qwe123',
                            '/opt/ignite/certs/truststore.jks', 'qwe123',
                            'admin1', 'qwe123')
    creds_auth = Creds(login='admin1', password='qwe123')
    creds_jks = Creds('/opt/certs/admin1.jks', 'qwe123',
                      '/opt/certs/truststore.jks', 'qwe123')
    creds_none = Creds()
    creds_full_default = Creds('/opt/certs/admin.jks', '123456',
                               '/opt/certs/truststore.jks', '123456',
                               'ignite', 'ignite')
    creds_ssl_default = Creds('/opt/certs/admin.jks', '123456',
                              '/opt/certs/truststore.jks', '123456')
    creds_auth_default = Creds(login='ignite', password='ignite')

    test_globals_full_jks = dict(test_auth)
    test_globals_full_jks['ssl'] = test_ssl_jks
    test_globals_full_path = dict(test_auth)
    test_globals_full_path['ssl'] = test_ssl_path
    test_globals_auth = dict(test_auth)
    test_globals_ssl = dict()
    test_globals_ssl['ssl'] = test_ssl_jks
    test_globals_none = dict

    test_kwargs_full_jks = dict(test_auth, **test_ssl_jks)
    test_kwargs_full_path = dict(test_auth, **test_ssl_path)
    test_kwargs_auth = dict(test_auth)
    test_kwargs_ssl = test_ssl_jks


class CheckCaseGlobalsSetKwargsNotSet:
    """
    Check that control_utulity.py correctly parse credentials from globals
    """

    test_globals_full_jks = TestParams.test_globals_full_jks
    test_globals_full_path = TestParams.test_globals_full_path
    test_globals_auth = TestParams.test_globals_auth
    test_globals_ssl = TestParams.test_globals_ssl
    test_globals_none = TestParams.test_globals_none

    creds_full_jks = TestParams.creds_full_jks
    creds_full_path = TestParams.creds_full_path
    creds_auth = TestParams.creds_auth
    creds_jks = TestParams.creds_jks
    creds_none = TestParams.creds_none

    @staticmethod
    @pytest.mark.parametrize('test_globals, test_kwargs, expected',
                             [({'use_ssl': True,
                                'use_auth': True,
                                'admin': test_globals_full_jks}, {}, creds_full_jks),
                              ({'use_ssl': True,
                                'use_auth': True,
                                'admin': test_globals_full_path}, {}, creds_full_path),
                              ({'use_auth': True,
                                'admin': test_globals_auth}, {}, creds_auth),
                              ({'use_ssl': True,
                                'admin': test_globals_ssl}, {}, creds_jks),
                              ({'admin': test_globals_none}, {}, creds_none),
                              ({'use_ssl': False,
                                'use_auth': False,
                                'admin': test_globals_full_jks}, {}, creds_none), ])
    def check_parse(test_globals, test_kwargs, expected):
        """
        Check that control_utulity.py correctly parse credentials from globals
        """

        assert ControlUtility(Cluster(test_globals), **test_kwargs).creds.__eq__(expected)


class CheckCaseGlobalsNotSetKwargsSet:
    """
   Check that control_utulity.py correctly parse credentials from kwargs
   """

    test_kwargs_full_jks = TestParams.test_kwargs_full_jks
    test_kwargs_full_path = TestParams.test_kwargs_full_path
    test_kwargs_auth = TestParams.test_kwargs_auth
    test_kwargs_ssl = TestParams.test_ssl_jks

    creds_full_jks = TestParams.creds_full_jks
    creds_full_path = TestParams.creds_full_path
    creds_auth = TestParams.creds_auth
    creds_jks = TestParams.creds_jks
    creds_none = TestParams.creds_none

    @staticmethod
    @pytest.mark.parametrize('test_globals, test_kwargs, expected',
                             [({}, test_kwargs_full_jks, creds_full_jks),
                              ({}, test_kwargs_full_path, creds_full_path),
                              ({}, test_kwargs_auth, creds_auth),
                              ({}, test_kwargs_ssl, creds_jks),
                              ({}, {}, creds_none)])
    def check_parse(test_globals, test_kwargs, expected):
        """
        Check that control_utulity.py correctly parse credentials from kwargs
        """

        assert ControlUtility(Cluster(test_globals), **test_kwargs).creds.__eq__(expected)


class CheckCaseGlobalsSetKwargsSet:
    """
    Check that control_utulity.py correctly parse credentials
    """

    test_kwargs_full_jks = TestParams.test_kwargs_full_jks
    test_kwargs_full_path = TestParams.test_kwargs_full_path

    creds_full_jks = TestParams.creds_full_jks
    creds_full_path = TestParams.creds_full_path

    test_globals_full_jks = TestParams.test_globals_full_jks
    test_globals_full_path = TestParams.test_globals_full_path

    @staticmethod
    @pytest.mark.parametrize('test_globals, test_kwargs, expected',
                             [({'use_ssl': True,
                                'use_auth': True,
                                'admin': test_globals_full_jks}, test_kwargs_full_path, creds_full_jks),
                              ({'use_ssl': True,
                                'use_auth': True,
                                'admin': test_globals_full_path}, test_kwargs_full_jks, creds_full_path)])
    def check_parse(test_globals, test_kwargs, expected):
        """
        Check that control_utulity.py correctly parse credentials
        """

        assert ControlUtility(Cluster(test_globals), **test_kwargs).creds.__eq__(expected)


class CheckCaseDefaults:
    """
    Check that control_utulity.py correctly use default credentials
    """

    test_globals_full_path = TestParams.test_globals_full_path

    test_kwargs_modify_jks = {'key_store_jks': 'admin11.jks'}
    test_kwargs_modify_login = {'login': 'admin11'}
    test_kwargs_modify_jks_login = {'key_store_jks': 'admin11.jks', 'login': 'admin11'}

    creds_modify_jks = Creds('/opt/certs/admin11.jks', '123456',
                             '/opt/certs/truststore.jks', '123456')
    creds_modify_login = Creds(login='admin11', password="ignite")
    creds_modify_jks_login = Creds('/opt/certs/admin11.jks', '123456',
                                   '/opt/certs/truststore.jks', '123456',
                                   'admin11', 'ignite')

    creds_full_default = TestParams.creds_full_default
    creds_ssl_default = TestParams.creds_ssl_default
    creds_auth_default = TestParams.creds_auth_default
    creds_none = TestParams.creds_none

    @staticmethod
    @pytest.mark.parametrize("test_globals, test_kwargs, expected",
                             [({'use_ssl': True,
                                'use_auth': True}, {}, creds_full_default),
                              ({'use_ssl': True}, {}, creds_ssl_default),
                              ({'use_auth': True}, {}, creds_auth_default),
                              ({'admin': test_globals_full_path}, {}, creds_none),
                              ({}, test_kwargs_modify_jks, creds_modify_jks),
                              ({}, test_kwargs_modify_login, creds_modify_login),
                              ({}, test_kwargs_modify_jks_login, creds_modify_jks_login)])
    def check_parse(test_globals, test_kwargs, expected):
        """
        Check that control_utulity.py correctly parse credentials from globals
        """

        assert ControlUtility(Cluster(test_globals), **test_kwargs).creds.__eq__(expected)
