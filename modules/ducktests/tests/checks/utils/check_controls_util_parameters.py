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
from ignitetest.services.utils.control_utility import ControlUtility


class Cluster:
    """
    Need this to initialise ControlUtility
    """
    def __init__(self, globals_dict):
        self.context = Context(globals_dict)
        self.certificate_dir = ''


class Context:
    """
    Need this to initialise ControlUtility
    """
    def __init__(self, globals_dict):
        self.logger = ''
        self.globals = globals_dict


class CheckControlUtility:
    """
    Check that control_utulity.py correctly parse globals
    """

    test_admin_data_full = {"login": "admin1", "password": "qwe123",
                            "ssl": {"key_store_jks": "admin1.jks", "key_store_password": "qwe123",
                                    "trust_store_jks": "truststore.jks", "trust_store_password": "qwe123"}}

    test_admin_data_ssl = {
        "ssl": {"key_store_jks": "admin1.jks", "key_store_password": "qwe123", "trust_store_jks": "truststore.jks",
                "trust_store_password": "qwe123"}}
    test_admin_data_auth = {"login": "admin1", "password": "qwe123"}

    globals_full = {
        'use_ssl': True,
        'use_auth': True,
        'admin': test_admin_data_full
    }

    globals_auth = {
        'use_ssl': True,
        'use_auth': True,
        'admin': test_admin_data_auth
    }

    globals_ssl = {
        'use_ssl': True,
        'use_auth': True,
        'admin': test_admin_data_ssl
    }

    globals_no_ssl = {
        'use_auth': True,
        'admin': test_admin_data_full
    }

    globals_no_auth = {
        'use_ssl': True,
        'admin': test_admin_data_full
    }

    globals_no_ssl_no_auth = {
        'admin': test_admin_data_full
    }

    globals_ssl_auth = {
        'use_ssl': True,
        'use_auth': True
    }

    globals_nil = {}

    @pytest.mark.parametrize("test_globals, test_kwargs, expected",
                             [(globals_full, {}, {'key_store_path': 'admin1.jks', 'key_store_password': 'qwe123',
                                                  'trust_store_path': 'truststore.jks',
                                                  'trust_store_password': 'qwe123',
                                                  'login': 'admin1', 'password': 'qwe123'}),
                              (globals_no_auth, {}, {'key_store_path': 'admin1.jks', 'key_store_password': 'qwe123',
                                                     'trust_store_path': 'truststore.jks',
                                                     'trust_store_password': 'qwe123'}),
                              (globals_auth, {}, {'key_store_path': 'admin.jks', 'key_store_password': '123456',
                                                  'trust_store_path': 'truststore.jks',
                                                  'trust_store_password': '123456',
                                                  'login': 'admin1', 'password': 'qwe123'}),
                              (globals_ssl, {}, {'key_store_path': 'admin1.jks', 'key_store_password': 'qwe123',
                                                 'trust_store_path': 'truststore.jks', 'trust_store_password': 'qwe123',
                                                 'login': 'ignite', 'password': 'ignite'}),
                              (globals_no_ssl, {}, {'login': 'admin1', 'password': 'qwe123'}),
                              (globals_no_auth, {}, {'key_store_path': 'admin1.jks', 'key_store_password': 'qwe123',
                                                     'trust_store_path': 'truststore.jks',
                                                     'trust_store_password': 'qwe123'}),
                              (globals_no_ssl_no_auth, {}, {}),
                              (globals_ssl_auth, {}, {'key_store_path': 'admin.jks', 'key_store_password': '123456',
                                                      'trust_store_path': 'truststore.jks',
                                                      'trust_store_password': '123456', 'login': 'ignite',
                                                      'password': 'ignite'}),
                              (globals_no_ssl_no_auth, {'key_store_jks': 'admin5', 'key_store_password': 'qwe123'},
                               {'key_store_path': 'admin5', 'key_store_password': 'qwe123',
                                'trust_store_path': 'truststore.jks', 'trust_store_password': '123456'}),
                              (globals_no_ssl_no_auth, {'login': 'admin5', 'password': 'qwe123'},
                               {'login': 'admin5', 'password': 'qwe123'}),
                              (globals_no_ssl_no_auth,
                               {'login': 'admin5', 'password': 'qwe123', 'key_store_jks': 'admin5',
                                'key_store_password': 'qwe123'},
                               {'key_store_path': 'admin5', 'key_store_password': 'qwe123',
                                'trust_store_path': 'truststore.jks', 'trust_store_password': '123456',
                                'login': 'admin5',
                                'password': 'qwe123'}),
                              (globals_full, {'login': 'admin5', 'password': 'qwe123', 'key_store_jks': 'admin5',
                                              'key_store_password': 'qwe123'},
                               {'key_store_path': 'admin1.jks', 'key_store_password': 'qwe123',
                                'trust_store_path': 'truststore.jks', 'trust_store_password': 'qwe123',
                                'login': 'admin1',
                                'password': 'qwe123'}),
                              (globals_no_ssl, {'login': 'admin5', 'password': 'qwe123', 'key_store_jks': 'admin5',
                                                'key_store_password': 'qwe123'},
                               {'key_store_path': 'admin5', 'key_store_password': 'qwe123',
                                'trust_store_path': 'truststore.jks', 'trust_store_password': '123456',
                                'login': 'admin1',
                                'password': 'qwe123'})])
    def check_parse(self, test_globals, test_kwargs, expected):
        """
        Check that control_utulity.py correctly parse globals
        """
        assert ControlUtility(Cluster(test_globals), **test_kwargs).creds == expected
