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
    Check that control_utulity.py correctly parse credentials from globals
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
    creds_full = Creds('admin1.jks', 'qwe123', 'truststore.jks', 'qwe123', 'admin1', 'qwe123')

    globals_auth = {
        'use_ssl': True,
        'use_auth': True,
        'admin': test_admin_data_auth
    }
    creds_auth = Creds('admin.jks', '123456', 'truststore.jks', '123456', 'admin1', 'qwe123')

    globals_ssl = {
        'use_ssl': True,
        'use_auth': True,
        'admin': test_admin_data_ssl
    }
    creds_ssl = Creds('admin1.jks', 'qwe123', 'truststore.jks', 'qwe123', 'ignite', 'ignite')

    globals_no_ssl = {
        'use_auth': True,
        'admin': test_admin_data_full
    }
    creds_no_ssl = Creds(login='admin1', password='qwe123')

    globals_no_auth = {
        'use_ssl': True,
        'admin': test_admin_data_full
    }
    creds_no_auth = Creds('admin1.jks', 'qwe123', 'truststore.jks', 'qwe123')

    globals_no_ssl_no_auth = {
        'admin': test_admin_data_full
    }
    creds_no_ssl_no_auth = Creds()

    globals_ssl_auth = {
        'use_ssl': True,
        'use_auth': True
    }
    creds_ssl_auth = Creds('admin.jks', '123456', 'truststore.jks', '123456', 'ignite', 'ignite')

    globals_nil = {}
    creds_nil = Creds()

    @staticmethod
    @pytest.mark.parametrize("test_globals, test_kwargs, expected",
                             [(globals_full, {}, creds_full),
                              (globals_nil, {}, creds_nil),
                              (globals_auth, {}, creds_auth),
                              (globals_ssl, {}, creds_ssl),
                              (globals_no_ssl, {}, creds_no_ssl),
                              (globals_no_auth, {}, creds_no_auth),
                              (globals_no_ssl_no_auth, {}, Creds()),
                              (globals_ssl_auth, {}, creds_ssl_auth),
                              (globals_no_ssl_no_auth, {'key_store_jks': 'admin5.jks', 'key_store_password': 'qwe123'},
                               Creds('admin5.jks', 'qwe123', 'truststore.jks', '123456')),
                              (globals_no_ssl_no_auth, {'login': 'admin5', 'password': 'qwe123'},
                               Creds(login='admin5', password='qwe123')),
                              (globals_no_ssl_no_auth,
                               {'login': 'admin5', 'password': 'qwe123', 'key_store_jks': 'admin5',
                                'key_store_password': 'qwe123'},
                               Creds('admin5', 'qwe123', 'truststore.jks', '123456', 'admin5', 'qwe123')),
                              (globals_full, {'login': 'admin5', 'password': 'qwe123', 'key_store_jks': 'admin5',
                                              'key_store_password': 'qwe123'},
                               Creds('admin1.jks', 'qwe123', 'truststore.jks', 'qwe123', 'admin1', 'qwe123')),
                              (globals_no_ssl, {'login': 'admin5', 'password': 'qwe123', 'key_store_jks': 'admin5',
                                                'key_store_password': 'qwe123'},
                               Creds('admin5', 'qwe123', 'truststore.jks', '123456', 'admin1', 'qwe123'))])
    def check_parse(test_globals, test_kwargs, expected):
        """
        Check that control_utulity.py correctly parse globals
        """
        assert ControlUtility(Cluster(test_globals), **test_kwargs).creds.__eq__(expected)
