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
# limitations under the License

"""
This module contains classes and utilities for Ignite SslContextFactory.
"""
import os

IGNITE_SERVER_ALIAS = 'server'
IGNITE_CLIENT_ALIAS = 'client'
IGNITE_ADMIN_ALIAS = 'admin'

DEFAULT_SERVER_KEYSTORE = 'server.jks'
DEFAULT_CLIENT_KEYSTORE = 'client.jks'
DEFAULT_ADMIN_KEYSTORE = 'admin.jks'
DEFAULT_PASSWORD = "123456"
DEFAULT_TRUSTSTORE = "truststore.jks"

SSL_PARAMS_KEY = "params"
SSL_KEY = "ssl"
ENABLED_KEY = "enabled"

default_keystore = {
    IGNITE_SERVER_ALIAS: DEFAULT_SERVER_KEYSTORE,
    IGNITE_CLIENT_ALIAS: DEFAULT_CLIENT_KEYSTORE,
    IGNITE_ADMIN_ALIAS: DEFAULT_ADMIN_KEYSTORE
}


class SslParams:
    """
    Params for Ignite SslContextFactory.
    """

    def __init__(self, root_dir: str, key_store_jks: str = None, key_store_password: str = DEFAULT_PASSWORD,
                 trust_store_jks: str = DEFAULT_TRUSTSTORE, trust_store_password: str = DEFAULT_PASSWORD,
                 key_store_path: str = None, trust_store_path: str = None, cipher_suites: str = None,
                 trust_managers: str = None):
        if not key_store_jks and not key_store_path:
            raise Exception("Keystore must be specified to init SslParams")

        self.key_store_path = key_store_path if key_store_path else os.path.join(root_dir, key_store_jks)
        self.key_store_password = key_store_password
        self.trust_store_path = trust_store_path if trust_store_path else os.path.join(root_dir, trust_store_jks)
        self.trust_store_password = trust_store_password
        self.cipher_suites = cipher_suites
        self.trust_managers = trust_managers


def get_ssl_params(_globals: dict, shared_root: str, alias: str):
    """
    Gets SSL params from Globals
    Structure may be found in modules/ducktests/tests/checks/utils/check_get_ssl_params.py

    There are three possible interactions with a cluster in a ducktape, each of them has its own alias,
    which corresponds to keystore:
    Ignite(clientMode = False) - server
    Ignite(clientMode = True) - client
    ControlUtility - admin

    If we enable SSL in globals, these SSL params will be injected in corresponding
    configuration
    You can also override keystore corresponding to alias throw globals

    Default keystores for these services are generated automaticaly on creating envoriment
    If you specyfy ssl_params in test, you override globals
    """

    if SSL_PARAMS_KEY in _globals[SSL_KEY] and alias in _globals[SSL_KEY][SSL_PARAMS_KEY]:
        ssl_param = _globals[SSL_KEY][SSL_PARAMS_KEY][alias]
    elif alias in default_keystore:
        ssl_param = {'key_store_jks': default_keystore[alias]}
    else:
        raise Exception("We don't have SSL params for: " + alias)

    return SslParams(shared_root, **ssl_param) if ssl_param else None


def is_ssl_enabled(_globals: dict):
    """
    Return True if SSL enabled throw globals
    :param _globals:
    :return: bool
    """
    return SSL_KEY in _globals and _globals[SSL_KEY][ENABLED_KEY]
