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

from ignitetest.services.utils.auth import IGNITE_SERVER_ALIAS, IGNITE_CLIENT_ALIAS, IGNITE_ADMIN_ALIAS

DEFAULT_SERVER_KEYSTORE = 'server.jks'
DEFAULT_CLIENT_KEYSTORE = 'client.jks'
DEFAULT_ADMIN_KEYSTORE = 'admin.jks'
DEFAULT_PASSWORD = "123456"
DEFAULT_TRUSTSTORE = "truststore.jks"
DEFAULT_ROOT = "/opt/"

SSL_ENABLED_KEY = 'use_ssl'
SSL_PARAMS_KEY = 'ssl'

default_keystore = {
    IGNITE_SERVER_ALIAS: DEFAULT_SERVER_KEYSTORE,
    IGNITE_CLIENT_ALIAS: DEFAULT_CLIENT_KEYSTORE,
    IGNITE_ADMIN_ALIAS: DEFAULT_ADMIN_KEYSTORE
}


class SslParams:
    """
    Params for Ignite SslContextFactory.
    """

    # pylint: disable=R0913
    def __init__(self, key_store_jks: str = None, key_store_password: str = DEFAULT_PASSWORD,
                 trust_store_jks: str = DEFAULT_TRUSTSTORE, trust_store_password: str = DEFAULT_PASSWORD,
                 key_store_path: str = None, trust_store_path: str = None, root_dir: str = DEFAULT_ROOT):

        if key_store_jks is None and key_store_path is None:
            raise Exception("Keystore must be specified to init SslParams")

        certificate_dir = os.path.join(root_dir, "ignite-dev", "modules", "ducktests", "tests", "certs")

        self.key_store_path = key_store_path if key_store_path is not None \
            else os.path.join(certificate_dir, key_store_jks)
        self.key_store_password = key_store_password
        self.trust_store_path = trust_store_path if trust_store_path is not None \
            else os.path.join(certificate_dir, trust_store_jks)
        self.trust_store_password = trust_store_password


def get_ssl_params(_globals: dict, service_alias: str):
    """
    Gets SSL params from Globals
    Structure may be found in modules/ducktests/tests/checks/utils/check_get_ssl_params.py

    There are three services in ducktests, each of them has its own alias, which corresponds to its own keystore
    IgniteService - server
    IgniteApplicationService - client
    ControlUtility - admin
    If we set "use_ssl=True" in globals, this SSL params will be injected in corresponding service configuration
    You can also override keystore corresponding to alias throw globals

    Default keystores for this services are generated automaticaly on creating envoriment
    If you specyfy ssl_params in test, you override globals
    """

    root_dir = _globals.get("install_root", DEFAULT_ROOT)
    ssl_param = None
    if _globals.get(SSL_ENABLED_KEY):
        if service_alias in _globals and SSL_PARAMS_KEY in _globals[service_alias]:
            ssl_param = _globals[service_alias][SSL_PARAMS_KEY]
        elif service_alias in default_keystore:
            ssl_param = {'key_store_jks': default_keystore[service_alias]}
        else:
            raise Exception("Unknown service name to get SSL params: " + service_alias)

    return SslParams(root_dir=root_dir, **ssl_param) if ssl_param else None
