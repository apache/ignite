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

This file contains three user presets:
1. server for Ignite(clientMode=False)
2. client for Ignte(clientMode=True) node and ThinClient
3. admin for GridClient and console utils (control.sh)

Keystores for this presets are generated automaticaly on creating envoriment
If you like to specify different certificate for preset you can pass them throw globals
If you specyfy ssl_params in test, you override globals
"""
import os


DEFAULT_KEYSTORE = 'server.jks'
DEFAULT_CLIENT_KEYSTORE = 'client.jks'
DEFAULT_ADMIN_KEYSTORE = 'admin.jks'
DEFAULT_PASSWORD = "123456"
DEFAULT_TRUSTSTORE = "truststore.jks"
DEFAULT_ROOT = "/opt/"

default_keystore = {
    'server': DEFAULT_KEYSTORE,
    'client': DEFAULT_CLIENT_KEYSTORE,
    'admin': DEFAULT_ADMIN_KEYSTORE
}


class SslParams:
    """
    Params for Ignite SslContextFactory.
    """

    # pylint: disable=R0913
    def __init__(self, root_dir: str = DEFAULT_ROOT,
                 key_store_jks: str = DEFAULT_KEYSTORE, key_store_password: str = DEFAULT_PASSWORD,
                 trust_store_jks: str = DEFAULT_TRUSTSTORE, trust_store_password: str = DEFAULT_PASSWORD,
                 key_store_path: str = None, trust_store_path: str = None):
        certificate_dir = os.path.join(root_dir, "ignite-dev", "modules", "ducktests", "tests", "certs")

        self.key_store_path = key_store_path if key_store_path is not None \
            else os.path.join(certificate_dir, key_store_jks)
        self.key_store_password = key_store_password
        self.trust_store_path = trust_store_path if trust_store_path is not None \
            else os.path.join(certificate_dir, trust_store_jks)
        self.trust_store_password = trust_store_password


def get_ssl_params_from_globals(_globals: dict, user: str):
    """
    Gets SSL params from Globals
    Structure may be found in modules/ducktests/tests/checks/utils/check_get_ssl_params_from_globals.py
    """
    root_dir = _globals.get("install_root", DEFAULT_ROOT)
    ssl_param = None
    if _globals.get('use_ssl'):
        if user in _globals and 'ssl' in _globals[user]:
            ssl_param = _globals[user]['ssl']
        else:
            ssl_param = {'key_store_jks': default_keystore[user]}

    return SslParams(root_dir, **ssl_param) if ssl_param else None
