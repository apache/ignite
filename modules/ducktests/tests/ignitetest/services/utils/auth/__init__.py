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
This module contains authentication classes and utilities.
"""

DEFAULT_AUTH_PASSWORD = 'ignite'
DEFAULT_AUTH_USERNAME = 'ignite'
AUTHENTICATION_ENABLED_KEY = 'use_auth'
CREDENTIALS_KEY = 'credentials'
IGNITE_SERVER_ALIAS = 'server'
IGNITE_CLIENT_ALIAS = 'client'
IGNITE_ADMIN_ALIAS = 'admin'

default_credentials = {
    IGNITE_SERVER_ALIAS: (DEFAULT_AUTH_USERNAME, DEFAULT_AUTH_PASSWORD),
    IGNITE_CLIENT_ALIAS: (DEFAULT_AUTH_USERNAME, DEFAULT_AUTH_PASSWORD),
    IGNITE_ADMIN_ALIAS: (DEFAULT_AUTH_USERNAME, DEFAULT_AUTH_PASSWORD)
}


def get_credentials(_globals: dict, service_name: str):
    """
    Gets Credentials from Globals
    Structure may be found in modules/ducktests/tests/checks/utils/check_get_credentials.py

    There are three possible interactions with a cluster in a ducktape, each of them has its own alias,
    which corresponds to its keystore:
    Ignite(clientMode = False) - server
    Ignite(clientMode = True) - client
    ControlUtility - admin

    If we set "use_auth=True" in globals, these credentials will be injected in corresponding  configuration
    You can also override credentials corresponding to alias throw globals

    We use same credentials for all services by default
    """
    username, password = None, None
    if _globals.get(AUTHENTICATION_ENABLED_KEY):
        if service_name in _globals and CREDENTIALS_KEY in _globals[service_name]:
            username, password = _globals[service_name][CREDENTIALS_KEY]
        elif service_name in default_credentials:
            username, password = default_credentials[service_name]
        else:
            raise Exception("Unknown service name to get Credentials: " + service_name)
    return username, password
