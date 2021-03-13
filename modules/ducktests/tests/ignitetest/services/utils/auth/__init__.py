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

DEFAULT_CREDENTIALS_KEY = "default_credentials"
AUTHENTICATION_KEY = "authentication"
ENABLED_KEY = "enabled"


def get_credentials(_globals: dict):
    """
    Gets Credentials from Globals
    Structure may be found in modules/ducktests/tests/checks/utils/check_get_credentials.py
    If authentication is enabled in globals this function return default username and password
    If authentication is not enabled this function return None, None
    Default may be overriden throw globals
    """
    return _globals[AUTHENTICATION_KEY][DEFAULT_CREDENTIALS_KEY] if DEFAULT_CREDENTIALS_KEY in _globals[
        AUTHENTICATION_KEY] else (DEFAULT_AUTH_USERNAME, DEFAULT_AUTH_PASSWORD)


def is_auth_enabled(_globals: dict):
    """
    Return True if Authentication enabled throw globals
    :param _globals:
    :return: bool
    """
    return AUTHENTICATION_KEY in _globals and _globals[AUTHENTICATION_KEY][ENABLED_KEY]
