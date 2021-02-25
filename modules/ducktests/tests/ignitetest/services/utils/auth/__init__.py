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

DEFAULT_AUTH_PASSWORD = "ignite"
DEFAULT_AUTH_USERNAME = "ignite"


def get_credentials_from_globals(_globals: dict, user: str):
    """
    Parse globals with structure like that.
    {
        "use_auth": True,
        "admin": {
            "credentials": ["username", "qwerty_123"]
        }
    }
    """
    username, password = None, None
    if _globals.get('use_auth'):
        if user in _globals and 'credentials' in _globals[user]:
            username, password = _globals[user]['credentials']
        else:
            username, password = DEFAULT_AUTH_USERNAME, DEFAULT_AUTH_PASSWORD
    return username, password
