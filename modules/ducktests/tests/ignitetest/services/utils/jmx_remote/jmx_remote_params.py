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

from typing import NamedTuple

ENABLED = "enabled"

JMX_REMOTE_KEY_NAME = "jmx_remote"
JMX_REMOTE_PORT_KEY_NAME = "port"

JMX_REMOTE_DEFAULT_PORT = 1098


class JmxRemoteParams(NamedTuple):
    """
    Params for JMX Remote.

    If enabled the Ignite node exposes JMX endpoint to non-local hosts via the provided port.
    Port is optional. If omitted the JMX_REMOTE_DEFAULT_PORT is used.
    """
    enabled: bool
    port: int = JMX_REMOTE_DEFAULT_PORT


def get_jmx_remote_params(_globals: dict):
    """
    Gets JMX Remote params from Globals. Format is like below (port field is optional):
    {
      "jmx_remote": {
        "enabled": true
        "port": 1098
      }
    }
    :param _globals: Globals parameters
    :return: instance of JmxRemoteParams
    """
    if JMX_REMOTE_KEY_NAME in _globals and _globals[JMX_REMOTE_KEY_NAME].get(ENABLED, False):
        return JmxRemoteParams(enabled=True,
                               port=_globals[JMX_REMOTE_KEY_NAME].get(JMX_REMOTE_PORT_KEY_NAME,
                                                                      JMX_REMOTE_DEFAULT_PORT))
    else:
        return JmxRemoteParams(enabled=False)
