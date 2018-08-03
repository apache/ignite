# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import ssl

from pyignite.constants import *


def wrap(conn, _socket):
    """ Wrap socket in SSL wrapper. """
    if conn.init_kwargs.get('use_ssl', None):
        _socket = ssl.wrap_socket(
            _socket,
            ssl_version=conn.init_kwargs.get(
                'ssl_version', SSL_DEFAULT_VERSION
            ),
            ciphers=conn.init_kwargs.get(
                'ssl_ciphers', SSL_DEFAULT_CIPHERS
            ),
            cert_reqs=conn.init_kwargs.get(
                'ssl_cert_reqs', ssl.CERT_NONE
            ),
            keyfile=conn.init_kwargs.get('ssl_keyfile', None),
            certfile=conn.init_kwargs.get('ssl_certfile', None),
            ca_certs=conn.init_kwargs.get('ssl_ca_certfile', None),
        )
    return _socket
