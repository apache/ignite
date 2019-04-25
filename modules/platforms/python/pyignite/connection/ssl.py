#
# Copyright 2019 GridGain Systems, Inc. and Contributors.
#
# Licensed under the GridGain Community Edition License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import ssl

from pyignite.constants import *


def wrap(client, _socket):
    """ Wrap socket in SSL wrapper. """
    if client.init_kwargs.get('use_ssl', None):
        _socket = ssl.wrap_socket(
            _socket,
            ssl_version=client.init_kwargs.get(
                'ssl_version', SSL_DEFAULT_VERSION
            ),
            ciphers=client.init_kwargs.get(
                'ssl_ciphers', SSL_DEFAULT_CIPHERS
            ),
            cert_reqs=client.init_kwargs.get(
                'ssl_cert_reqs', ssl.CERT_NONE
            ),
            keyfile=client.init_kwargs.get('ssl_keyfile', None),
            certfile=client.init_kwargs.get('ssl_certfile', None),
            ca_certs=client.init_kwargs.get('ssl_ca_certfile', None),
        )
    return _socket
