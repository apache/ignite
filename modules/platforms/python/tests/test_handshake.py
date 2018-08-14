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

import socket

from pyignite import Client
from pyignite.client.handshake import HandshakeRequest, read_response


def test_handshake(
    ignite_host, ignite_port, use_ssl, ssl_keyfile, ssl_certfile,
    ssl_ca_certfile, ssl_cert_reqs, ssl_ciphers, ssl_version
):
    client = Client(
        use_ssl=use_ssl,
        ssl_keyfile=ssl_keyfile,
        ssl_certfile=ssl_certfile,
        ssl_ca_certfile=ssl_ca_certfile,
        ssl_cert_reqs=ssl_cert_reqs,
        ssl_ciphers=ssl_ciphers,
        ssl_version=ssl_version
    )
    client.socket = client._wrap(socket.socket(socket.AF_INET, socket.SOCK_STREAM))
    client.socket.connect((ignite_host, ignite_port))
    hs_request = HandshakeRequest()
    client.send(hs_request)
    hs_response = read_response(client)
    assert hs_response.op_code != 0

    client.close()

    # intentionally pass wrong protocol version
    client.socket = client._wrap(socket.socket(socket.AF_INET, socket.SOCK_STREAM))
    client.socket.connect((ignite_host, ignite_port))
    hs_request = HandshakeRequest()
    hs_request.version_major = 10
    client.send(hs_request)
    hs_response = read_response(client)
    assert hs_response.op_code == 0

    client.close()
