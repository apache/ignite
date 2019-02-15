#                   GridGain Community Edition Licensing
#                   Copyright 2019 GridGain Systems, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
# Restriction; you may not use this file except in compliance with the License. You may obtain a
# copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the specific language governing permissions
# and limitations under the License.
#
# Commons Clause Restriction
#
# The Software is provided to you by the Licensor under the License, as defined below, subject to
# the following condition.
#
# Without limiting other conditions in the License, the grant of rights under the License will not
# include, and the License does not grant to you, the right to Sell the Software.
# For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
# under the License to provide to third parties, for a fee or other consideration (including without
# limitation fees for hosting or consulting/ support services related to the Software), a product or
# service whose value derives, entirely or substantially, from the functionality of the Software.
# Any license notice or attribution required by the License must also include this Commons Clause
# License Condition notice.
#
# For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
# the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
# Edition software provided with this notice.

import socket

from pyignite import Client
from pyignite.connection.handshake import HandshakeRequest, read_response


def test_handshake(
    monkeypatch,
    ignite_host, ignite_port, use_ssl, ssl_keyfile, ssl_certfile,
    ssl_ca_certfile, ssl_cert_reqs, ssl_ciphers, ssl_version,
    username, password,
):
    client = Client(
        use_ssl=use_ssl,
        ssl_keyfile=ssl_keyfile,
        ssl_certfile=ssl_certfile,
        ssl_ca_certfile=ssl_ca_certfile,
        ssl_cert_reqs=ssl_cert_reqs,
        ssl_ciphers=ssl_ciphers,
        ssl_version=ssl_version,
        username=username,
        password=password,
    )
    client._socket = client._wrap(
        socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    )
    client.socket.connect((ignite_host, ignite_port))
    hs_request = HandshakeRequest(username, password)
    client.send(hs_request)
    hs_response = read_response(client)
    assert hs_response['op_code'] != 0

    client.close()

    # intentionally pass wrong protocol version
    from pyignite.connection import handshake
    monkeypatch.setattr(handshake, 'PROTOCOL_VERSION_MAJOR', 10)

    client._socket = client._wrap(
        socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    )
    client.socket.connect((ignite_host, ignite_port))
    hs_request = HandshakeRequest(username, password)
    client.send(hs_request)
    hs_response = read_response(client)
    assert hs_response['op_code'] == 0

    client.close()
