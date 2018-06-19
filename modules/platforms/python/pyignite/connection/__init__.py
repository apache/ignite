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

from pyignite.constants import *
from .handshake import HandshakeRequest, read_response


class SocketError(Exception):
    pass


class Connection:
    """
    Socket wrapper. Detects fragmentation and network errors.

    https://docs.python.org/3/howto/sockets.html
    """

    socket = None

    def __init__(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    read_response = read_response

    def connect(self, host, port):
        if self.socket is not None:
            self.socket.connect((host, port))

        hs_request = HandshakeRequest()
        self.send(hs_request)
        hs_response = self.read_response()
        if hs_response.op_code == 0:
            self.close()
            raise SocketError(
                (
                    'Ignite protocol version mismatch: requested {}.{}.{}, '
                    'received {}.{}.{}.'
                ).format(
                    PROTOCOL_VERSION_MAJOR,
                    PROTOCOL_VERSION_MINOR,
                    PROTOCOL_VERSION_PATCH,
                    hs_response.version_major,
                    hs_response.version_minor,
                    hs_response.version_patch,
                )
            )

    def send(self, data, flags=None):
        kwargs = {}
        if flags is not None:
            kwargs['flags'] = flags
        data = bytes(data)
        total_bytes_sent = 0

        while total_bytes_sent < len(data):
            bytes_sent = self.socket.send(data[total_bytes_sent:], **kwargs)
            if bytes_sent == 0:
                self.socket.close()
                raise SocketError('Socket connection broken.')
            total_bytes_sent += bytes_sent

    def recv(self, buffersize, flags=None):
        kwargs = {}
        if flags is not None:
            kwargs['flags'] = flags
        chunks = []
        bytes_rcvd = 0

        while bytes_rcvd < buffersize:
            chunk = self.socket.recv(buffersize-bytes_rcvd, **kwargs)
            if chunk == b'':
                self.socket.close()
                raise SocketError('Socket connection broken.')
            chunks.append(chunk)
            bytes_rcvd += len(chunk)

        return b''.join(chunks)

    def close(self):
        self.socket.close()
        self.socket = None


class PrefetchConnection(Connection):
    prefetch = None
    conn = None

    def __init__(self, conn: Connection, prefetch: bytes=b''):
        super().__init__()
        self.conn = conn
        self.prefetch = prefetch

    def recv(self, buffersize, flags=None):
        pref_size = len(self.prefetch)
        if buffersize > pref_size:
            result = self.prefetch + self.conn.recv(
                buffersize-pref_size, flags)
            self.prefetch = b''
            return result
        else:
            result = self.prefetch[:buffersize]
            self.prefetch = self.prefetch[buffersize:]
            return result
