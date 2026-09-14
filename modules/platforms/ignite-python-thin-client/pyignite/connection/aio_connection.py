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

import asyncio
from collections import OrderedDict
from typing import Union

from pyignite.constants import PROTOCOLS, PROTOCOL_BYTE_ORDER
from pyignite.exceptions import HandshakeError, SocketError, connection_errors, AuthenticationError
from .bitmask_feature import BitmaskFeature
from .connection import BaseConnection

from .handshake import HandshakeRequest, HandshakeResponse
from .protocol_context import ProtocolContext
from .ssl import create_ssl_context
from ..stream.binary_stream import BinaryStreamBase


class BaseProtocol(asyncio.Protocol):
    def __init__(self, conn, handshake_fut):
        super().__init__()
        self._buffer = bytearray()
        self._conn = conn
        self._handshake_fut = handshake_fut

    def connection_lost(self, exc):
        self.__process_connection_error(exc if exc else SocketError("Connection closed"))

    def connection_made(self, transport: asyncio.WriteTransport) -> None:
        try:
            self.__send_handshake(transport, self._conn)
        except Exception as e:
            if not self._handshake_fut.done():
                self._handshake_fut.set_exception(e)

    def data_received(self, data: bytes) -> None:
        self._buffer += data
        while self.__has_full_response():
            packet_sz = self.__packet_size(self._buffer)
            packet = self._buffer[0:packet_sz]
            if not self._handshake_fut.done():
                hs_response = self.__parse_handshake(packet, self._conn.client)
                self._handshake_fut.set_result(hs_response)
            elif not self._handshake_fut.cancelled() or not self._handshake_fut.exception():
                self._conn.process_message(packet)
            self._buffer = self._buffer[packet_sz:len(self._buffer)]

    def __has_full_response(self):
        if len(self._buffer) > 4:
            response_len = int.from_bytes(self._buffer[0:4], byteorder=PROTOCOL_BYTE_ORDER, signed=True)
            return response_len + 4 <= len(self._buffer)

    @staticmethod
    def __packet_size(buffer):
        return int.from_bytes(buffer[0:4], byteorder=PROTOCOL_BYTE_ORDER, signed=True) + 4

    def __process_connection_error(self, exc):
        connected = self._handshake_fut.done()
        if not connected:
            self._handshake_fut.set_exception(exc)
        self._conn.process_connection_lost(exc, connected)

    @staticmethod
    def __send_handshake(transport, conn):
        hs_request = HandshakeRequest(conn.protocol_context, conn.username, conn.password)
        with BinaryStreamBase(client=conn.client) as stream:
            hs_request.from_python(stream)
            transport.write(stream.getvalue())

    @staticmethod
    def __parse_handshake(data, client):
        with BinaryStreamBase(client, data) as stream:
            return HandshakeResponse.parse(stream, client.protocol_context)


class AioConnection(BaseConnection):
    """
    Asyncio connection to Ignite node. It serves multiple purposes:

    * wrapper of asyncio streams. See also https://docs.python.org/3/library/asyncio-stream.html
    * encapsulates handshake and reconnection.
    """

    def __init__(self, client: 'AioClient', host: str, port: int, username: str = None, password: str = None,
                 **ssl_params):
        """
        Initialize connection.

        For the use of the SSL-related parameters see
        https://docs.python.org/3/library/ssl.html#ssl-certificates.

        :param client: Ignite client object,
        :param host: Ignite server node's host name or IP,
        :param port: Ignite server node's port number,
        :param handshake_timeout: (optional) sets timeout (in seconds) for performing handshake (connection)
         with node. Default is 10.0 seconds,
        :param use_ssl: (optional) set to True if Ignite server uses SSL
         on its binary connector. Defaults to use SSL when username
         and password has been supplied, not to use SSL otherwise,
        :param ssl_version: (optional) SSL version constant from standard
         `ssl` module. Defaults to TLS v1.2,
        :param ssl_ciphers: (optional) ciphers to use. If not provided,
         `ssl` default ciphers are used,
        :param ssl_cert_reqs: (optional) determines how the remote side
         certificate is treated:

         * `ssl.CERT_NONE` − remote certificate is ignored (default),
         * `ssl.CERT_OPTIONAL` − remote certificate will be validated,
           if provided,
         * `ssl.CERT_REQUIRED` − valid remote certificate is required,

        :param ssl_keyfile: (optional) a path to SSL key file to identify
         local (client) party,
        :param ssl_keyfile_password: (optional) password for SSL key file,
         can be provided when key file is encrypted to prevent OpenSSL
         password prompt,
        :param ssl_certfile: (optional) a path to ssl certificate file
         to identify local (client) party,
        :param ssl_ca_certfile: (optional) a path to a trusted certificate
         or a certificate chain. Required to check the validity of the remote
         (server-side) certificate,
        :param username: (optional) user name to authenticate to Ignite
         cluster,
        :param password: (optional) password to authenticate to Ignite cluster.
        """
        super().__init__(client, host, port, username, password, **ssl_params)
        self._pending_reqs = {}
        self._transport = None
        self._loop = asyncio.get_event_loop()
        self._closed = False
        self._transport_closed_fut = None

    @property
    def closed(self) -> bool:
        """ Tells if socket is closed. """
        return self._closed or not self._transport or self._transport.is_closing()

    async def connect(self):
        """
        Connect to the given server node with protocol version fallback.
        """
        if self.alive:
            return
        await self._connect()

    async def _connect(self):
        detecting_protocol = False

        # choose highest version first
        if self.client.protocol_context is None:
            detecting_protocol = True
            self.client.protocol_context = ProtocolContext(max(PROTOCOLS), BitmaskFeature.all_supported())

        while True:
            try:
                self._on_handshake_start()
                result = await self._connect_version()
                self._on_handshake_success(result)
                return
            except HandshakeError as e:
                if e.expected_version in PROTOCOLS:
                    self.client.protocol_context.version = e.expected_version
                    continue
                else:
                    self._on_handshake_fail(e)
                    raise e
            except AuthenticationError as e:
                self._on_handshake_fail(e)
                raise e
            except Exception as e:
                self._on_handshake_fail(e)
                # restore undefined protocol version
                if detecting_protocol:
                    self.client.protocol_context = None
                raise e

    def process_connection_lost(self, err, reconnect=False):
        self.failed = True
        for _, fut in self._pending_reqs.items():
            if not fut.done():
                fut.set_exception(err)
        self._pending_reqs.clear()

        if self._transport_closed_fut and not self._transport_closed_fut.done():
            self._transport_closed_fut.set_result(None)

        if reconnect and not self._closed:
            self._on_connection_lost(err)
            self._loop.create_task(self._reconnect())

    def process_message(self, data):
        req_id = int.from_bytes(data[4:12], byteorder=PROTOCOL_BYTE_ORDER, signed=True)

        req_fut = self._pending_reqs.get(req_id)
        if req_fut:
            if not req_fut.done():
                req_fut.set_result(data)
            del self._pending_reqs[req_id]

    async def _connect_version(self) -> Union[dict, OrderedDict]:
        """
        Connect to the given server node using protocol version
        defined on client.
        """

        ssl_context = create_ssl_context(self.ssl_params)
        handshake_fut = self._loop.create_future()
        self._closed = False
        self._transport, _ = await self._loop.create_connection(lambda: BaseProtocol(self, handshake_fut),
                                                                host=self.host, port=self.port, ssl=ssl_context)
        try:
            hs_response = await asyncio.wait_for(handshake_fut, self.handshake_timeout)
        except asyncio.TimeoutError:
            raise ConnectionError('timed out')

        if hs_response.op_code == 0:
            await self.close()
            self._process_handshake_error(hs_response)

        return hs_response

    async def reconnect(self):
        await self._reconnect()

    async def _reconnect(self):
        if self.alive:
            return

        await self._close_transport()
        # connect and silence the connection errors
        try:
            await self._connect()
        except connection_errors:
            pass

    async def request(self, query_id, data: Union[bytes, bytearray]) -> bytearray:
        """
        Perform request.
        :param query_id: id of query.
        :param data: bytes to send.
        """
        if not self.alive:
            raise SocketError('Attempt to use closed connection.')

        return await self._send(query_id, data)

    async def _send(self, query_id, data):
        fut = self._loop.create_future()
        self._pending_reqs[query_id] = fut
        self._transport.write(data)
        return await fut

    async def close(self):
        self._closed = True
        await self._close_transport()

    async def _close_transport(self):
        """
        Close connection.
        """
        if self._transport and not self._transport.is_closing():
            self._transport_closed_fut = self._loop.create_future()

            self._transport.close()
            self._transport = None
            try:
                await asyncio.wait_for(self._transport_closed_fut, 1.0)
            except asyncio.TimeoutError:
                pass
            finally:
                self._on_connection_lost(expected=True)
                self._transport_closed_fut = None
