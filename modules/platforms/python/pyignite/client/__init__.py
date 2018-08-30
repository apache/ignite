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

"""
This module contains `Client` class, that lets you communicate with Apache
Ignite cluster node by the means of Ignite binary client protocol.

To start the communication, you may connect to the node of their choice
by instantiating the `Client` object and calling
:py:meth:`~pyignite.client.Client.connect` method with proper parameters.
Client wraps TCP socket handling, as well as Ignite protocol handshaking.

The whole storage room of Ignite cluster is split up onto named structures,
called caches. For accessing the particular cache in key-value style
(a-la Redis or memcached) you should first create
the :class:`~pyignite.cache.Cache` object by calling
:py:meth:`~pyignite.client.Client.create_cache` or
:py:meth:`~pyignite.client.Client.get_or_create_cache()` methods, than call
:class:`~pyignite.cache.Cache` methods. If you wish to create a cache object
without communicating with server, there is also a
:py:meth:`~pyignite.client.Client.get_cache()` method that does just that.

For using Ignite SQL, call :py:meth:`~pyignite.client.Client.sql` method.
It returns a generator with result rows.

:py:meth:`~pyignite.client.Client.register_binary_type` and
:py:meth:`~pyignite.client.Client.query_binary_type` methods operates
the local (class-wise) registry for Ignite Complex objects.
"""

from collections import defaultdict, OrderedDict
import socket
from typing import Iterable, Type, Union

from pyignite.constants import *
from pyignite.exceptions import (
    BinaryTypeError, CacheError, HandshakeError, ParameterError,
    ReconnectError, SocketError, SQLError,
)
from pyignite.utils import (
    entity_id, is_iterable, schema_id, status_to_exception,
)
from .binary import GenericObjectMeta
from .handshake import HandshakeRequest, read_response
from .ssl import wrap


__all__ = ['Client']


class Client:
    """
    This is a main `pyignite` class, that represents a connection to Ignite
    node. It serves multiple purposes:

     * socket wrapper. Detects fragmentation and network errors. See also
       https://docs.python.org/3/howto/sockets.html,
     * binary protocol connector. Incapsulates handshake, data read-ahead and
       failover reconnection,
     * cache factory. Cache objects are used for key-value operations,
     * Ignite SQL endpoint,
     * binary types registration endpoint.
    """

    _socket = None
    nodes = None
    host = None
    port = None
    timeout = None
    prefetch = None
    username = None
    password = None

    _registry = defaultdict(dict)

    @staticmethod
    def _check_kwargs(kwargs):
        expected_args = [
            'timeout',
            'use_ssl',
            'ssl_version',
            'ssl_ciphers',
            'ssl_cert_reqs',
            'ssl_keyfile',
            'ssl_certfile',
            'ssl_ca_certfile',
            'username',
            'password',
        ]
        for kw in kwargs:
            if kw not in expected_args:
                raise ParameterError((
                    'Unexpected parameter for client initialization: `{}`'
                ).format(kw))

    def __init__(self, prefetch: bytes=b'', **kwargs):
        """
        Initialize client.

        For the use of the SSL-related parameters see
        https://docs.python.org/3/library/ssl.html#ssl-certificates.

        :param prefetch: (optional) initialize the read-ahead data buffer.
         Empty by default,
        :param timeout: (optional) sets timeout (in seconds) for each socket
         operation including `connect`. 0 means non-blocking mode. Can accept
         integer or float value. Default is None (blocking mode),
        :param use_ssl: (optional) set to True if Ignite server uses SSL
         on its binary connector. Defaults to use SSL when username
         and password has been supplied, not to use SSL otherwise,
        :param ssl_version: (optional) SSL version constant from standard
         `ssl` module. Defaults to TLS v1.1, as in Ignite 2.5,
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
        :param ssl_certfile: (optional) a path to ssl certificate file
         to identify local (client) party,
        :param ssl_ca_certfile: (optional) a path to a trusted certificate
         or a certificate chain. Required to check the validity of the remote
         (server-side) certificate,
        :param username: (optional) user name to authenticate to Ignite
         cluster,
        :param password: (optional) password to authenticate to Ignite cluster.
        """
        self.prefetch = prefetch
        self._check_kwargs(kwargs)
        self.timeout = kwargs.pop('timeout', None)
        self.username = kwargs.pop('username', None)
        self.password = kwargs.pop('password', None)
        if all([self.username, self.password, 'use_ssl' not in kwargs]):
            kwargs['use_ssl'] = True
        self.init_kwargs = kwargs

    read_response = read_response
    _wrap = wrap

    @property
    def socket(self) -> socket.socket:
        """
        Network socket.
        """
        if self._socket is None:
            self._reconnect()
        return self._socket

    def __repr__(self) -> str:
        if self.host and self.port:
            return '{}:{}'.format(self.host, self.port)
        else:
            return '<not connected>'

    def _connect(self, host: str, port: int):
        """
        Actually connect socket.
        """
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.settimeout(self.timeout)
        self._socket = self._wrap(self.socket)
        self._socket.connect((host, port))

        hs_request = HandshakeRequest(self.username, self.password)
        self.send(hs_request)
        hs_response = self.read_response()
        if hs_response['op_code'] == 0:
            self.close()
            raise HandshakeError(
                (
                    'Handshake error: {message} Expected protocol version: '
                    '{version_major}.{version_minor}.{version_patch}.'
                ).format(**hs_response)
            )
        self.host, self.port = host, port

    def connect(self, *args):
        """
        Connect to the server. Connection parameters may be either one node
        (host and port), or list (or other iterable) of nodes.

        :param host: Ignite server host,
        :param port: Ignite server port,
        :param nodes: iterable of (host, port) tuples.
        """
        self.nodes = iter([])
        if len(args) == 0:
            host, port = IGNITE_DEFAULT_HOST, IGNITE_DEFAULT_PORT
        elif len(args) == 1 and is_iterable(args[0]):
            self.nodes = iter(args[0])
            host, port = next(self.nodes)
        elif (
            len(args) == 2
            and isinstance(args[0], str)
            and isinstance(args[1], int)
        ):
            host, port = args
        else:
            raise ConnectionError('Connection parameters are not valid.')

        self._connect(host, port)

    def _reconnect(self):
        """
        Restore the connection using the next node in `nodes` iterable.
        """
        for host, port in self.nodes:
            try:
                self._connect(host, port)
                return
            except OSError:
                pass
        self.host = self.port = self.nodes = None
        # exception chaining gives a misleading traceback here
        raise ReconnectError('Can not reconnect: out of nodes') from None

    def _transfer_params(self, to: 'Client'):
        """
        Transfer non-SSL parameters to target client object.

        :param target: client object to transfer parameters to.
        """
        to.username = self.username
        to.password = self.password
        to.nodes = self.nodes
        to._registry = self._registry

    def clone(self, prefetch: bytes=b'') -> 'Client':
        """
        Clones this client in its current state.

        :return: `Client` object.
        """
        clone = Client(**self.init_kwargs)
        self._transfer_params(to=clone)
        if self.port and self.host:
            clone._connect(self.host, self.port)
        clone.prefetch = prefetch
        return clone

    def send(self, data: bytes, flags=None):
        """
        Send data down the socket.

        :param data: bytes to send,
        :param flags: (optional) OS-specific flags.
        """
        kwargs = {}
        if flags is not None:
            kwargs['flags'] = flags
        data = bytes(data)
        total_bytes_sent = 0

        while total_bytes_sent < len(data):
            try:
                bytes_sent = self.socket.send(data[total_bytes_sent:], **kwargs)
            except OSError:
                self._socket = self.host = self.port = None
                raise
            if bytes_sent == 0:
                self.socket.close()
                raise SocketError('Socket connection broken.')
            total_bytes_sent += bytes_sent

    def recv(self, buffersize, flags=None) -> bytes:
        """
        Receive data from socket or read-ahead buffer.

        :param buffersize: bytes to receive,
        :param flags: (optional) OS-specific flags,
        :return: data received.
        """
        pref_size = len(self.prefetch)
        if buffersize > pref_size:
            result = self.prefetch
            self.prefetch = b''
            try:
                result += self._recv(buffersize-pref_size, flags)
            except (SocketError, OSError):
                self._socket = self.host = self.port = None
                raise
            return result
        else:
            result = self.prefetch[:buffersize]
            self.prefetch = self.prefetch[buffersize:]
            return result

    def _recv(self, buffersize, flags=None) -> bytes:
        """
        Handle socket data reading.
        """
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

    @status_to_exception(BinaryTypeError)
    def get_binary_type(self, binary_type: Union[str, int]) -> dict:
        """
        Gets the binary type information from the Ignite server. This is quite
        a low-level implementation of Ignite thin client protocol's
        `OP_GET_BINARY_TYPE` operation. You would probably want to use
        :py:meth:`~pyignite.client.Client.query_binary_type` instead.

        :param binary_type: binary type name or ID,
        :return: binary type description − a dict with the following fields:

         - `type_exists`: True if the type is registered, False otherwise. In
           the latter case all the following fields are omitted,
         - `type_id`: Complex object type ID,
         - `type_name`: Complex object type name,
         - `affinity_key_field`: string value or None,
         - `is_enum`: False in case of Complex object registration,
         - `schemas`: a list, containing the Complex object schemas in format:
           OrderedDict[field name: field type hint]. A schema can be empty.
        """
        from pyignite.api.binary import get_binary_type
        from pyignite.datatypes.internal import tc_map

        def convert_type(tc_type: int):
            from pyignite.datatypes import BinaryObject

            try:
                return tc_map(tc_type.to_bytes(1, PROTOCOL_BYTE_ORDER))
            except (KeyError, OverflowError):
                # if conversion to char or type lookup failed,
                # we probably have a binary object type ID
                return BinaryObject

        def convert_schema(
            field_ids: list, binary_fields: list
        ) -> OrderedDict:
            converted_schema = OrderedDict()
            for field_id in field_ids:
                binary_field = [
                    x
                    for x in binary_fields
                    if x['field_id'] == field_id
                ][0]
                converted_schema[binary_field['field_name']] = convert_type(
                    binary_field['type_id']
                )
            return converted_schema

        result = get_binary_type(self, binary_type)
        if result.status != 0 or not result.value['type_exists']:
            return result

        binary_fields = result.value.pop('binary_fields')
        old_format_schemas = result.value.pop('schema')
        result.value['schemas'] = []
        for s_id, field_ids in old_format_schemas.items():
            result.value['schemas'].append(
                convert_schema(field_ids, binary_fields)
            )
        return result

    @status_to_exception(BinaryTypeError)
    def put_binary_type(
        self, type_name: str, affinity_key_field: str=None,
        is_enum=False, schema: dict=None
    ):
        """
        Registers binary type information in cluster. Do not update binary
        registry. This is a literal implementation of Ignite thin client
        protocol's `OP_PUT_BINARY_TYPE` operation. You would probably want
        to use :py:meth:`~pyignite.client.Client.register_binary_type` instead.

        :param type_name: name of the data type being registered,
        :param affinity_key_field: (optional) name of the affinity key field,
        :param is_enum: (optional) register enum if True, binary object
         otherwise. Defaults to False,
        :param schema: (optional) when register enum, pass a dict
         of enumerated parameter names as keys and an integers as values.
         When register binary type, pass a dict of field names: field types.
         Binary type with no fields is OK.
        """
        from pyignite.api.binary import put_binary_type

        return put_binary_type(
            self, type_name, affinity_key_field, is_enum, schema
        )

    @staticmethod
    def _create_dataclass(type_name: str, schema: OrderedDict=None) -> Type:
        """
        Creates default (generic) class for Ignite Complex object.

        :param type_name: Complex object type name,
        :param schema: Complex object schema,
        :return: the resulting class.
        """
        schema = schema or {}
        return GenericObjectMeta(type_name, (), {}, schema=schema)

    def _sync_binary_registry(self, type_id: int):
        """
        Reads Complex object description from Ignite server. Creates default
        Complex object classes and puts in registry, if not already there.

        :param type_id: Complex object type ID.
        """
        type_info = self.get_binary_type(type_id)
        if type_info['type_exists']:
            for schema in type_info['schemas']:
                if not self._registry[type_id].get(schema_id(schema), None):
                    data_class = self._create_dataclass(
                        type_info['type_name'],
                        schema,
                    )
                    self._registry[type_id][schema_id(schema)] = data_class

    def register_binary_type(
        self, data_class: Type, affinity_key_field: str=None,
    ):
        """
        Register the given class as a representation of a certain Complex
        object type. Discards autogenerated or previously registered class.

        :param data_class: Complex object class,
        :param affinity_key_field: (optional) affinity parameter.
        """
        if not self.query_binary_type(
            data_class.type_id, data_class.schema_id
        ):
            self.put_binary_type(
                data_class.type_name,
                affinity_key_field,
                schema=data_class.schema,
            )
        self._registry[data_class.type_id][data_class.schema_id] = data_class

    def query_binary_type(
        self, binary_type: Union[int, str], schema: Union[int, dict]=None,
        sync: bool=True
    ):
        """
        Queries the registry of Complex object classes.

        :param binary_type: Complex object type name or ID,
        :param schema: (optional) Complex object schema or schema ID,
        :param sync: (optional) look up the Ignite server for registered
         Complex objects and create data classes for them if needed,
        :return: found dataclass or None, if `schema` parameter is provided,
         a dict of {schema ID: dataclass} format otherwise.
        """
        type_id = entity_id(binary_type)
        s_id = schema_id(schema)

        if schema:
            try:
                result = self._registry[type_id][s_id]
            except KeyError:
                result = None
        else:
            result = self._registry[type_id]

        if sync and not result:
            self._sync_binary_registry(type_id)
            return self.query_binary_type(type_id, s_id, sync=False)

        return result

    def create_cache(self, settings: Union[str, dict]) -> 'Cache':
        """
        Creates Ignite cache by name. Raises `CacheError` if such a cache is
        already exists.

        :param settings: cache name or dict of cache properties' codes
         and values. All cache properties are documented here:
         :ref:`cache_props`. See also the
         :ref:`cache creation example <sql_cache_create>`,
        :return: :class:`~pyignite.cache.Cache` object.
        """
        from pyignite.cache import Cache

        return Cache(self, settings)

    def get_or_create_cache(self, settings: Union[str, dict]) -> 'Cache':
        """
        Creates Ignite cache, if not exist.

        :param settings: cache name or dict of cache properties' codes
         and values. All cache properties are documented here:
         :ref:`cache_props`. See also the
         :ref:`cache creation example <sql_cache_create>`,
        :return: :class:`~pyignite.cache.Cache` object.
        """
        from pyignite.cache import Cache

        return Cache(self, settings, with_get=True)

    def get_cache(self, settings: Union[str, dict]) -> 'Cache':
        """
        Creates Cache object with a given cache name without checking it up
        on server. If such a cache does not exist, some kind of exception
        (most probably `CacheError`) may be raised later.

        :param settings: cache name or cache properties (but only `PROP_NAME`
         property is allowed),
        :return: :class:`~pyignite.cache.Cache` object.
        """
        from pyignite.cache import Cache

        return Cache(self, settings, get_only=True)

    @status_to_exception(CacheError)
    def get_cache_names(self) -> list:
        """
        Gets existing cache names.

        :return: list of cache names.
        """
        from pyignite.api.cache_config import cache_get_names

        return cache_get_names(self)

    def sql(
        self, query_str: str, page_size: int=1, query_args: Iterable=None,
        schema: Union[int, str]='PUBLIC',
        statement_type: int=0, distributed_joins: bool=False,
        local: bool=False, replicated_only: bool=False,
        enforce_join_order: bool=False, collocated: bool=False,
        lazy: bool=False, include_field_names: bool=False,
        max_rows: int=-1, timeout: int=0,
    ):
        """
        Runs an SQL query and returns its result.

        :param query_str: SQL query string,
        :param page_size: (optional) cursor page size. Default is 1, which
         means that client makes one server call per row,
        :param query_args: (optional) query arguments. List of values or
         (value, type hint) tuples,
        :param schema: (optional) schema for the query. Defaults to `PUBLIC`,
        :param statement_type: (optional) statement type. Can be:

         * StatementType.ALL − any type (default),
         * StatementType.SELECT − select,
         * StatementType.UPDATE − update.

        :param distributed_joins: (optional) distributed joins. Defaults
         to False,
        :param local: (optional) pass True if this query should be executed
         on local node only. Defaults to False,
        :param replicated_only: (optional) whether query contains only
         replicated tables or not. Defaults to False,
        :param enforce_join_order: (optional) enforce join order. Defaults
         to False,
        :param collocated: (optional) whether your data is co-located or not.
         Defaults to False,
        :param lazy: (optional) lazy query execution. Defaults to False,
        :param include_field_names: (optional) include field names in result.
         Defaults to False,
        :param max_rows: (optional) query-wide maximum of rows. Defaults to -1
         (all rows),
        :param timeout: (optional) non-negative timeout value in ms.
         Zero disables timeout (default),
        :return: generator with result rows as a lists. If
         `include_field_names` was set, the first row will hold field names.
        """
        from pyignite.api.sql import sql_fields, sql_fields_cursor_get_page

        def generate_result(value):
            cursor = value['cursor']
            more = value['more']

            if include_field_names:
                yield value['fields']
                field_count = len(value['fields'])
            else:
                field_count = value['field_count']
            for line in value['data']:
                yield line

            while more:
                inner_result = sql_fields_cursor_get_page(
                    self, cursor, field_count
                )
                if inner_result.status != 0:
                    raise SQLError(result.message)
                more = inner_result.value['more']
                for line in inner_result.value['data']:
                    yield line

        schema = self.get_or_create_cache(schema)
        result = sql_fields(
            self, schema.cache_id, query_str,
            page_size, query_args, schema.name,
            statement_type, distributed_joins, local, replicated_only,
            enforce_join_order, collocated, lazy, include_field_names,
            max_rows, timeout,
        )
        if result.status != 0:
            raise SQLError(result.message)

        return generate_result(result.value)

    def close(self):
        """
        Mark socket closed. This is recommended but not required, since
        sockets are automatically closed when they are garbage-collected.
        """
        self._socket.shutdown(socket.SHUT_RDWR)
        self._socket.close()
        self._socket = self.host = self.port = None
