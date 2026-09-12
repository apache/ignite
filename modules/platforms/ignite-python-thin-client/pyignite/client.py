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
:py:meth:`~pyignite.connection.Connection.connect` method with proper
parameters.

The whole storage room of Ignite cluster is split up into named structures,
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
import time
from collections import defaultdict, OrderedDict
import random
import re
from itertools import chain
from typing import Iterable, Type, Union, Any, Dict, Optional, Sequence

from .api import cache_get_node_partitions
from .api.binary import get_binary_type, put_binary_type
from .api.cache_config import cache_get_names
from .cluster import Cluster
from .cursors import SqlFieldsCursor
from .cache import Cache, create_cache, get_cache, get_or_create_cache, BaseCache
from .connection import Connection
from .constants import IGNITE_DEFAULT_HOST, IGNITE_DEFAULT_PORT, PROTOCOL_BYTE_ORDER, AFFINITY_RETRIES, AFFINITY_DELAY
from .datatypes import BinaryObject, AnyDataObject, TransactionConcurrency, TransactionIsolation
from .datatypes.base import IgniteDataType
from .datatypes.internal import tc_map
from .exceptions import BinaryTypeError, CacheError, ReconnectError, connection_errors
from .queries.cache_info import CacheInfo
from .stream import BinaryStream, READ_BACKWARD
from .transaction import Transaction
from .utils import (
    cache_id, capitalize, entity_id, schema_id, process_delimiter, status_to_exception, is_iterable,
    get_field_by_id, unsigned
)
from .binary import GenericObjectMeta
from .monitoring import _EventListeners


__all__ = ['Client']


class BaseClient:
    # used for Complex object data class names sanitizing
    _identifier = re.compile(r'[^0-9a-zA-Z_.+$]', re.UNICODE)
    _ident_start = re.compile(r'^[^a-zA-Z_]+', re.UNICODE)

    def __init__(self, compact_footer: bool = None, partition_aware: bool = False,
                 event_listeners: Optional[Sequence] = None, **kwargs):
        self._compact_footer = compact_footer
        self._partition_aware = partition_aware
        self._connection_args = kwargs
        self._registry = defaultdict(dict)
        self._nodes = []
        self._current_node = 0
        self._partition_aware = partition_aware
        self.affinity_version = (0, 0)
        self._affinity = {'version': self.affinity_version, 'partition_mapping': defaultdict(dict)}
        self._protocol_context = None
        self._event_listeners = _EventListeners(event_listeners)

    @property
    def protocol_context(self):
        """
        Returns protocol context, or None, if no connection to the Ignite
        cluster was not yet established.

        This method is not a part of the public API. Unless you wish to
        extend the `pyignite` capabilities (with additional testing, logging,
        examining connections, et c.) you probably should not use it.
        """
        return self._protocol_context

    @protocol_context.setter
    def protocol_context(self, value):
        self._protocol_context = value

    @property
    def partition_aware(self):
        return self._partition_aware and self.partition_awareness_supported_by_protocol

    @property
    def partition_awareness_supported_by_protocol(self):
        return self.protocol_context is not None \
            and self.protocol_context.is_partition_awareness_supported()

    @property
    def compact_footer(self) -> bool:
        """
        This property remembers Complex object schema encoding approach when
        decoding any Complex object, to use the same approach on Complex
        object encoding.

        :return: True if compact schema was used by server or no Complex
         object decoding has yet taken place, False if full schema was used.
        """
        # this is an ordinary object property, but its backing storage
        # is a class attribute

        # use compact schema by default, but leave initial (falsy) backing
        # value unchanged
        return self._compact_footer or self._compact_footer is None

    @compact_footer.setter
    def compact_footer(self, value: bool):
        # normally schema approach should not change
        if self._compact_footer not in (value, None):
            raise Warning('Can not change client schema approach.')
        else:
            self._compact_footer = value

    @staticmethod
    def _process_connect_args(*args):
        if len(args) == 0:
            # no parameters − use default Ignite host and port
            return [(IGNITE_DEFAULT_HOST, IGNITE_DEFAULT_PORT)]
        if len(args) == 1 and is_iterable(args[0]):
            # iterable of host-port pairs is given
            return args[0]
        if len(args) == 2 and isinstance(args[0], str) and isinstance(args[1], int):
            # host and port are given
            return [args]

        raise ConnectionError('Connection parameters are not valid.')

    def _process_get_binary_type_result(self, result):
        if result.status != 0 or not result.value['type_exists']:
            return result

        binary_fields = result.value.pop('binary_fields')
        old_format_schemas = result.value.pop('schema')
        result.value['schemas'] = []
        for s_id, field_ids in old_format_schemas.items():
            result.value['schemas'].append(self._convert_schema(field_ids, binary_fields))
        return result

    @staticmethod
    def _convert_type(tc_type: int):
        try:
            return tc_map(tc_type.to_bytes(1, PROTOCOL_BYTE_ORDER))
        except (KeyError, OverflowError):
            # if conversion to char or type lookup failed,
            # we probably have a binary object type ID
            return BinaryObject

    def _convert_schema(self, field_ids: list, binary_fields: list) -> OrderedDict:
        converted_schema = OrderedDict()
        for field_id in field_ids:
            binary_field = next(x for x in binary_fields if x['field_id'] == field_id)
            converted_schema[binary_field['field_name']] = self._convert_type(binary_field['type_id'])
        return converted_schema

    @staticmethod
    def _create_dataclass(type_name: str, schema: OrderedDict = None) -> Type:
        """
        Creates default (generic) class for Ignite Complex object.

        :param type_name: Complex object type name,
        :param schema: Complex object schema,
        :return: the resulting class.
        """
        schema = schema or {}
        return GenericObjectMeta(type_name, (), {}, schema=schema)

    @classmethod
    def _create_type_name(cls, type_name: str) -> str:
        """
        Creates Python data class name from Ignite binary type name.

        Handles all the special cases found in
        `java.org.apache.ignite.binary.BinaryBasicNameMapper.simpleName()`.
        Tries to adhere to PEP8 along the way.
        """

        # general sanitizing
        type_name = cls._identifier.sub('', type_name)

        # - name ending with '$' (Scala)
        # - name + '$' + some digits (anonymous class)
        # - '$$Lambda$' in the middle
        type_name = process_delimiter(type_name, '$')

        # .NET outer/inner class delimiter
        type_name = process_delimiter(type_name, '+')

        # Java fully qualified class name
        type_name = process_delimiter(type_name, '.')

        # start chars sanitizing
        type_name = capitalize(cls._ident_start.sub('', type_name))

        return type_name

    def _sync_binary_registry(self, type_id: int, type_info: dict):
        """
        Sync binary registry
        :param type_id: Complex object type ID.
        :param type_info: Complex object type info.
        """
        if type_info['type_exists']:
            for schema in type_info['schemas']:
                if not self._registry[type_id].get(schema_id(schema), None):
                    data_class = self._create_dataclass(
                        self._create_type_name(type_info['type_name']),
                        schema,
                    )
                    self._registry[type_id][schema_id(schema)] = data_class

    def _get_from_registry(self, type_id, schema):
        """
        Get binary type info from registry.

        :param type_id: Complex object type ID.
        :param schema: Complex object schema.
        """
        if schema:
            try:
                return self._registry[type_id][schema_id(schema)]
            except KeyError:
                return None
        return self._registry[type_id]

    def register_cache(self, cache_id: int):
        if self.partition_aware and cache_id not in self._affinity:
            self._affinity['partition_mapping'][cache_id] = {}

    def _get_affinity_key(self, cache_id, key, key_hint=None):
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        cache_partition_mapping = self._cache_partition_mapping(cache_id)
        if cache_partition_mapping and cache_partition_mapping.get('is_applicable'):
            config = cache_partition_mapping.get('cache_config')
            if config:
                affinity_key_id = config.get(key_hint.type_id)

                if affinity_key_id and isinstance(key, GenericObjectMeta):
                    return get_field_by_id(key, affinity_key_id)

        return key, key_hint

    def _update_affinity(self, full_affinity):
        self._affinity['version'] = full_affinity['version']

        full_mapping = full_affinity.get('partition_mapping')
        if full_mapping:
            self._affinity['partition_mapping'].update(full_mapping)

    def _caches_to_update_affinity(self):
        if self._affinity['version'] < self.affinity_version:
            return list(self._affinity['partition_mapping'].keys())
        else:
            return list(c_id for c_id, c_mapping in self._affinity['partition_mapping'].items() if not c_mapping)

    def _cache_partition_mapping(self, cache_id):
        return self._affinity['partition_mapping'][cache_id]

    def _get_node_by_hashcode(self, cache_id, hashcode, parts):
        """
        Get node by key hashcode. Calculate partition and return node on that it is primary.
        (algorithm is taken from `RendezvousAffinityFunction.java`)
        """

        # calculate partition for key or affinity key
        # (algorithm is taken from `RendezvousAffinityFunction.java`)
        mask = parts - 1

        if parts & mask == 0:
            part = (hashcode ^ (unsigned(hashcode) >> 16)) & mask
        else:
            part = abs(hashcode // parts)

        assert 0 <= part < parts, 'Partition calculation has failed'

        node_mapping = self._cache_partition_mapping(cache_id).get('node_mapping')
        if not node_mapping:
            return None

        node_uuid, best_conn = None, None
        for u, p in node_mapping.items():
            if part in p:
                node_uuid = u
                break

        if node_uuid:
            for n in self._nodes:
                if n.uuid == node_uuid:
                    best_conn = n
                    break
            if best_conn and best_conn.alive:
                return best_conn


class _ConnectionContextManager:
    def __init__(self, client, nodes):
        self.client = client
        self.nodes = nodes
        self.client._connect(self.nodes)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()


class Client(BaseClient):
    """
    Synchronous Client implementation.
    """

    def __init__(self, compact_footer: bool = None, partition_aware: bool = True,
                 event_listeners: Optional[Sequence] = None, **kwargs):
        """
        Initialize client.

        For the use of the SSL-related parameters see
        https://docs.python.org/3/library/ssl.html#ssl-certificates.

        :param compact_footer: (optional) use compact (True, recommended) or
         full (False) schema approach when serializing Complex objects.
         Default is to use the same approach the server is using (None).
         Apache Ignite binary protocol documentation on this topic:
         https://ignite.apache.org/docs/latest/binary-client-protocol/data-format#schema
        :param partition_aware: (optional) try to calculate the exact data
         placement from the key before to issue the key operation to the
         server node, `True` by default,
        :param event_listeners: (optional) event listeners,
        :param timeout: (optional) sets timeout (in seconds) for each socket
         operation including `connect`. 0 means non-blocking mode, which is
         virtually guaranteed to fail. Can accept integer or float value.
         Default is None (blocking mode),
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
        super().__init__(compact_footer, partition_aware, event_listeners, **kwargs)

    def connect(self, *args):
        """
        Connect to Ignite cluster node(s).

        :param args: (optional) host(s) and port(s) to connect to.
        """
        nodes = self._process_connect_args(*args)
        return _ConnectionContextManager(self, nodes)

    def _connect(self, nodes):
        # the following code is quite twisted, because the protocol version
        # is initially unknown

        # TODO: open first node in foreground, others − in background
        for i, node in enumerate(nodes):
            host, port = node
            conn = Connection(self, host, port, **self._connection_args)

            try:
                if self.protocol_context is None or self.partition_aware:
                    # open connection before adding to the pool
                    conn.connect()

                    # now we have the protocol version
                    if not self.partition_aware:
                        # do not try to open more nodes
                        self._current_node = i

            except connection_errors:
                if self.partition_aware:
                    # schedule the reconnection
                    conn.reconnect()

            self._nodes.append(conn)

        if self.protocol_context is None:
            raise ReconnectError('Can not connect.')

    def close(self):
        for conn in self._nodes:
            conn.close()
        self._nodes.clear()

    @property
    def random_node(self) -> Connection:
        """
        Returns random usable node.

        This method is not a part of the public API. Unless you wish to
        extend the `pyignite` capabilities (with additional testing, logging,
        examining connections, et c.) you probably should not use it.
        """
        if self.partition_aware:
            # if partition awareness is used just pick a random connected node
            return self._get_random_node()
        else:
            # if partition awareness is not used then just return the current
            # node if it's alive or the next usable node if connection with the
            # current is broken
            node = self._nodes[self._current_node]
            if node.alive:
                return node

            # close current (supposedly failed) node
            self._nodes[self._current_node].close()

            # advance the node index
            self._current_node += 1
            if self._current_node >= len(self._nodes):
                self._current_node = 0

            # prepare the list of node indexes to try to connect to
            num_nodes = len(self._nodes)
            for i in chain(range(self._current_node, num_nodes), range(self._current_node)):
                node = self._nodes[i]
                try:
                    node.connect()
                except connection_errors:
                    pass
                else:
                    return node

            # no nodes left
            raise ReconnectError('Can not reconnect: out of nodes.')

    def _get_random_node(self, reconnect=True):
        alive_nodes = [n for n in self._nodes if n.alive]
        if alive_nodes:
            return random.choice(alive_nodes)
        elif reconnect:
            for n in self._nodes:
                n.reconnect()

            return self._get_random_node(reconnect=False)
        else:
            # cannot choose from an empty sequence
            raise ReconnectError('Can not reconnect: out of nodes.') from None

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
        result = get_binary_type(self.random_node, binary_type)
        return self._process_get_binary_type_result(result)

    @status_to_exception(BinaryTypeError)
    def put_binary_type(
        self, type_name: str, affinity_key_field: str = None,
        is_enum=False, schema: dict = None
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
        return put_binary_type(self.random_node, type_name, affinity_key_field, is_enum, schema)

    def register_binary_type(self, data_class: Type, affinity_key_field: str = None):
        """
        Register the given class as a representation of a certain Complex
        object type. Discards autogenerated or previously registered class.

        :param data_class: Complex object class,
        :param affinity_key_field: (optional) affinity parameter.
        """
        if not self.query_binary_type(data_class.type_id, data_class.schema_id):
            self.put_binary_type(data_class.type_name, affinity_key_field, schema=data_class.schema)
        self._registry[data_class.type_id][data_class.schema_id] = data_class

    def query_binary_type(self, binary_type: Union[int, str], schema: Union[int, dict] = None):
        """
        Queries the registry of Complex object classes.

        :param binary_type: Complex object type name or ID,
        :param schema: (optional) Complex object schema or schema ID
        :return: found dataclass or None, if `schema` parameter is provided,
         a dict of {schema ID: dataclass} format otherwise.
        """
        type_id = entity_id(binary_type)

        result = self._get_from_registry(type_id, schema)
        if not result:
            type_info = self.get_binary_type(type_id)
            self._sync_binary_registry(type_id, type_info)
            return self._get_from_registry(type_id, schema)

        return result

    def unwrap_binary(self, value: Any) -> Any:
        """
        Detects and recursively unwraps Binary Object or collections of BinaryObject.

        :param value: anything that could be a Binary Object or collection of BinaryObject,
        :return: the result of the Binary Object unwrapping with all other data
         left intact.
        """
        if isinstance(value, tuple) and len(value) == 2:
            if type(value[0]) is bytes and type(value[1]) is int:
                blob, offset = value
                with BinaryStream(self, blob) as stream:
                    data_class = BinaryObject.parse(stream)
                    return BinaryObject.to_python(stream.read_ctype(data_class, direction=READ_BACKWARD), client=self)

            if isinstance(value[0], int):
                col_type, collection = value
                if isinstance(collection, list):
                    return col_type, [self.unwrap_binary(v) for v in collection]

                if isinstance(collection, dict):
                    return col_type, {self.unwrap_binary(k): self.unwrap_binary(v) for k, v in collection.items()}
        return value

    @status_to_exception(CacheError)
    def _get_affinity(self, conn: 'Connection', caches: Iterable[int]) -> Dict:
        """
        Queries server for affinity mappings. Retries in case
        of an intermittent error (most probably “Getting affinity for topology
        version earlier than affinity is calculated”).

        :param conn: connection to Ignite server,
        :param caches: Ids of caches,
        :return: OP_CACHE_PARTITIONS operation result value.
        """
        for _ in range(AFFINITY_RETRIES or 1):
            result = cache_get_node_partitions(conn, caches)
            if result.status == 0:
                break
            time.sleep(AFFINITY_DELAY)

        return result

    def get_best_node(
            self, cache: Union[int, str, 'BaseCache'], key: Any = None, key_hint: 'IgniteDataType' = None
    ) -> 'Connection':
        """
        Returns the node from the list of the nodes, opened by client, that
        most probably contains the needed key-value pair. See IEP-23.

        This method is not a part of the public API. Unless you wish to
        extend the `pyignite` capabilities (with additional testing, logging,
        examining connections, et c.) you probably should not use it.

        :param cache: Ignite cache, cache name or cache id,
        :param key: (optional) pythonic key,
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        :return: Ignite connection object.
        """
        conn = self.random_node

        if self.partition_aware and key is not None:
            caches = self._caches_to_update_affinity()
            if caches:
                # update partition mapping
                while True:
                    try:
                        full_affinity = self._get_affinity(conn, caches)
                        break
                    except connection_errors:
                        # retry if connection failed
                        conn = self.random_node
                        pass
                    except CacheError:
                        # server did not create mapping in time
                        return conn

                self._update_affinity(full_affinity)

                for node in self._nodes:
                    if not node.alive:
                        node.reconnect()

            c_id = cache.cache_id if isinstance(cache, BaseCache) else cache_id(cache)
            parts = self._cache_partition_mapping(c_id).get('number_of_partitions')

            if not parts:
                return conn

            key, key_hint = self._get_affinity_key(c_id, key, key_hint)
            hashcode = key_hint.hashcode(key, client=self)

            best_node = self._get_node_by_hashcode(c_id, hashcode, parts)
            if best_node:
                return best_node

        return conn

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
        return create_cache(self, settings)

    def get_or_create_cache(self, settings: Union[str, dict]) -> 'Cache':
        """
        Creates Ignite cache, if not exist.

        :param settings: cache name or dict of cache properties' codes
         and values. All cache properties are documented here:
         :ref:`cache_props`. See also the
         :ref:`cache creation example <sql_cache_create>`,
        :return: :class:`~pyignite.cache.Cache` object.
        """
        return get_or_create_cache(self, settings)

    def get_cache(self, settings: Union[str, dict]) -> 'Cache':
        """
        Creates Cache object with a given cache name without checking it up
        on server. If such a cache does not exist, some kind of exception
        (most probably `CacheError`) may be raised later.

        :param settings: cache name or cache properties (but only `PROP_NAME`
         property is allowed),
        :return: :class:`~pyignite.cache.Cache` object.
        """
        return get_cache(self, settings)

    @status_to_exception(CacheError)
    def get_cache_names(self) -> list:
        """
        Gets existing cache names.

        :return: list of cache names.
        """
        return cache_get_names(self.random_node)

    def sql(
        self, query_str: str, page_size: int = 1024,
        query_args: Iterable = None, schema: str = 'PUBLIC',
        statement_type: int = 0, distributed_joins: bool = False,
        local: bool = False, replicated_only: bool = False,
        enforce_join_order: bool = False, collocated: bool = False,
        lazy: bool = False, include_field_names: bool = False,
        max_rows: int = -1, timeout: int = 0,
        cache: Union[int, str, Cache] = None
    ) -> SqlFieldsCursor:
        """
        Runs an SQL query and returns its result.

        :param query_str: SQL query string,
        :param page_size: (optional) cursor page size. Default is 1024, which
         means that client makes one server call per 1024 rows,
        :param query_args: (optional) query arguments. List of values or
         (value, type hint) tuples,
        :param schema: (optional) schema for the query. Defaults to `PUBLIC`,
        :param statement_type: (optional) statement type. Can be:

         * StatementType.ALL − any type (default),
         * StatementType.SELECT − select,
         * StatementType.UPDATE − update.

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
        :param cache: (optional) Name or ID of the cache to use to infer schema.
         If set, 'schema' argument is ignored,
        :return: sql fields cursor with result rows as a lists. If
         `include_field_names` was set, the first row will hold field names.
        """
        if isinstance(cache, (int, str)):
            c_info = CacheInfo(cache_id=cache_id(cache), protocol_context=self.protocol_context)
        elif isinstance(cache, Cache):
            c_info = cache.cache_info
        else:
            c_info = CacheInfo(protocol_context=self.protocol_context)

        if c_info.cache_id:
            schema = None

        return SqlFieldsCursor(self, c_info, query_str, page_size, query_args, schema, statement_type,
                               distributed_joins, local, replicated_only, enforce_join_order, collocated, lazy,
                               include_field_names, max_rows, timeout)

    def get_cluster(self) -> 'Cluster':
        """
        Get client cluster facade.

        :return: :py:class:`~pyignite.cluster.Cluster` instance.
        """
        return Cluster(self)

    def tx_start(self, concurrency: TransactionConcurrency = TransactionConcurrency.PESSIMISTIC,
                 isolation: TransactionIsolation = TransactionIsolation.REPEATABLE_READ,
                 timeout: int = 0, label: Optional[str] = None) -> 'Transaction':
        """
        Start thin client transaction.

        :param concurrency: (optional) transaction concurrency, see
                :py:class:`~pyignite.datatypes.transactions.TransactionConcurrency`,
        :param isolation: (optional) transaction isolation level, see
                :py:class:`~pyignite.datatypes.transactions.TransactionIsolation`,
        :param timeout: (optional) transaction timeout in milliseconds,
        :param label: (optional) transaction label.
        :return: :py:class:`~pyignite.transaction.Transaction` instance.
        """
        return Transaction(self, concurrency, isolation, timeout, label)
