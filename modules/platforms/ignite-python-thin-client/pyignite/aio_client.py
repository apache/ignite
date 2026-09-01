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
import random
import sys
from itertools import chain
from typing import Iterable, Type, Union, Any, Dict, Optional, Sequence

from .aio_cluster import AioCluster
from .api import cache_get_node_partitions_async
from .api.binary import get_binary_type_async, put_binary_type_async
from .api.cache_config import cache_get_names_async
from .cache import BaseCache
from .client import BaseClient
from .cursors import AioSqlFieldsCursor
from .aio_cache import AioCache, get_cache, create_cache, get_or_create_cache
from .connection import AioConnection
from .constants import AFFINITY_RETRIES, AFFINITY_DELAY
from .datatypes import BinaryObject, TransactionConcurrency, TransactionIsolation
from .exceptions import BinaryTypeError, CacheError, ReconnectError, connection_errors, NotSupportedError
from .queries.cache_info import CacheInfo
from .stream import AioBinaryStream, READ_BACKWARD
from .transaction import AioTransaction
from .utils import cache_id, entity_id, status_to_exception


__all__ = ['AioClient']


class _ConnectionContextManager:
    def __init__(self, client, nodes):
        self.client = client
        self.nodes = nodes

    def __await__(self):
        return (yield from self.__aenter__().__await__())

    async def __aenter__(self):
        await self.client._connect(self.nodes)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.close()


class AioClient(BaseClient):
    """
    Asynchronous Client implementation.
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
        self._registry_mux = asyncio.Lock()
        self._affinity_query_mux = asyncio.Lock()

    def connect(self, *args):
        """
        Connect to Ignite cluster node(s).

        :param args: (optional) host(s) and port(s) to connect to.
        """
        nodes = self._process_connect_args(*args)
        return _ConnectionContextManager(self, nodes)

    async def _connect(self, nodes):
        for i, node in enumerate(nodes):
            host, port = node
            conn = AioConnection(self, host, port, **self._connection_args)

            if not self.partition_aware:
                try:
                    if self.protocol_context is None:
                        # open connection before adding to the pool
                        await conn.connect()

                        # do not try to open more nodes
                        self._current_node = i
                except connection_errors:
                    pass

            self._nodes.append(conn)

        if self.partition_aware:
            connect_results = await asyncio.gather(
                *[conn.connect() for conn in self._nodes],
                return_exceptions=True
            )

            reconnect_coro = []
            for i, res in enumerate(connect_results):
                if isinstance(res, Exception):
                    if isinstance(res, connection_errors):
                        reconnect_coro.append(self._nodes[i].reconnect())
                    else:
                        raise res

            await asyncio.gather(*reconnect_coro, return_exceptions=True)

        if self.protocol_context is None:
            raise ReconnectError('Can not connect.')

    async def close(self):
        await asyncio.gather(*[conn.close() for conn in self._nodes], return_exceptions=True)
        self._nodes.clear()

    async def random_node(self) -> AioConnection:
        """
        Returns random usable node.

        This method is not a part of the public API. Unless you wish to
        extend the `pyignite` capabilities (with additional testing, logging,
        examining connections, et c.) you probably should not use it.
        """
        if self.partition_aware:
            # if partition awareness is used just pick a random connected node
            return await self._get_random_node()
        else:
            # if partition awareness is not used then just return the current
            # node if it's alive or the next usable node if connection with the
            # current is broken
            node = self._nodes[self._current_node]
            if node.alive:
                return node

            # close current (supposedly failed) node
            await self._nodes[self._current_node].close()

            # advance the node index
            self._current_node += 1
            if self._current_node >= len(self._nodes):
                self._current_node = 0

            # prepare the list of node indexes to try to connect to
            for i in chain(range(self._current_node, len(self._nodes)), range(self._current_node)):
                node = self._nodes[i]
                try:
                    await node.connect()
                except connection_errors:
                    pass
                else:
                    return node

            # no nodes left
            raise ReconnectError('Can not reconnect: out of nodes.')

    async def _get_random_node(self, reconnect=True):
        alive_nodes = [n for n in self._nodes if n.alive]
        if alive_nodes:
            return random.choice(alive_nodes)
        elif reconnect:
            await asyncio.gather(*[n.reconnect() for n in self._nodes], return_exceptions=True)
            return await self._get_random_node(reconnect=False)
        else:
            # cannot choose from an empty sequence
            raise ReconnectError('Can not reconnect: out of nodes.') from None

    @status_to_exception(BinaryTypeError)
    async def get_binary_type(self, binary_type: Union[str, int]) -> dict:
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
        conn = await self.random_node()
        result = await get_binary_type_async(conn, binary_type)
        return self._process_get_binary_type_result(result)

    @status_to_exception(BinaryTypeError)
    async def put_binary_type(self, type_name: str, affinity_key_field: str = None, is_enum=False, schema: dict = None):
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
        conn = await self.random_node()
        return await put_binary_type_async(conn, type_name, affinity_key_field, is_enum, schema)

    async def register_binary_type(self, data_class: Type, affinity_key_field: str = None):
        """
        Register the given class as a representation of a certain Complex
        object type. Discards autogenerated or previously registered class.

        :param data_class: Complex object class,
        :param affinity_key_field: (optional) affinity parameter.
        """
        if not await self.query_binary_type(data_class.type_id, data_class.schema_id):
            await self.put_binary_type(data_class.type_name, affinity_key_field, schema=data_class.schema)

        self._registry[data_class.type_id][data_class.schema_id] = data_class

    async def query_binary_type(self, binary_type: Union[int, str], schema: Union[int, dict] = None):
        """
        Queries the registry of Complex object classes.

        :param binary_type: Complex object type name or ID,
        :param schema: (optional) Complex object schema or schema ID,
        :return: found dataclass or None, if `schema` parameter is provided,
         a dict of {schema ID: dataclass} format otherwise.
        """
        type_id = entity_id(binary_type)

        result = self._get_from_registry(type_id, schema)

        if not result:
            async with self._registry_mux:
                result = self._get_from_registry(type_id, schema)

                if not result:
                    type_info = await self.get_binary_type(type_id)
                    self._sync_binary_registry(type_id, type_info)
                    return self._get_from_registry(type_id, schema)

        return result

    async def unwrap_binary(self, value: Any) -> Any:
        """
        Detects and recursively unwraps Binary Object.

        :param value: anything that could be a Binary Object,
        :return: the result of the Binary Object unwrapping with all other data
         left intact.
        """
        if isinstance(value, tuple) and len(value) == 2:
            if type(value[0]) is bytes and type(value[1]) is int:
                blob, offset = value
                with AioBinaryStream(self, blob) as stream:
                    data_class = await BinaryObject.parse_async(stream)
                    return await BinaryObject.to_python_async(stream.read_ctype(data_class, direction=READ_BACKWARD),
                                                              client=self)

            if isinstance(value[0], int):
                col_type, collection = value
                if isinstance(collection, list):
                    coros = [self.unwrap_binary(v) for v in collection]
                    return col_type, await asyncio.gather(*coros)

                if isinstance(collection, dict):
                    coros = [asyncio.gather(self.unwrap_binary(k), self.unwrap_binary(v))
                             for k, v in collection.items()]
                    return col_type, dict(await asyncio.gather(*coros))
        return value

    @status_to_exception(CacheError)
    async def _get_affinity(self, conn: 'AioConnection', caches: Iterable[int]) -> Dict:
        """
        Queries server for affinity mappings. Retries in case
        of an intermittent error (most probably “Getting affinity for topology
        version earlier than affinity is calculated”).

        :param conn: connection to Ignite server,
        :param caches: Ids of caches,
        :return: OP_CACHE_PARTITIONS operation result value.
        """
        for _ in range(AFFINITY_RETRIES or 1):
            result = await cache_get_node_partitions_async(conn, caches)
            if result.status == 0:
                break
            await asyncio.sleep(AFFINITY_DELAY)

        return result

    async def get_best_node(
            self, cache: Union[int, str, 'BaseCache'], key: Any = None, key_hint: 'IgniteDataType' = None
    ) -> 'AioConnection':
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
        conn = await self.random_node()

        if self.partition_aware and key is not None:
            caches = self._caches_to_update_affinity()
            if caches:
                async with self._affinity_query_mux:
                    while True:
                        caches = self._caches_to_update_affinity()
                        if not caches:
                            break

                        try:
                            full_affinity = await self._get_affinity(conn, caches)
                            self._update_affinity(full_affinity)

                            asyncio.ensure_future(
                                asyncio.gather(
                                    *[node.reconnect() for node in self._nodes if not node.alive],
                                    return_exceptions=True
                                )
                            )

                            break
                        except connection_errors:
                            # retry if connection failed
                            conn = await self.random_node()
                            pass
                        except CacheError:
                            # server did not create mapping in time
                            return conn

            c_id = cache.cache_id if isinstance(cache, BaseCache) else cache_id(cache)
            parts = self._cache_partition_mapping(c_id).get('number_of_partitions')

            if not parts:
                return conn

            key, key_hint = self._get_affinity_key(c_id, key, key_hint)

            hashcode = await key_hint.hashcode_async(key, client=self)

            best_node = self._get_node_by_hashcode(c_id, hashcode, parts)
            if best_node:
                return best_node

        return conn

    async def create_cache(self, settings: Union[str, dict]) -> 'AioCache':
        """
        Creates Ignite cache by name. Raises `CacheError` if such a cache is
        already exists.

        :param settings: cache name or dict of cache properties' codes
         and values. All cache properties are documented here:
         :ref:`cache_props`. See also the
         :ref:`cache creation example <sql_cache_create>`,
        :return: :class:`~pyignite.cache.Cache` object.
        """
        return await create_cache(self, settings)

    async def get_or_create_cache(self, settings: Union[str, dict]) -> 'AioCache':
        """
        Creates Ignite cache, if not exist.

        :param settings: cache name or dict of cache properties' codes
         and values. All cache properties are documented here:
         :ref:`cache_props`. See also the
         :ref:`cache creation example <sql_cache_create>`,
        :return: :class:`~pyignite.cache.Cache` object.
        """
        return await get_or_create_cache(self, settings)

    async def get_cache(self, settings: Union[str, dict]) -> 'AioCache':
        """
        Creates Cache object with a given cache name without checking it up
        on server. If such a cache does not exist, some kind of exception
        (most probably `CacheError`) may be raised later.

        :param settings: cache name or cache properties (but only `PROP_NAME`
         property is allowed),
        :return: :class:`~pyignite.cache.Cache` object.
        """
        return await get_cache(self, settings)

    @status_to_exception(CacheError)
    async def get_cache_names(self) -> list:
        """
        Gets existing cache names.

        :return: list of cache names.
        """
        conn = await self.random_node()
        return await cache_get_names_async(conn)

    def sql(
        self, query_str: str, page_size: int = 1024,
        query_args: Iterable = None, schema: str = 'PUBLIC',
        statement_type: int = 0, distributed_joins: bool = False,
        local: bool = False, replicated_only: bool = False,
        enforce_join_order: bool = False, collocated: bool = False,
        lazy: bool = False, include_field_names: bool = False,
        max_rows: int = -1, timeout: int = 0,
        cache: Union[int, str, 'AioCache'] = None
    ) -> AioSqlFieldsCursor:
        """
        Runs an SQL query and returns its result.

        :param query_str: SQL query string,
        :param page_size: (optional) cursor page size. Default is 1024, which
         means that client makes one server call per 1024 rows,
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
        :param cache: (optional) Name or ID of the cache to use to infer schema.
         If set, 'schema' argument is ignored,
        :return: async sql fields cursor with result rows as a lists. If
         `include_field_names` was set, the first row will hold field names.
        """
        if isinstance(cache, (int, str)):
            c_info = CacheInfo(cache_id=cache_id(cache), protocol_context=self.protocol_context)
        elif isinstance(cache, AioCache):
            c_info = cache.cache_info
        else:
            c_info = CacheInfo(protocol_context=self.protocol_context)

        if c_info.cache_id:
            schema = None

        return AioSqlFieldsCursor(self, c_info, query_str, page_size, query_args, schema, statement_type,
                                  distributed_joins, local, replicated_only, enforce_join_order, collocated,
                                  lazy, include_field_names, max_rows, timeout)

    def get_cluster(self) -> 'AioCluster':
        """
        Get client cluster facade.

        :return: :py:class:`~pyignite.aio_cluster.AioCluster` instance.
        """
        return AioCluster(self)

    def tx_start(self, concurrency: TransactionConcurrency = TransactionConcurrency.PESSIMISTIC,
                 isolation: TransactionIsolation = TransactionIsolation.REPEATABLE_READ,
                 timeout: int = 0, label: Optional[str] = None) -> 'AioTransaction':
        """
        Start async thin client transaction. **Supported only python 3.7+**

        :param concurrency: (optional) transaction concurrency, see
                :py:class:`~pyignite.datatypes.transactions.TransactionConcurrency`,
        :param isolation: (optional) transaction isolation level, see
                :py:class:`~pyignite.datatypes.transactions.TransactionIsolation`,
        :param timeout: (optional) transaction timeout in milliseconds,
        :param label: (optional) transaction label.
        :return: :py:class:`~pyignite.transaction.AioTransaction` instance.
        """
        if sys.version_info < (3, 7):
            raise NotSupportedError(f"Transactions are not supported in async client on current python {sys.version}")
        return AioTransaction(self, concurrency, isolation, timeout, label)
