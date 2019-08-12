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
"""
This module contains `Client` class, that lets you communicate with GridGain
cluster node by the means of GridGain binary client protocol.

To start the communication, you may connect to the node of their choice
by instantiating the `Client` object and calling
:py:meth:`~pygridgain.connection.Connection.connect` method with proper
parameters.

The whole storage room of GridGain cluster is split up into named structures,
called caches. For accessing the particular cache in key-value style
(a-la Redis or memcached) you should first create
the :class:`~pygridgain.cache.Cache` object by calling
:py:meth:`~pygridgain.client.Client.create_cache` or
:py:meth:`~pygridgain.client.Client.get_or_create_cache()` methods, than call
:class:`~pygridgain.cache.Cache` methods. If you wish to create a cache object
without communicating with server, there is also a
:py:meth:`~pygridgain.client.Client.get_cache()` method that does just that.

For using GridGain SQL, call :py:meth:`~pygridgain.client.Client.sql` method.
It returns a generator with result rows.

:py:meth:`~pygridgain.client.Client.register_binary_type` and
:py:meth:`~pygridgain.client.Client.query_binary_type` methods operates
the local (class-wise) registry for GridGain Complex objects.
"""

from collections import defaultdict, OrderedDict
import random
import re
from typing import Dict, Iterable, List, Optional, Tuple, Type, Union

from .api.binary import get_binary_type, put_binary_type
from .api.cache_config import cache_get_names
from .api.sql import sql_fields, sql_fields_cursor_get_page
from .cache import Cache
from .connection import Connection
from .constants import *
from .datatypes import BinaryObject
from .datatypes.internal import tc_map
from .exceptions import (
    BinaryTypeError, CacheError, ReconnectError, SQLError, connection_errors,
)
from .utils import (
    capitalize, entity_id, schema_id, process_delimiter, select_version,
    status_to_exception, is_iterable,
)
from .binary import GenericObjectMeta


__all__ = ['Client']


class Client:
    """
    This is a main `pygridgain` class, that is build upon the
    :class:`~pygridgain.connection.Connection`. In addition to the attributes,
    properties and methods of its parent class, `Client` implements
    the following features:

     * cache factory. Cache objects are used for key-value operations,
     * GridGain SQL endpoint,
     * binary types registration endpoint.
    """

    _registry = defaultdict(dict)
    _compact_footer: bool = None
    _connection_args: Dict = None
    _current_node: int = None
    _nodes: List[Connection] = None

    # used for Complex object data class names sanitizing
    _identifier = re.compile(r'[^0-9a-zA-Z_.+$]', re.UNICODE)
    _ident_start = re.compile(r'^[^a-zA-Z_]+', re.UNICODE)

    affinity_version: Optional[Tuple] = None
    protocol_version: Optional[Tuple] = None

    def __init__(
        self, compact_footer: bool = None, affinity_aware: bool = False,
        **kwargs
    ):
        """
        Initialize client.

        :param compact_footer: (optional) use compact (True, recommended) or
         full (False) schema approach when serializing Complex objects.
         Default is to use the same approach the server is using (None).
         GridGain binary protocol documentation on this topic:
         https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-schema
        :param affinity_aware: (optional) try to calculate the exact data
         placement from the key before to issue the key operation to the
         server node:
         https://cwiki.apache.org/confluence/display/IGNITE/IEP-23%3A+Best+Effort+Affinity+for+thin+clients
         The feature is in experimental status, so the parameter is `False`
         by default. This will be changed later.
        """
        self._compact_footer = compact_footer
        self._connection_args = kwargs
        self._nodes = []
        self._current_node = 0
        self._affinity_aware = affinity_aware
        self.affinity_version = (0, 0)

    def get_protocol_version(self) -> Optional[Tuple]:
        """
        Returns the tuple of major, minor, and revision numbers of the used
        thin protocol version, or None, if no connection to the GridGain
        cluster was not yet established.

        This method is not a part of the public API. Unless you wish to
        extend the `pygridgain` capabilities (with additional testing,
        logging, examining connections, et c.) you probably should not use it.
        """
        return self.protocol_version

    @property
    def affinity_aware(self):
        return self._affinity_aware

    def connect(self, *args):
        """
        Connect to GridGain cluster node(s).

        :param args: (optional) host(s) and port(s) to connect to.
        """
        if len(args) == 0:
            # no parameters − use default GridGain host and port
            nodes = [(DEFAULT_HOST, DEFAULT_PORT)]
        elif len(args) == 1 and is_iterable(args[0]):
            # iterable of host-port pairs is given
            nodes = args[0]
        elif (
            len(args) == 2
            and isinstance(args[0], str)
            and isinstance(args[1], int)
        ):
            # host and port are given
            nodes = [args]
        else:
            raise ConnectionError('Connection parameters are not valid.')

        # the following code is quite twisted, because the protocol version
        # is initially unknown

        # TODO: open first node in foregroung, others − in background
        for i, node in enumerate(nodes):
            host, port = node
            conn = Connection(self, **self._connection_args)
            conn.host = host
            conn.port = port

            try:
                if (
                    self.protocol_version is None
                    or self.protocol_version >= (1, 4, 0)
                ):
                    # open connection before adding to the pool
                    conn.connect(host, port)

                    # now we have the protocol version
                    if self.protocol_version < (1, 4, 0):
                        # do not try to open more nodes
                        self._current_node = i
                    else:
                        # take a chance to schedule the reconnection
                        # for all the failed connections, that was probed
                        # before this
                        for failed_node in self._nodes[:i]:
                            failed_node.reconnect()

            except connection_errors:
                conn._fail()
                if (
                    self.protocol_version
                    and self.protocol_version >= (1, 4, 0)
                ):
                    # schedule the reconnection
                    conn.reconnect()

            self._nodes.append(conn)

        if self.protocol_version is None:
            raise ReconnectError('Can not connect.')

    def close(self):
        for conn in self._nodes:
            conn.close()
        self._nodes.clear()

    @property
    @select_version
    def random_node(self) -> Connection:
        """
        Returns random usable node.

        This method is not a part of the public API. Unless you wish to
        extend the `pygridgain` capabilities (with additional testing,
        logging, examining connections, et c.) you probably should not use it.
        """
        try:
            return random.choice(
                list(n for n in self._nodes if n.alive)
            )
        except IndexError:
            # cannot choose from an empty sequence
            raise ReconnectError('Can not reconnect: out of nodes.') from None

    def random_node_130(self):
        # it actually returns the next usable node, but the name stands for
        # the code unification reason
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
        seq = list(range(len(self._nodes)))
        seq = seq[self._current_node:] + seq[:self._current_node]

        for i in seq:
            node = self._nodes[i]
            try:
                node.connect(node.host, node.port)
            except connection_errors:
                pass
            else:
                return node

        # no nodes left
        raise ReconnectError('Can not reconnect: out of nodes.')

    random_node_120 = random_node_130

    @status_to_exception(BinaryTypeError)
    def get_binary_type(self, binary_type: Union[str, int]) -> dict:
        """
        Gets the binary type information from the GridGain server. This is quite
        a low-level implementation of GridGain thin client protocol's
        `OP_GET_BINARY_TYPE` operation. You would probably want to use
        :py:meth:`~pygridgain.client.Client.query_binary_type` instead.

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
        def convert_type(tc_type: int):
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

        conn = self.random_node

        result = get_binary_type(conn, binary_type)
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
        return (
            self.__class__._compact_footer
            or self.__class__._compact_footer is None
        )

    @compact_footer.setter
    def compact_footer(self, value: bool):
        # normally schema approach should not change
        if self.__class__._compact_footer not in (value, None):
            raise Warning('Can not change client schema approach.')
        else:
            self.__class__._compact_footer = value

    @status_to_exception(BinaryTypeError)
    def put_binary_type(
        self, type_name: str, affinity_key_field: str = None,
        is_enum=False, schema: dict = None
    ):
        """
        Registers binary type information in cluster. Do not update binary
        registry. This is a literal implementation of GridGain thin client
        protocol's `OP_PUT_BINARY_TYPE` operation. You would probably want
        to use :py:meth:`~pygridgain.client.Client.register_binary_type`
        instead.

        :param type_name: name of the data type being registered,
        :param affinity_key_field: (optional) name of the affinity key field,
        :param is_enum: (optional) register enum if True, binary object
         otherwise. Defaults to False,
        :param schema: (optional) when register enum, pass a dict
         of enumerated parameter names as keys and an integers as values.
         When register binary type, pass a dict of field names: field types.
         Binary type with no fields is OK.
        """
        return put_binary_type(
            self.random_node, type_name, affinity_key_field, is_enum, schema
        )

    @staticmethod
    def _create_dataclass(type_name: str, schema: OrderedDict = None) -> Type:
        """
        Creates default (generic) class for GridGain Complex object.

        :param type_name: Complex object type name,
        :param schema: Complex object schema,
        :return: the resulting class.
        """
        schema = schema or {}
        return GenericObjectMeta(type_name, (), {}, schema=schema)

    def _sync_binary_registry(self, type_id: int):
        """
        Reads Complex object description from GridGain server. Creates default
        Complex object classes and puts in registry, if not already there.

        :param type_id: Complex object type ID.
        """
        type_info = self.get_binary_type(type_id)
        if type_info['type_exists']:
            for schema in type_info['schemas']:
                if not self._registry[type_id].get(schema_id(schema), None):
                    data_class = self._create_dataclass(
                        self._create_type_name(type_info['type_name']),
                        schema,
                    )
                    self._registry[type_id][schema_id(schema)] = data_class

    @classmethod
    def _create_type_name(cls, type_name: str) -> str:
        """
        Creates Python data class name from GridGain binary type name.

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

    def register_binary_type(
        self, data_class: Type, affinity_key_field: str = None,
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
        self, binary_type: Union[int, str], schema: Union[int, dict] = None,
        sync: bool = True
    ):
        """
        Queries the registry of Complex object classes.

        :param binary_type: Complex object type name or ID,
        :param schema: (optional) Complex object schema or schema ID,
        :param sync: (optional) look up the GridGain server for registered
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
        Creates GridGain cache by name. Raises `CacheError` if such a cache is
        already exists.

        :param settings: cache name or dict of cache properties' codes
         and values. All cache properties are documented here:
         :ref:`cache_props`. See also the
         :ref:`cache creation example <sql_cache_create>`,
        :return: :class:`~pygridgain.cache.Cache` object.
        """
        return Cache(self, settings)

    def get_or_create_cache(self, settings: Union[str, dict]) -> 'Cache':
        """
        Creates GridGain cache, if not exist.

        :param settings: cache name or dict of cache properties' codes
         and values. All cache properties are documented here:
         :ref:`cache_props`. See also the
         :ref:`cache creation example <sql_cache_create>`,
        :return: :class:`~pygridgain.cache.Cache` object.
        """
        return Cache(self, settings, with_get=True)

    def get_cache(self, settings: Union[str, dict]) -> 'Cache':
        """
        Creates Cache object with a given cache name without checking it up
        on server. If such a cache does not exist, some kind of exception
        (most probably `CacheError`) may be raised later.

        :param settings: cache name or cache properties (but only `PROP_NAME`
         property is allowed),
        :return: :class:`~pygridgain.cache.Cache` object.
        """
        return Cache(self, settings, get_only=True)

    @status_to_exception(CacheError)
    def get_cache_names(self) -> list:
        """
        Gets existing cache names.

        :return: list of cache names.
        """
        return cache_get_names(self.random_node)

    def sql(
        self, query_str: str, page_size: int = 1, query_args: Iterable = None,
        schema: Union[int, str] = 'PUBLIC',
        statement_type: int = 0, distributed_joins: bool = False,
        local: bool = False, replicated_only: bool = False,
        enforce_join_order: bool = False, collocated: bool = False,
        lazy: bool = False, include_field_names: bool = False,
        max_rows: int = -1, timeout: int = 0,
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
                    conn, cursor, field_count
                )
                if inner_result.status != 0:
                    raise SQLError(result.message)
                more = inner_result.value['more']
                for line in inner_result.value['data']:
                    yield line

        conn = self.random_node

        schema = self.get_or_create_cache(schema)
        result = sql_fields(
            conn, schema.cache_id, query_str,
            page_size, query_args, schema.name,
            statement_type, distributed_joins, local, replicated_only,
            enforce_join_order, collocated, lazy, include_field_names,
            max_rows, timeout,
        )
        if result.status != 0:
            raise SQLError(result.message)

        return generate_result(result.value)
