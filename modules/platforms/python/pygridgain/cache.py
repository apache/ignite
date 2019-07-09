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
import time
from typing import Any, Dict, Iterable, Optional, Tuple, Union

from .constants import *
from .binary import GenericObjectMeta
from .datatypes import prop_codes
from .datatypes.internal import AnyDataObject
from .exceptions import (
    CacheCreationError, CacheError, ParameterError, SQLError,
    connection_errors,
)
from .utils import (
    cache_id, get_field_by_id, is_wrapped, select_version,
    status_to_exception, unsigned, unwrap_binary,
)
from .api.cache_config import (
    cache_create, cache_create_with_config,
    cache_get_or_create, cache_get_or_create_with_config,
    cache_destroy, cache_get_configuration,
)
from .api.key_value import (
    cache_get, cache_put, cache_get_all, cache_put_all, cache_replace,
    cache_clear, cache_clear_key, cache_clear_keys,
    cache_contains_key, cache_contains_keys,
    cache_get_and_put, cache_get_and_put_if_absent, cache_put_if_absent,
    cache_get_and_remove, cache_get_and_replace,
    cache_remove_key, cache_remove_keys, cache_remove_all,
    cache_remove_if_equals, cache_replace_if_equals, cache_get_size,
)
from .api.sql import scan, scan_cursor_get_page, sql, sql_cursor_get_page
from .api.affinity import cache_get_node_partitions


PROP_CODES = set([
    getattr(prop_codes, x)
    for x in dir(prop_codes)
    if x.startswith('PROP_')
])
CACHE_CREATE_FUNCS = {
    True: {
        True: cache_get_or_create_with_config,
        False: cache_create_with_config,
    },
    False: {
        True: cache_get_or_create,
        False: cache_create,
    },
}


class Cache:
    """
    GridGain cache abstraction. Users should never use this class directly,
    but construct its instances with
    :py:meth:`~pygridgain.client.Client.create_cache`,
    :py:meth:`~pygridgain.client.Client.get_or_create_cache` or
    :py:meth:`~pygridgain.client.Client.get_cache` methods instead. See
    :ref:`this example <create_cache>` on how to do it.
    """

    affinity = None
    _cache_id = None
    _name = None
    _client = None
    _settings = None

    @staticmethod
    def _validate_settings(
        settings: Union[str, dict] = None, get_only: bool = False,
    ):
        if any([
            not settings,
            type(settings) not in (str, dict),
            type(settings) is dict and prop_codes.PROP_NAME not in settings,
        ]):
            raise ParameterError('You should supply at least cache name')

        if all([
            type(settings) is dict,
            not set(settings).issubset(PROP_CODES),
        ]):
            raise ParameterError('One or more settings was not recognized')

        if get_only and type(settings) is dict and len(settings) != 1:
            raise ParameterError('Only cache name allowed as a parameter')

    def __init__(
        self, client: 'Client', settings: Union[str, dict] = None,
        with_get: bool = False, get_only: bool = False,
    ):
        """
        Initialize cache object.

        :param client: GridGain client,
        :param settings: cache settings. Can be a string (cache name) or a dict
         of cache properties and their values. In this case PROP_NAME is
         mandatory,
        :param with_get: (optional) do not raise exception, if the cache
         is already exists. Defaults to False,
        :param get_only: (optional) do not communicate with GridGain server
         at all, only create Cache instance. Defaults to False.
        """
        self._client = client
        self._validate_settings(settings)
        if type(settings) == str:
            self._name = settings
        else:
            self._name = settings[prop_codes.PROP_NAME]

        if not get_only:
            func = CACHE_CREATE_FUNCS[type(settings) is dict][with_get]
            result = func(client.random_node, settings)
            if result.status != 0:
                raise CacheCreationError(result.message)

        self._cache_id = cache_id(self._name)
        self.affinity = {
            'version': (0, 0),
        }

    def get_protocol_version(self) -> Optional[Tuple]:
        """
        Returns the tuple of major, minor, and revision numbers of the used
        thin protocol version, or None, if no connection to the GridGain
        cluster was not yet established.

        This method is not a part of the public API. Unless you wish to
        extend the `pygridgain` capabilities (with additional testing,
        logging, examining connections, et c.) you probably should not use it.
        """
        return self.client.protocol_version

    @property
    def settings(self) -> Optional[dict]:
        """
        Lazy Cache settings. See the :ref:`example <sql_cache_read>`
        of reading this property.

        All cache properties are documented here: :ref:`cache_props`.

        :return: dict of cache properties and their values.
        """
        if self._settings is None:
            config_result = cache_get_configuration(
                self.get_best_node(),
                self._cache_id
            )
            if config_result.status == 0:
                self._settings = config_result.value
            else:
                raise CacheError(config_result.message)

        return self._settings

    @property
    def name(self) -> str:
        """
        Lazy cache name.

        :return: cache name string.
        """
        if self._name is None:
            self._name = self.settings[prop_codes.PROP_NAME]

        return self._name

    @property
    def client(self) -> 'Client':
        """
        GridGain :class:`~pygridgain.client.Client` object.

        :return: Client object, through which the cache is accessed.
        """
        return self._client

    @property
    def cache_id(self) -> int:
        """
        Cache ID.

        :return: integer value of the cache ID.
        """
        return self._cache_id

    def _process_binary(self, value: Any) -> Any:
        """
        Detects and recursively unwraps Binary Object.

        :param value: anything that could be a Binary Object,
        :return: the result of the Binary Object unwrapping with all other data
         left intact.
        """
        if is_wrapped(value):
            return unwrap_binary(self._client, value)
        return value

    @status_to_exception(CacheError)
    def destroy(self):
        """
        Destroys cache with a given name.
        """
        return cache_destroy(self.get_best_node(), self._cache_id)

    @status_to_exception(CacheError)
    def _get_affinity(self, conn: 'Connection') -> Dict:
        """
        Queries server for affinity mappings. Retries in case
        of an intermittent error (most probably “Getting affinity for topology
        version earlier than affinity is calculated”).

        :param conn: connection to Igneite server,
        :return: OP_CACHE_PARTITIONS operation result value.
        """
        for _ in range(AFFINITY_RETRIES or 1):
            result = cache_get_node_partitions(conn, self._cache_id)
            if result.status == 0 and result.value['partition_mapping']:
                break
            time.sleep(AFFINITY_DELAY)

        return result

    @select_version
    def get_best_node(
        self, key: Any = None, key_hint: 'GridGainDataType' = None,
    ) -> 'Connection':
        """
        Returns the node from the list of the nodes, opened by client, that
        most probably contains the needed key-value pair. See IEP-23.

        This method is not a part of the public API. Unless you wish to
        extend the `pygridgain` capabilities (with additional testing,
        logging, examining connections, et c.) you probably should not use it.

        :param key: (optional) pythonic key,
        :param key_hint: (optional) GridGain data type, for which the given
         key should be converted,
        :return: GridGain connection object.
        """
        conn = self._client.random_node

        if self.client.affinity_aware and key is not None:
            if key_hint is None:
                key_hint = AnyDataObject.map_python_type(key)

            if self.affinity['version'] < self._client.affinity_version:
                # update partition mapping
                while True:
                    try:
                        self.affinity = self._get_affinity(conn)
                        break
                    except connection_errors:
                        # retry if connection failed
                        pass
                    except CacheError:
                        # server did not create mapping in time
                        return conn

                # flatten it a bit
                try:
                    self.affinity.update(self.affinity['partition_mapping'][0])
                except IndexError:
                    return conn
                del self.affinity['partition_mapping']

                # calculate the number of partitions
                parts = sum(
                    [len(p) for _, p in self.affinity['node_mapping'].items()]
                )
                self.affinity['number_of_partitions'] = parts
            else:
                # get number of partitions
                parts = self.affinity['number_of_partitions']

            if self.affinity['is_applicable']:
                affinity_key_id = self.affinity['cache_config'].get(
                    key_hint.type_id,
                    None
                )
                if affinity_key_id and isinstance(key, GenericObjectMeta):
                    key, key_hint = get_field_by_id(key, affinity_key_id)

            # calculate partition for key or affinity key
            # (algorithm is taken from `RendezvousAffinityFunction.java`)
            base_value = key_hint.hashcode(key, self._client)
            mask = parts - 1

            if parts & mask == 0:
                part = (base_value ^ (unsigned(base_value) >> 16)) & mask
            else:
                part = abs(base_value // parts)

            assert 0 <= part < parts, 'Partition calculation has failed'

            # search for connection
            try:
                node_uuid = next(
                    u for u, p
                    in self.affinity['node_mapping'].items()
                    if part in p
                )
                best_conn = next(
                    n for n in conn.client._nodes if n.uuid == node_uuid
                )
                if best_conn.alive:
                    conn = best_conn
            except (StopIteration, KeyError):
                pass

        return conn

    def get_best_node_130(self, *args, **kwargs):
        return self.client.random_node

    get_best_node_120 = get_best_node_130

    @status_to_exception(CacheError)
    def get(self, key, key_hint: object = None) -> Any:
        """
        Retrieves a value from cache by key.

        :param key: key for the cache entry. Can be of any supported type,
        :param key_hint: (optional) GridGain data type, for which the given
         key should be converted,
        :return: value retrieved.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        result = cache_get(
            self.get_best_node(key, key_hint),
            self._cache_id,
            key,
            key_hint=key_hint
        )
        result.value = self._process_binary(result.value)
        return result

    @status_to_exception(CacheError)
    def put(
        self, key, value, key_hint: object = None, value_hint: object = None
    ):
        """
        Puts a value with a given key to cache (overwriting existing value
        if any).

        :param key: key for the cache entry. Can be of any supported type,
        :param value: value for the key,
        :param key_hint: (optional) GridGain data type, for which the given
         key should be converted,
        :param value_hint: (optional) GridGain data type, for which the given
         value should be converted.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        return cache_put(
            self.get_best_node(key, key_hint),
            self._cache_id, key, value,
            key_hint=key_hint, value_hint=value_hint
        )

    @status_to_exception(CacheError)
    def get_all(self, keys: list) -> list:
        """
        Retrieves multiple key-value pairs from cache.

        :param keys: list of keys or tuples of (key, key_hint),
        :return: a dict of key-value pairs.
        """
        result = cache_get_all(self.get_best_node(), self._cache_id, keys)
        if result.value:
            for key, value in result.value.items():
                result.value[key] = self._process_binary(value)
        return result

    @status_to_exception(CacheError)
    def put_all(self, pairs: dict):
        """
        Puts multiple key-value pairs to cache (overwriting existing
        associations if any).

        :param pairs: dictionary type parameters, contains key-value pairs
         to save. Each key or value can be an item of representable
         Python type or a tuple of (item, hint),
        """
        return cache_put_all(
            self.get_best_node(), self._cache_id, pairs
        )

    @status_to_exception(CacheError)
    def replace(
        self, key, value, key_hint: object = None, value_hint: object = None
    ):
        """
        Puts a value with a given key to cache only if the key already exist.

        :param key: key for the cache entry. Can be of any supported type,
        :param value: value for the key,
        :param key_hint: (optional) GridGain data type, for which the given
         key should be converted,
        :param value_hint: (optional) GridGain data type, for which the given
         value should be converted.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        result = cache_replace(
            self.get_best_node(key, key_hint),
            self._cache_id, key, value,
            key_hint=key_hint, value_hint=value_hint
        )
        result.value = self._process_binary(result.value)
        return result

    @status_to_exception(CacheError)
    def clear(self, keys: Optional[list] = None):
        """
        Clears the cache without notifying listeners or cache writers.

        :param keys: (optional) list of cache keys or (key, key type
         hint) tuples to clear (default: clear all).
        """
        conn = self.get_best_node()
        if keys:
            return cache_clear_keys(conn, self._cache_id, keys)
        else:
            return cache_clear(conn, self._cache_id)

    @status_to_exception(CacheError)
    def clear_key(self, key, key_hint: object = None):
        """
        Clears the cache key without notifying listeners or cache writers.

        :param key: key for the cache entry,
        :param key_hint: (optional) GridGain data type, for which the given
         key should be converted,
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        return cache_clear_key(
            self.get_best_node(key, key_hint),
            self._cache_id,
            key,
            key_hint=key_hint
        )

    @status_to_exception(CacheError)
    def contains_key(self, key, key_hint=None) -> bool:
        """
        Returns a value indicating whether given key is present in cache.

        :param key: key for the cache entry. Can be of any supported type,
        :param key_hint: (optional) GridGain data type, for which the given
         key should be converted,
        :return: boolean `True` when key is present, `False` otherwise.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        return cache_contains_key(
            self.get_best_node(key, key_hint),
            self._cache_id,
            key,
            key_hint=key_hint
        )

    @status_to_exception(CacheError)
    def contains_keys(self, keys: Iterable) -> bool:
        """
        Returns a value indicating whether all given keys are present in cache.

        :param keys: a list of keys or (key, type hint) tuples,
        :return: boolean `True` when all keys are present, `False` otherwise.
        """
        return cache_contains_keys(self._client, self._cache_id, keys)

    @status_to_exception(CacheError)
    def get_and_put(self, key, value, key_hint=None, value_hint=None) -> Any:
        """
        Puts a value with a given key to cache, and returns the previous value
        for that key, or null value if there was not such key.

        :param key: key for the cache entry. Can be of any supported type,
        :param value: value for the key,
        :param key_hint: (optional) GridGain data type, for which the given
         key should be converted,
        :param value_hint: (optional) GridGain data type, for which the given
         value should be converted.
        :return: old value or None.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        result = cache_get_and_put(
            self.get_best_node(key, key_hint),
            self._cache_id,
            key, value,
            key_hint, value_hint
        )
        result.value = self._process_binary(result.value)
        return result

    @status_to_exception(CacheError)
    def get_and_put_if_absent(
        self, key, value, key_hint=None, value_hint=None
    ):
        """
        Puts a value with a given key to cache only if the key does not
        already exist.

        :param key: key for the cache entry. Can be of any supported type,
        :param value: value for the key,
        :param key_hint: (optional) GridGain data type, for which the given
         key should be converted,
        :param value_hint: (optional) GridGain data type, for which the given
         value should be converted,
        :return: old value or None.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        result = cache_get_and_put_if_absent(
            self.get_best_node(key, key_hint),
            self._cache_id,
            key, value,
            key_hint, value_hint
        )
        result.value = self._process_binary(result.value)
        return result

    @status_to_exception(CacheError)
    def put_if_absent(self, key, value, key_hint=None, value_hint=None):
        """
        Puts a value with a given key to cache only if the key does not
        already exist.

        :param key: key for the cache entry. Can be of any supported type,
        :param value: value for the key,
        :param key_hint: (optional) GridGain data type, for which the given
         key should be converted,
        :param value_hint: (optional) GridGain data type, for which the given
         value should be converted.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        return cache_put_if_absent(
            self.get_best_node(key, key_hint),
            self._cache_id,
            key, value,
            key_hint, value_hint
        )

    @status_to_exception(CacheError)
    def get_and_remove(self, key, key_hint=None) -> Any:
        """
        Removes the cache entry with specified key, returning the value.

        :param key: key for the cache entry. Can be of any supported type,
        :param key_hint: (optional) GridGain data type, for which the given
         key should be converted,
        :return: old value or None.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        result = cache_get_and_remove(
            self.get_best_node(key, key_hint),
            self._cache_id,
            key,
            key_hint
        )
        result.value = self._process_binary(result.value)
        return result

    @status_to_exception(CacheError)
    def get_and_replace(
        self, key, value, key_hint=None, value_hint=None
    ) -> Any:
        """
        Puts a value with a given key to cache, returning previous value
        for that key, if and only if there is a value currently mapped
        for that key.

        :param key: key for the cache entry. Can be of any supported type,
        :param value: value for the key,
        :param key_hint: (optional) GridGain data type, for which the given
         key should be converted,
        :param value_hint: (optional) GridGain data type, for which the given
         value should be converted.
        :return: old value or None.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        result = cache_get_and_replace(
            self.get_best_node(key, key_hint),
            self._cache_id,
            key, value,
            key_hint, value_hint
        )
        result.value = self._process_binary(result.value)
        return result

    @status_to_exception(CacheError)
    def remove_key(self, key, key_hint=None):
        """
        Clears the cache key without notifying listeners or cache writers.

        :param key: key for the cache entry,
        :param key_hint: (optional) GridGain data type, for which the given
         key should be converted,
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        return cache_remove_key(
            self.get_best_node(key, key_hint), self._cache_id, key, key_hint
        )

    @status_to_exception(CacheError)
    def remove_keys(self, keys: list):
        """
        Removes cache entries by given list of keys, notifying listeners
        and cache writers.

        :param keys: list of keys or tuples of (key, key_hint) to remove.
        """
        return cache_remove_keys(
            self.get_best_node(), self._cache_id, keys
        )

    @status_to_exception(CacheError)
    def remove_all(self):
        """
        Removes all cache entries, notifying listeners and cache writers.
        """
        return cache_remove_all(self.get_best_node(), self._cache_id)

    @status_to_exception(CacheError)
    def remove_if_equals(self, key, sample, key_hint=None, sample_hint=None):
        """
        Removes an entry with a given key if provided value is equal to
        actual value, notifying listeners and cache writers.

        :param key:  key for the cache entry,
        :param sample: a sample to compare the stored value with,
        :param key_hint: (optional) GridGain data type, for which the given
         key should be converted,
        :param sample_hint: (optional) GridGain data type, for whic
         the given sample should be converted.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        return cache_remove_if_equals(
            self.get_best_node(key, key_hint),
            self._cache_id,
            key, sample,
            key_hint, sample_hint
        )

    @status_to_exception(CacheError)
    def replace_if_equals(
        self, key, sample, value,
        key_hint=None, sample_hint=None, value_hint=None
    ) -> Any:
        """
        Puts a value with a given key to cache only if the key already exists
        and value equals provided sample.

        :param key:  key for the cache entry,
        :param sample: a sample to compare the stored value with,
        :param value: new value for the given key,
        :param key_hint: (optional) GridGain data type, for which the given
         key should be converted,
        :param sample_hint: (optional) GridGain data type, for which
         the given sample should be converted
        :param value_hint: (optional) GridGain data type, for which the given
         value should be converted,
        :return: boolean `True` when key is present, `False` otherwise.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        result = cache_replace_if_equals(
            self.get_best_node(key, key_hint),
            self._cache_id,
            key, sample, value,
            key_hint, sample_hint, value_hint
        )
        result.value = self._process_binary(result.value)
        return result

    @status_to_exception(CacheError)
    def get_size(self, peek_modes=0):
        """
        Gets the number of entries in cache.

        :param peek_modes: (optional) limit count to near cache partition
         (PeekModes.NEAR), primary cache (PeekModes.PRIMARY), or backup cache
         (PeekModes.BACKUP). Defaults to all cache partitions (PeekModes.ALL),
        :return: integer number of cache entries.
        """
        return cache_get_size(
            self.get_best_node(), self._cache_id, peek_modes
        )

    def scan(
        self, page_size: int = 1, partitions: int = -1, local: bool = False
    ):
        """
        Returns all key-value pairs from the cache, similar to `get_all`, but
        with internal pagination, which is slower, but safer.

        :param page_size: (optional) page size. Default size is 1 (slowest
         and safest),
        :param partitions: (optional) number of partitions to query
         (negative to query entire cache),
        :param local: (optional) pass True if this query should be executed
         on local node only. Defaults to False,
        :return: generator with key-value pairs.
        """
        node = self.get_best_node()

        result = scan(
            node,
            self._cache_id,
            page_size,
            partitions,
            local
        )
        if result.status != 0:
            raise CacheError(result.message)

        cursor = result.value['cursor']
        for k, v in result.value['data'].items():
            k = self._process_binary(k)
            v = self._process_binary(v)
            yield k, v

        while result.value['more']:
            result = scan_cursor_get_page(node, cursor)
            if result.status != 0:
                raise CacheError(result.message)

            for k, v in result.value['data'].items():
                k = self._process_binary(k)
                v = self._process_binary(v)
                yield k, v

    def select_row(
        self, query_str: str, page_size: int = 1,
        query_args: Optional[list] = None, distributed_joins: bool = False,
        replicated_only: bool = False, local: bool = False, timeout: int = 0
    ):
        """
        Executes a simplified SQL SELECT query over data stored in the cache.
        The query returns the whole record (key and value).

        :param query_str: SQL query string,
        :param page_size: (optional) cursor page size. Default is 1, which
         means that client makes one server call per row,
        :param query_args: (optional) query arguments,
        :param distributed_joins: (optional) distributed joins. Defaults
         to False,
        :param replicated_only: (optional) whether query contains only
         replicated tables or not. Defaults to False,
        :param local: (optional) pass True if this query should be executed
         on local node only. Defaults to False,
        :param timeout: (optional) non-negative timeout value in ms. Zero
         disables timeout (default),
        :return: generator with key-value pairs.
        """
        node = self.get_best_node()

        def generate_result(value):
            cursor = value['cursor']
            more = value['more']
            for k, v in value['data'].items():
                k = self._process_binary(k)
                v = self._process_binary(v)
                yield k, v

            while more:
                inner_result = sql_cursor_get_page(node, cursor)
                if result.status != 0:
                    raise SQLError(result.message)
                more = inner_result.value['more']
                for k, v in inner_result.value['data'].items():
                    k = self._process_binary(k)
                    v = self._process_binary(v)
                    yield k, v

        type_name = self.settings[
            prop_codes.PROP_QUERY_ENTITIES
        ][0]['value_type_name']
        if not type_name:
            raise SQLError('Value type is unknown')
        result = sql(
            node,
            self._cache_id,
            type_name,
            query_str,
            page_size,
            query_args,
            distributed_joins,
            replicated_only,
            local,
            timeout
        )
        if result.status != 0:
            raise SQLError(result.message)

        return generate_result(result.value)
