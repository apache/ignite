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
import datetime
from typing import Any, Iterable, Optional, Tuple, Union

from .api.tx_api import get_tx_connection
from .datatypes import prop_codes, ExpiryPolicy
from .datatypes.internal import AnyDataObject
from .exceptions import CacheCreationError, CacheError, ParameterError, SQLError, NotSupportedByClusterError
from .queries.cache_info import CacheInfo
from .utils import cache_id, status_to_exception
from .api.cache_config import (
    cache_create, cache_create_with_config, cache_get_or_create, cache_get_or_create_with_config, cache_destroy,
    cache_get_configuration
)
from .api.key_value import (
    cache_get, cache_put, cache_get_all, cache_put_all, cache_replace, cache_clear, cache_clear_key, cache_clear_keys,
    cache_contains_key, cache_contains_keys, cache_get_and_put, cache_get_and_put_if_absent, cache_put_if_absent,
    cache_get_and_remove, cache_get_and_replace, cache_remove_key, cache_remove_keys, cache_remove_all,
    cache_remove_if_equals, cache_replace_if_equals, cache_get_size
)
from .cursors import ScanCursor, SqlCursor

PROP_CODES = set([
    getattr(prop_codes, x)
    for x in dir(prop_codes)
    if x.startswith('PROP_')
])


def get_cache(client: 'Client', settings: Union[str, dict]) -> 'Cache':
    name, settings = __parse_settings(settings)
    if settings:
        raise ParameterError('Only cache name allowed as a parameter')

    return Cache(client, name)


def create_cache(client: 'Client', settings: Union[str, dict]) -> 'Cache':
    name, settings = __parse_settings(settings)

    conn = client.random_node
    if settings:
        result = cache_create_with_config(conn, settings)
    else:
        result = cache_create(conn, name)

    if result.status != 0:
        raise CacheCreationError(result.message)

    return Cache(client, name)


def get_or_create_cache(client: 'Client', settings: Union[str, dict]) -> 'Cache':
    name, settings = __parse_settings(settings)

    conn = client.random_node
    if settings:
        result = cache_get_or_create_with_config(conn, settings)
    else:
        result = cache_get_or_create(conn, name)

    if result.status != 0:
        raise CacheCreationError(result.message)

    return Cache(client, name)


def __parse_settings(settings: Union[str, dict]) -> Tuple[Optional[str], Optional[dict]]:
    if isinstance(settings, str):
        return settings, None
    elif isinstance(settings, dict) and prop_codes.PROP_NAME in settings:
        name = settings[prop_codes.PROP_NAME]
        if len(settings) == 1:
            return name, None

        if not set(settings).issubset(PROP_CODES):
            raise ParameterError('One or more settings was not recognized')

        return name, settings
    else:
        raise ParameterError('You should supply at least cache name')


class BaseCache:
    def __init__(self, client: 'BaseClient', name: str, expiry_policy: ExpiryPolicy = None):
        self._client = client
        self._name = name
        self._settings = None
        self._cache_info = CacheInfo(cache_id=cache_id(self._name),
                                     protocol_context=client.protocol_context,
                                     expiry_policy=expiry_policy)
        self._client.register_cache(self.cache_info.cache_id)

    @property
    def name(self) -> str:
        """
        :return: cache name string.
        """
        return self._name

    @property
    def client(self) -> 'BaseClient':
        """
        :return: Client object, through which the cache is accessed.
        """
        return self._client

    @property
    def cache_info(self) -> CacheInfo:
        """
        Cache meta info.
        """
        return self._cache_info

    @property
    def cache_id(self) -> int:
        """
        Cache ID.

        :return: integer value of the cache ID.
        """
        return self._cache_info.cache_id

    def with_expire_policy(
            self, expiry_policy: Optional[ExpiryPolicy] = None,
            create: Union[int, datetime.timedelta] = ExpiryPolicy.UNCHANGED,
            update: Union[int, datetime.timedelta] = ExpiryPolicy.UNCHANGED,
            access: Union[int, datetime.timedelta] = ExpiryPolicy.UNCHANGED
    ):
        """
        :param expiry_policy: optional :class:`~pyignite.datatypes.expiry_policy.ExpiryPolicy`
         object. If it is set, other params will be ignored,
        :param create: TTL for create in milliseconds or :py:class:`~time.timedelta`,
        :param update: TTL for update in milliseconds or :py:class:`~time.timedelta`,
        :param access: TTL for access in milliseconds or :py:class:`~time.timedelta`,
        :return: cache decorator with expiry policy set.
        """
        if not self.client.protocol_context.is_expiry_policy_supported():
            raise NotSupportedByClusterError("'ExpiryPolicy' API is not supported by the cluster")

        cache_cls = type(self)
        if not expiry_policy:
            expiry_policy = ExpiryPolicy(create=create, update=update, access=access)

        return cache_cls(self.client, self.name, expiry_policy)


class Cache(BaseCache):
    """
    Ignite cache abstraction. Users should never use this class directly,
    but construct its instances with
    :py:meth:`~pyignite.client.Client.create_cache`,
    :py:meth:`~pyignite.client.Client.get_or_create_cache` or
    :py:meth:`~pyignite.client.Client.get_cache` methods instead. See
    :ref:`this example <create_cache>` on how to do it.
    """

    def __init__(self, client: 'Client', name: str, expiry_policy: ExpiryPolicy = None):
        """
        Initialize cache object. For internal use.

        :param client: Ignite client,
        :param name: Cache name.
        """
        super().__init__(client, name, expiry_policy)

    def _get_best_node(self, key=None, key_hint=None):
        tx_conn = get_tx_connection()
        return tx_conn if tx_conn else self.client.get_best_node(self, key, key_hint)

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
                self._get_best_node(),
                self.cache_info
            )
            if config_result.status == 0:
                self._settings = config_result.value
            else:
                raise CacheError(config_result.message)

        return self._settings

    @status_to_exception(CacheError)
    def destroy(self):
        """
        Destroys cache with a given name.
        """
        return cache_destroy(self._get_best_node(), self.cache_id)

    @status_to_exception(CacheError)
    def get(self, key, key_hint: object = None) -> Any:
        """
        Retrieves a value from cache by key.

        :param key: key for the cache entry. Can be of any supported type,
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        :return: value retrieved.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        result = cache_get(
            self._get_best_node(key, key_hint),
            self.cache_info,
            key,
            key_hint=key_hint
        )
        result.value = self.client.unwrap_binary(result.value)
        return result

    @status_to_exception(CacheError)
    def put(self, key, value, key_hint: object = None, value_hint: object = None):
        """
        Puts a value with a given key to cache (overwriting existing value
        if any).

        :param key: key for the cache entry. Can be of any supported type,
        :param value: value for the key,
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        :param value_hint: (optional) Ignite data type, for which the given
         value should be converted.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        return cache_put(
            self._get_best_node(key, key_hint),
            self.cache_info, key, value,
            key_hint=key_hint, value_hint=value_hint
        )

    @status_to_exception(CacheError)
    def get_all(self, keys: list) -> dict:
        """
        Retrieves multiple key-value pairs from cache.

        :param keys: list of keys or tuples of (key, key_hint),
        :return: a dict of key-value pairs.
        """
        result = cache_get_all(self._get_best_node(), self.cache_info, keys)
        if result.value:
            for key, value in result.value.items():
                result.value[key] = self.client.unwrap_binary(value)
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
        return cache_put_all(self._get_best_node(), self.cache_info, pairs)

    @status_to_exception(CacheError)
    def replace(
            self, key, value, key_hint: object = None, value_hint: object = None
    ):
        """
        Puts a value with a given key to cache only if the key already exist.

        :param key: key for the cache entry. Can be of any supported type,
        :param value: value for the key,
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        :param value_hint: (optional) Ignite data type, for which the given
         value should be converted.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        result = cache_replace(
            self._get_best_node(key, key_hint),
            self.cache_info, key, value,
            key_hint=key_hint, value_hint=value_hint
        )
        result.value = self.client.unwrap_binary(result.value)
        return result

    @status_to_exception(CacheError)
    def clear(self, keys: Optional[list] = None):
        """
        Clears the cache without notifying listeners or cache writers.

        :param keys: (optional) list of cache keys or (key, key type
         hint) tuples to clear (default: clear all).
        """
        conn = self._get_best_node()
        if keys:
            return cache_clear_keys(conn, self.cache_info, keys)
        else:
            return cache_clear(conn, self.cache_info)

    @status_to_exception(CacheError)
    def clear_key(self, key, key_hint: object = None):
        """
        Clears the cache key without notifying listeners or cache writers.

        :param key: key for the cache entry,
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        return cache_clear_key(
            self._get_best_node(key, key_hint),
            self.cache_info,
            key,
            key_hint=key_hint
        )

    @status_to_exception(CacheError)
    def clear_keys(self, keys: Iterable):
        """
        Clears the cache key without notifying listeners or cache writers.

        :param keys: a list of keys or (key, type hint) tuples
        """

        return cache_clear_keys(self._get_best_node(), self.cache_info, keys)

    @status_to_exception(CacheError)
    def contains_key(self, key, key_hint=None) -> bool:
        """
        Returns a value indicating whether given key is present in cache.

        :param key: key for the cache entry. Can be of any supported type,
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        :return: boolean `True` when key is present, `False` otherwise.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        return cache_contains_key(
            self._get_best_node(key, key_hint),
            self.cache_info,
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
        return cache_contains_keys(self._get_best_node(), self.cache_info, keys)

    @status_to_exception(CacheError)
    def get_and_put(self, key, value, key_hint=None, value_hint=None) -> Any:
        """
        Puts a value with a given key to cache, and returns the previous value
        for that key, or null value if there was not such key.

        :param key: key for the cache entry. Can be of any supported type,
        :param value: value for the key,
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        :param value_hint: (optional) Ignite data type, for which the given
         value should be converted.
        :return: old value or None.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        result = cache_get_and_put(
            self._get_best_node(key, key_hint),
            self.cache_info,
            key, value,
            key_hint, value_hint
        )
        result.value = self.client.unwrap_binary(result.value)
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
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        :param value_hint: (optional) Ignite data type, for which the given
         value should be converted,
        :return: old value or None.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        result = cache_get_and_put_if_absent(
            self._get_best_node(key, key_hint),
            self.cache_info,
            key, value,
            key_hint, value_hint
        )
        result.value = self.client.unwrap_binary(result.value)
        return result

    @status_to_exception(CacheError)
    def put_if_absent(self, key, value, key_hint=None, value_hint=None):
        """
        Puts a value with a given key to cache only if the key does not
        already exist.

        :param key: key for the cache entry. Can be of any supported type,
        :param value: value for the key,
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        :param value_hint: (optional) Ignite data type, for which the given
         value should be converted.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        return cache_put_if_absent(
            self._get_best_node(key, key_hint),
            self.cache_info,
            key, value,
            key_hint, value_hint
        )

    @status_to_exception(CacheError)
    def get_and_remove(self, key, key_hint=None) -> Any:
        """
        Removes the cache entry with specified key, returning the value.

        :param key: key for the cache entry. Can be of any supported type,
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        :return: old value or None.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        result = cache_get_and_remove(
            self._get_best_node(key, key_hint),
            self.cache_info,
            key,
            key_hint
        )
        result.value = self.client.unwrap_binary(result.value)
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
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        :param value_hint: (optional) Ignite data type, for which the given
         value should be converted.
        :return: old value or None.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        result = cache_get_and_replace(
            self._get_best_node(key, key_hint),
            self.cache_info,
            key, value,
            key_hint, value_hint
        )
        result.value = self.client.unwrap_binary(result.value)
        return result

    @status_to_exception(CacheError)
    def remove_key(self, key, key_hint=None):
        """
        Clears the cache key without notifying listeners or cache writers.

        :param key: key for the cache entry,
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        return cache_remove_key(
            self._get_best_node(key, key_hint), self.cache_info, key, key_hint
        )

    @status_to_exception(CacheError)
    def remove_keys(self, keys: list):
        """
        Removes cache entries by given list of keys, notifying listeners
        and cache writers.

        :param keys: list of keys or tuples of (key, key_hint) to remove.
        """
        return cache_remove_keys(
            self._get_best_node(), self.cache_info, keys
        )

    @status_to_exception(CacheError)
    def remove_all(self):
        """
        Removes all cache entries, notifying listeners and cache writers.
        """
        return cache_remove_all(self._get_best_node(), self.cache_info)

    @status_to_exception(CacheError)
    def remove_if_equals(self, key, sample, key_hint=None, sample_hint=None):
        """
        Removes an entry with a given key if provided value is equal to
        actual value, notifying listeners and cache writers.

        :param key:  key for the cache entry,
        :param sample: a sample to compare the stored value with,
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        :param sample_hint: (optional) Ignite data type, for whic
         the given sample should be converted.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        return cache_remove_if_equals(
            self._get_best_node(key, key_hint),
            self.cache_info,
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
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        :param sample_hint: (optional) Ignite data type, for whic
         the given sample should be converted
        :param value_hint: (optional) Ignite data type, for which the given
         value should be converted,
        :return: boolean `True` when key is present, `False` otherwise.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        result = cache_replace_if_equals(
            self._get_best_node(key, key_hint),
            self.cache_info,
            key, sample, value,
            key_hint, sample_hint, value_hint
        )
        result.value = self.client.unwrap_binary(result.value)
        return result

    @status_to_exception(CacheError)
    def get_size(self, peek_modes=None):
        """
        Gets the number of entries in cache.

        :param peek_modes: (optional) limit count to near cache partition
         (PeekModes.NEAR), primary cache (PeekModes.PRIMARY), or backup cache
         (PeekModes.BACKUP). Defaults to primary cache partitions (PeekModes.PRIMARY),
        :return: integer number of cache entries.
        """
        return cache_get_size(
            self._get_best_node(), self.cache_info, peek_modes
        )

    def scan(self, page_size: int = 1, partitions: int = -1, local: bool = False) -> ScanCursor:
        """
        Returns all key-value pairs from the cache, similar to `get_all`, but
        with internal pagination, which is slower, but safer.

        :param page_size: (optional) page size. Default size is 1 (slowest
         and safest),
        :param partitions: (optional) number of partitions to query
         (negative to query entire cache),
        :param local: (optional) pass True if this query should be executed
         on local node only. Defaults to False,
        :return: Scan query cursor.
        """
        return ScanCursor(self.client, self.cache_info, page_size, partitions, local)

    def select_row(
            self, query_str: str, page_size: int = 1,
            query_args: Optional[list] = None, distributed_joins: bool = False,
            replicated_only: bool = False, local: bool = False, timeout: int = 0
    ) -> SqlCursor:
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
        :return: Sql cursor.
        """
        type_name = self.settings[
            prop_codes.PROP_QUERY_ENTITIES
        ][0]['value_type_name']
        if not type_name:
            raise SQLError('Value type is unknown')

        return SqlCursor(self.client, self.cache_info, type_name, query_str, page_size, query_args,
                         distributed_joins, replicated_only, local, timeout)
