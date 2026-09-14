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
from typing import Any, Iterable, Optional, Union

from .api.tx_api import get_tx_connection
from .datatypes import ExpiryPolicy
from .datatypes.internal import AnyDataObject
from .exceptions import CacheCreationError, CacheError, ParameterError
from .utils import status_to_exception
from .api.cache_config import (
    cache_create_async, cache_get_or_create_async, cache_destroy_async, cache_get_configuration_async,
    cache_create_with_config_async, cache_get_or_create_with_config_async
)
from .api.key_value import (
    cache_get_async, cache_contains_key_async, cache_clear_key_async, cache_clear_keys_async, cache_clear_async,
    cache_replace_async, cache_put_all_async, cache_get_all_async, cache_put_async, cache_contains_keys_async,
    cache_get_and_put_async, cache_get_and_put_if_absent_async, cache_put_if_absent_async, cache_get_and_remove_async,
    cache_get_and_replace_async, cache_remove_key_async, cache_remove_keys_async, cache_remove_all_async,
    cache_remove_if_equals_async, cache_replace_if_equals_async, cache_get_size_async,
)
from .cursors import AioScanCursor
from .cache import __parse_settings, BaseCache


async def get_cache(client: 'AioClient', settings: Union[str, dict]) -> 'AioCache':
    name, settings = __parse_settings(settings)
    if settings:
        raise ParameterError('Only cache name allowed as a parameter')

    return AioCache(client, name)


async def create_cache(client: 'AioClient', settings: Union[str, dict]) -> 'AioCache':
    name, settings = __parse_settings(settings)

    conn = await client.random_node()
    if settings:
        result = await cache_create_with_config_async(conn, settings)
    else:
        result = await cache_create_async(conn, name)

    if result.status != 0:
        raise CacheCreationError(result.message)

    return AioCache(client, name)


async def get_or_create_cache(client: 'AioClient', settings: Union[str, dict]) -> 'AioCache':
    name, settings = __parse_settings(settings)

    conn = await client.random_node()
    if settings:
        result = await cache_get_or_create_with_config_async(conn, settings)
    else:
        result = await cache_get_or_create_async(conn, name)

    if result.status != 0:
        raise CacheCreationError(result.message)

    return AioCache(client, name)


class AioCache(BaseCache):
    """
    Ignite cache abstraction. Users should never use this class directly,
    but construct its instances with
    :py:meth:`~pyignite.aio_client.AioClient.create_cache`,
    :py:meth:`~pyignite.aio_client.AioClient.get_or_create_cache` or
    :py:meth:`~pyignite.aio_client.AioClient.get_cache` methods instead. See
    :ref:`this example <create_cache>` on how to do it.
    """
    def __init__(self, client: 'AioClient', name: str, expiry_policy: ExpiryPolicy = None):
        """
        Initialize async cache object. For internal use.

        :param client: Async Ignite client,
        :param name: Cache name.
        """
        super().__init__(client, name, expiry_policy)

    async def _get_best_node(self, key=None, key_hint=None):
        tx_conn = get_tx_connection()
        if tx_conn:
            return tx_conn
        return await self.client.get_best_node(self, key, key_hint)

    async def settings(self, timeout: Union[int, float] = 0) -> Optional[dict]:
        """
        Lazy Cache settings. See the :ref:`example <sql_cache_read>`
        of reading this property.

        All cache properties are documented here: :ref:`cache_props`.

        :param timeout: (optional) request timeout.
        :return: dict of cache properties and their values.
        """
        if self._settings is None:
            conn = await self._get_best_node()

            config_result_coro = cache_get_configuration_async(conn, self.cache_info)
            if timeout:
                config_result = await asyncio.wait_for(config_result_coro, timeout)
            else:
                config_result = await config_result_coro

            if config_result.status == 0:
                self._settings = config_result.value
            else:
                raise CacheError(config_result.message)

        return self._settings

    @status_to_exception(CacheError)
    async def destroy(self, timeout: Union[int, float] = 0):
        """
        Destroys cache with a given name.

        :param timeout: (optional) request timeout.
        """
        conn = await self._get_best_node()
        return await cache_destroy_async(conn, self.cache_id)

    @status_to_exception(CacheError)
    async def get(self, key, key_hint: object = None, timeout: Union[int, float] = 0) -> Any:
        """
        Retrieves a value from cache by key.

        :param key: key for the cache entry. Can be of any supported type,
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        :param timeout: (optional) request timeout.
        :return: value retrieved.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        conn = await self._get_best_node(key, key_hint)
        result = await cache_get_async(conn, self.cache_info, key, key_hint=key_hint)
        result.value = await self.client.unwrap_binary(result.value)
        return result

    @status_to_exception(CacheError)
    async def put(self, key, value, key_hint: object = None, value_hint: object = None, timeout: Union[int, float] = 0):
        """
        Puts a value with a given key to cache (overwriting existing value
        if any).

        :param key: key for the cache entry. Can be of any supported type,
        :param value: value for the key,
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        :param value_hint: (optional) Ignite data type, for which the given
         value should be converted,
        :param timeout: (optional) request timeout.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        conn = await self._get_best_node(key, key_hint)
        return await cache_put_async(conn, self.cache_info, key, value, key_hint=key_hint, value_hint=value_hint)

    @status_to_exception(CacheError)
    async def get_all(self, keys: list, timeout: Union[int, float] = 0) -> dict:
        """
        Retrieves multiple key-value pairs from cache.

        :param keys: list of keys or tuples of (key, key_hint),
        :param timeout: (optional) request timeout,
        :return: a dict of key-value pairs.
        """
        conn = await self._get_best_node()
        result = await cache_get_all_async(conn, self.cache_info, keys)
        if result.value:
            keys = list(result.value.keys())
            values = await asyncio.gather(*[self.client.unwrap_binary(value) for value in result.value.values()])

            for i, key in enumerate(keys):
                result.value[key] = values[i]
        return result

    @status_to_exception(CacheError)
    async def put_all(self, pairs: dict, timeout: Union[int, float] = 0):
        """
        Puts multiple key-value pairs to cache (overwriting existing
        associations if any).

        :param pairs: dictionary type parameters, contains key-value pairs
         to save. Each key or value can be an item of representable
         Python type or a tuple of (item, hint),
        :param timeout: (optional) request timeout.
        """
        conn = await self._get_best_node()
        return await cache_put_all_async(conn, self.cache_info, pairs)

    @status_to_exception(CacheError)
    async def replace(self, key, value, key_hint: object = None, value_hint: object = None,
                      timeout: Union[int, float] = 0):
        """
        Puts a value with a given key to cache only if the key already exist.

        :param key: key for the cache entry. Can be of any supported type,
        :param value: value for the key,
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        :param value_hint: (optional) Ignite data type, for which the given
         value should be converted,
        :param timeout: (optional) request timeout.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        conn = await self._get_best_node(key, key_hint)
        result = await cache_replace_async(conn, self.cache_info, key, value, key_hint=key_hint, value_hint=value_hint)
        result.value = await self.client.unwrap_binary(result.value)
        return result

    @status_to_exception(CacheError)
    async def clear(self, keys: Optional[list] = None, timeout: Union[int, float] = 0):
        """
        Clears the cache without notifying listeners or cache writers.

        :param keys: (optional) list of cache keys or (key, key type
         hint) tuples to clear (default: clear all).
        :param timeout: (optional) request timeout.
        """
        conn = await self._get_best_node()
        if keys:
            return await cache_clear_keys_async(conn, self.cache_info, keys)
        else:
            return await cache_clear_async(conn, self.cache_info)

    @status_to_exception(CacheError)
    async def clear_key(self, key, key_hint: object = None, timeout: Union[int, float] = 0):
        """
        Clears the cache key without notifying listeners or cache writers.

        :param key: key for the cache entry,
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        :param timeout: (optional) request timeout.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        conn = await self._get_best_node(key, key_hint)
        return await cache_clear_key_async(conn, self.cache_info, key, key_hint=key_hint)

    @status_to_exception(CacheError)
    async def clear_keys(self, keys: Iterable, timeout: Union[int, float] = 0):
        """
        Clears the cache key without notifying listeners or cache writers.

        :param keys: a list of keys or (key, type hint) tuples
        :param timeout: (optional) request timeout.
        """
        conn = await self._get_best_node()
        return await cache_clear_keys_async(conn, self.cache_info, keys)

    @status_to_exception(CacheError)
    async def contains_key(self, key, key_hint=None, timeout: Union[int, float] = 0) -> bool:
        """
        Returns a value indicating whether given key is present in cache.

        :param key: key for the cache entry. Can be of any supported type,
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        :param timeout: (optional) request timeout,
        :return: boolean `True` when key is present, `False` otherwise.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        conn = await self._get_best_node(key, key_hint)
        return await cache_contains_key_async(conn, self.cache_info, key, key_hint=key_hint)

    @status_to_exception(CacheError)
    async def contains_keys(self, keys: Iterable, timeout: Union[int, float] = 0) -> bool:
        """
        Returns a value indicating whether all given keys are present in cache.

        :param keys: a list of keys or (key, type hint) tuples,
        :param timeout: (optional) request timeout,
        :return: boolean `True` when all keys are present, `False` otherwise.
        """
        conn = await self._get_best_node()
        return await cache_contains_keys_async(conn, self.cache_info, keys)

    @status_to_exception(CacheError)
    async def get_and_put(self, key, value, key_hint=None, value_hint=None, timeout: Union[int, float] = 0) -> Any:
        """
        Puts a value with a given key to cache, and returns the previous value
        for that key, or null value if there was not such key.

        :param key: key for the cache entry. Can be of any supported type,
        :param value: value for the key,
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        :param value_hint: (optional) Ignite data type, for which the given
         value should be converted,
        :param timeout: (optional) request timeout,
        :return: old value or None.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        conn = await self._get_best_node(key, key_hint)
        result = await cache_get_and_put_async(conn, self.cache_info, key, value, key_hint, value_hint)

        result.value = await self.client.unwrap_binary(result.value)
        return result

    @status_to_exception(CacheError)
    async def get_and_put_if_absent(self, key, value, key_hint=None, value_hint=None, timeout: Union[int, float] = 0):
        """
        Puts a value with a given key to cache only if the key does not
        already exist.

        :param key: key for the cache entry. Can be of any supported type,
        :param value: value for the key,
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        :param value_hint: (optional) Ignite data type, for which the given
         value should be converted,
        :param timeout: (optional) request timeout,
        :return: old value or None.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        conn = await self._get_best_node(key, key_hint)
        result = await cache_get_and_put_if_absent_async(conn, self.cache_info, key, value, key_hint, value_hint)
        result.value = await self.client.unwrap_binary(result.value)
        return result

    @status_to_exception(CacheError)
    async def put_if_absent(self, key, value, key_hint=None, value_hint=None, timeout: Union[int, float] = 0):
        """
        Puts a value with a given key to cache only if the key does not
        already exist.

        :param key: key for the cache entry. Can be of any supported type,
        :param value: value for the key,
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        :param value_hint: (optional) Ignite data type, for which the given
         value should be converted,
        :param timeout: (optional) request timeout,
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        conn = await self._get_best_node(key, key_hint)
        return await cache_put_if_absent_async(conn, self.cache_info, key, value, key_hint, value_hint)

    @status_to_exception(CacheError)
    async def get_and_remove(self, key, key_hint=None, timeout: Union[int, float] = 0) -> Any:
        """
        Removes the cache entry with specified key, returning the value.

        :param key: key for the cache entry. Can be of any supported type,
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        :param timeout: (optional) request timeout,
        :return: old value or None.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        conn = await self._get_best_node(key, key_hint)
        result = await cache_get_and_remove_async(conn, self.cache_info, key, key_hint)
        result.value = await self.client.unwrap_binary(result.value)
        return result

    @status_to_exception(CacheError)
    async def get_and_replace(self, key, value, key_hint=None, value_hint=None, timeout: Union[int, float] = 0) -> Any:
        """
        Puts a value with a given key to cache, returning previous value
        for that key, if and only if there is a value currently mapped
        for that key.

        :param key: key for the cache entry. Can be of any supported type,
        :param value: value for the key,
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        :param value_hint: (optional) Ignite data type, for which the given
         value should be converted,
        :param timeout: (optional) request timeout,
        :return: old value or None.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        conn = await self._get_best_node(key, key_hint)
        result = await cache_get_and_replace_async(conn, self.cache_info, key, value, key_hint, value_hint)
        result.value = await self.client.unwrap_binary(result.value)
        return result

    @status_to_exception(CacheError)
    async def remove_key(self, key, key_hint=None, timeout: Union[int, float] = 0):
        """
        Clears the cache key without notifying listeners or cache writers.

        :param key: key for the cache entry,
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        :param timeout: (optional) request timeout.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        conn = await self._get_best_node(key, key_hint)
        return await cache_remove_key_async(conn, self.cache_info, key, key_hint)

    @status_to_exception(CacheError)
    async def remove_keys(self, keys: list, timeout: Union[int, float] = 0):
        """
        Removes cache entries by given list of keys, notifying listeners
        and cache writers.

        :param keys: list of keys or tuples of (key, key_hint) to remove,
        :param timeout: (optional) request timeout.
        """
        conn = await self._get_best_node()
        return await cache_remove_keys_async(conn, self.cache_info, keys)

    @status_to_exception(CacheError)
    async def remove_all(self, timeout: Union[int, float] = 0):
        """
        Removes all cache entries, notifying listeners and cache writers.

        :param timeout: (optional) request timeout.
        """
        conn = await self._get_best_node()
        return await cache_remove_all_async(conn, self.cache_info)

    @status_to_exception(CacheError)
    async def remove_if_equals(self, key, sample, key_hint=None, sample_hint=None, timeout: Union[int, float] = 0):
        """
        Removes an entry with a given key if provided value is equal to
        actual value, notifying listeners and cache writers.

        :param key:  key for the cache entry,
        :param sample: a sample to compare the stored value with,
        :param key_hint: (optional) Ignite data type, for which the given key
         should be converted,
        :param sample_hint: (optional) Ignite data type, for whic
         the given sample should be converted,
        :param timeout: (optional) request timeout.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        conn = await self._get_best_node(key, key_hint)
        return await cache_remove_if_equals_async(conn, self.cache_info, key, sample, key_hint, sample_hint)

    @status_to_exception(CacheError)
    async def replace_if_equals(self, key, sample, value, key_hint=None, sample_hint=None, value_hint=None,
                                timeout: Union[int, float] = 0) -> Any:
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
        :param timeout: (optional) request timeout,
        :return: boolean `True` when key is present, `False` otherwise.
        """
        if key_hint is None:
            key_hint = AnyDataObject.map_python_type(key)

        conn = await self._get_best_node(key, key_hint)
        result = await cache_replace_if_equals_async(conn, self.cache_info, key, sample, value, key_hint, sample_hint,
                                                     value_hint)
        result.value = await self.client.unwrap_binary(result.value)
        return result

    @status_to_exception(CacheError)
    async def get_size(self, peek_modes=None, timeout: Union[int, float] = 0):
        """
        Gets the number of entries in cache.

        :param peek_modes: (optional) limit count to near cache partition
         (PeekModes.NEAR), primary cache (PeekModes.PRIMARY), or backup cache
         (PeekModes.BACKUP). Defaults to primary cache partitions (PeekModes.PRIMARY),
        :param timeout: (optional) request timeout,
        :return: integer number of cache entries.
        """
        conn = await self._get_best_node()
        return await cache_get_size_async(conn, self.cache_info, peek_modes)

    def scan(self, page_size: int = 1, partitions: int = -1, local: bool = False,
             timeout: Union[int, float] = 0) -> AioScanCursor:
        """
        Returns all key-value pairs from the cache, similar to `get_all`, but
        with internal pagination, which is slower, but safer.

        :param page_size: (optional) page size. Default size is 1 (slowest
         and safest),
        :param partitions: (optional) number of partitions to query
         (negative to query entire cache),
        :param local: (optional) pass True if this query should be executed
         on local node only. Defaults to False,
        :return: async scan query cursor
        """
        return AioScanCursor(self.client, self.cache_info, page_size, partitions, local)
