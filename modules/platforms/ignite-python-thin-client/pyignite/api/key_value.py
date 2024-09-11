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

from typing import Any, Iterable, Union

from pyignite.connection import AioConnection, Connection
from pyignite.queries.op_codes import (
    OP_CACHE_PUT, OP_CACHE_GET, OP_CACHE_GET_ALL, OP_CACHE_PUT_ALL, OP_CACHE_CONTAINS_KEY, OP_CACHE_CONTAINS_KEYS,
    OP_CACHE_GET_AND_PUT, OP_CACHE_GET_AND_REPLACE, OP_CACHE_GET_AND_REMOVE, OP_CACHE_PUT_IF_ABSENT,
    OP_CACHE_GET_AND_PUT_IF_ABSENT, OP_CACHE_REPLACE, OP_CACHE_REPLACE_IF_EQUALS, OP_CACHE_CLEAR, OP_CACHE_CLEAR_KEY,
    OP_CACHE_CLEAR_KEYS, OP_CACHE_REMOVE_KEY, OP_CACHE_REMOVE_IF_EQUALS, OP_CACHE_REMOVE_KEYS, OP_CACHE_REMOVE_ALL,
    OP_CACHE_GET_SIZE, OP_CACHE_LOCAL_PEEK
)
from pyignite.datatypes import Map, Bool, Long, AnyDataArray, AnyDataObject, ByteArray
from pyignite.datatypes.base import IgniteDataType
from pyignite.queries import Query, query_perform

from .result import APIResult
from ..queries.cache_info import CacheInfo


def cache_put(connection: 'Connection', cache_info: CacheInfo, key: Any, value: Any,
              key_hint: 'IgniteDataType' = None, value_hint: 'IgniteDataType' = None) -> 'APIResult':
    """
    Puts a value with a given key to cache (overwriting existing value if any).

    :param connection: connection to Ignite server,
    :param cache_info: cache meta info,
    :param key: key for the cache entry. Can be of any supported type,
    :param value: value for the key,
    :param key_hint: (optional) Ignite data type, for which the given key
     should be converted,
    :param value_hint: (optional) Ignite data type, for which the given value
     should be converted.
    :return: API result data object. Contains zero status if a value
     is written, non-zero status and an error description otherwise.
    """
    return __cache_put(connection, cache_info, key, value, key_hint, value_hint)


async def cache_put_async(connection: 'AioConnection', cache_info: CacheInfo, key: Any, value: Any,
                          key_hint: 'IgniteDataType' = None, value_hint: 'IgniteDataType' = None) -> 'APIResult':
    """
    Async version of cache_put
    """
    return await __cache_put(connection, cache_info, key, value, key_hint, value_hint)


def __cache_put(connection, cache_info, key, value, key_hint, value_hint):
    query_struct = Query(
        OP_CACHE_PUT,
        [
            ('cache_info', CacheInfo),
            ('key', key_hint or AnyDataObject),
            ('value', value_hint or AnyDataObject),
        ]
    )
    return query_perform(
        query_struct, connection,
        query_params={
            'cache_info': cache_info,
            'key': key,
            'value': value
        }
    )


def cache_get(connection: 'Connection', cache_info: CacheInfo, key: Any,
              key_hint: 'IgniteDataType' = None) -> 'APIResult':
    """
    Retrieves a value from cache by key.

    :param connection: connection to Ignite server,
    :param cache_info: cache meta info,
    :param key: key for the cache entry. Can be of any supported type,
    :param key_hint: (optional) Ignite data type, for which the given key
     should be converted
    :return: API result data object. Contains zero status and a value
     retrieved on success, non-zero status and an error description on failure.
    """
    return __cache_get(connection, cache_info, key, key_hint)


async def cache_get_async(connection: 'AioConnection', cache_info: CacheInfo, key: Any,
                          key_hint: 'IgniteDataType' = None) -> 'APIResult':
    """
    Async version of cache_get
    """
    return await __cache_get(connection, cache_info, key, key_hint)


def __cache_get(connection, cache_info, key, key_hint):
    query_struct = Query(
        OP_CACHE_GET,
        [
            ('cache_info', CacheInfo),
            ('key', key_hint or AnyDataObject),
        ]
    )
    return query_perform(
        query_struct, connection,
        query_params={
            'cache_info': cache_info,
            'key': key,
        },
        response_config=[
            ('value', AnyDataObject),
        ],
        post_process_fun=__post_process_value_by_key('value')
    )


def cache_get_all(connection: 'Connection', cache_info: CacheInfo, keys: Iterable) -> 'APIResult':
    """
    Retrieves multiple key-value pairs from cache.

    :param connection: connection to Ignite server,
    :param cache_info: cache meta info,
    :param keys: list of keys or tuples of (key, key_hint),
    :return: API result data object. Contains zero status and a dict, made of
     retrieved key-value pairs, non-zero status and an error description
     on failure.
    """
    return __cache_get_all(connection, cache_info, keys)


async def cache_get_all_async(connection: 'AioConnection', cache_info: CacheInfo, keys: Iterable) -> 'APIResult':
    """
    Async version of cache_get_all.
    """
    return await __cache_get_all(connection, cache_info, keys)


def __cache_get_all(connection, cache_info, keys):
    query_struct = Query(
        OP_CACHE_GET_ALL,
        [
            ('cache_info', CacheInfo),
            ('keys', AnyDataArray()),
        ]
    )
    return query_perform(
        query_struct, connection,
        query_params={
            'cache_info': cache_info,
            'keys': keys,
        },
        response_config=[
            ('data', Map),
        ],
        post_process_fun=__post_process_value_by_key('data')
    )


def cache_put_all(connection: 'Connection', cache_info: CacheInfo, pairs: dict) -> 'APIResult':
    """
    Puts multiple key-value pairs to cache (overwriting existing associations
    if any).

    :param connection: connection to Ignite server,
    :param cache_info: cache meta info,
    :param pairs: dictionary type parameters, contains key-value pairs to save.
     Each key or value can be an item of representable Python type or a tuple
     of (item, hint),
    :return: API result data object. Contains zero status if key-value pairs
     are written, non-zero status and an error description otherwise.
    """
    return __cache_put_all(connection, cache_info, pairs)


async def cache_put_all_async(connection: 'AioConnection', cache_info: CacheInfo, pairs: dict) -> 'APIResult':
    """
    Async version of cache_put_all.
    """
    return await __cache_put_all(connection, cache_info, pairs)


def __cache_put_all(connection, cache_info, pairs):
    query_struct = Query(
        OP_CACHE_PUT_ALL,
        [
            ('cache_info', CacheInfo),
            ('data', Map),
        ]
    )
    return query_perform(
        query_struct, connection,
        query_params={
            'cache_info': cache_info,
            'data': pairs,
        },
    )


def cache_contains_key(connection: 'Connection', cache_info: CacheInfo, key: Any,
                       key_hint: 'IgniteDataType' = None) -> 'APIResult':
    """
    Returns a value indicating whether given key is present in cache.

    :param connection: connection to Ignite server,
    :param cache_info: cache meta info,
    :param key: key for the cache entry. Can be of any supported type,
    :param key_hint: (optional) Ignite data type, for which the given key
     should be converted
    :return: API result data object. Contains zero status and a bool value
     retrieved on success: `True` when key is present, `False` otherwise,
     non-zero status and an error description on failure.
    """
    return __cache_contains_key(connection, cache_info, key, key_hint)


async def cache_contains_key_async(connection: 'AioConnection', cache_info: CacheInfo, key: Any,
                                   key_hint: 'IgniteDataType' = None) -> 'APIResult':
    """
    Async version of cache_contains_key.
    """
    return await __cache_contains_key(connection, cache_info, key, key_hint)


def __cache_contains_key(connection, cache_info, key, key_hint):
    query_struct = Query(
        OP_CACHE_CONTAINS_KEY,
        [
            ('cache_info', CacheInfo),
            ('key', key_hint or AnyDataObject),
        ]
    )
    return query_perform(
        query_struct, connection,
        query_params={
            'cache_info': cache_info,
            'key': key,
        },
        response_config=[
            ('value', Bool),
        ],
        post_process_fun=__post_process_value_by_key('value')
    )


def cache_contains_keys(connection: 'Connection', cache_info: CacheInfo, keys: Iterable,
                        ) -> 'APIResult':
    """
    Returns a value indicating whether all given keys are present in cache.

    :param connection: connection to Ignite server,
    :param cache_info: cache meta info,
    :param keys: a list of keys or (key, type hint) tuples,
    :return: API result data object. Contains zero status and a bool value
     retrieved on success: `True` when all keys are present, `False` otherwise,
     non-zero status and an error description on failure.
    """
    return __cache_contains_keys(connection, cache_info, keys)


async def cache_contains_keys_async(connection: 'AioConnection', cache_info: CacheInfo, keys: Iterable) -> 'APIResult':
    """
    Async version of cache_contains_keys.
    """
    return await __cache_contains_keys(connection, cache_info, keys)


def __cache_contains_keys(connection, cache_info, keys):
    query_struct = Query(
        OP_CACHE_CONTAINS_KEYS,
        [
            ('cache_info', CacheInfo),
            ('keys', AnyDataArray()),
        ]
    )
    return query_perform(
        query_struct, connection,
        query_params={
            'cache_info': cache_info,
            'keys': keys,
        },
        response_config=[
            ('value', Bool),
        ],
        post_process_fun=__post_process_value_by_key('value')
    )


def cache_get_and_put(connection: 'Connection', cache_info: CacheInfo, key: Any, value: Any,
                      key_hint: 'IgniteDataType' = None, value_hint: 'IgniteDataType' = None) -> 'APIResult':
    """
    Puts a value with a given key to cache_info, and returns the previous value
    for that key, or null value if there was not such key.

    :param connection: connection to Ignite server,
    :param cache_info: cache meta info,
    :param key: key for the cache entry. Can be of any supported type,
    :param value: value for the key,
    :param key_hint: (optional) Ignite data type, for which the given key
     should be converted,
    :param value_hint: (optional) Ignite data type, for which the given value
     should be converted.
    :return: API result data object. Contains zero status and an old value
     or None if a value is written, non-zero status and an error description
     in case of error.
    """
    return __cache_get_and_put(connection, cache_info, key, value, key_hint, value_hint)


async def cache_get_and_put_async(
        connection: 'AioConnection', cache_info: CacheInfo, key: Any, value: Any,
        key_hint: 'IgniteDataType' = None, value_hint: 'IgniteDataType' = None
) -> 'APIResult':
    """
    Async version of cache_get_and_put.
    """
    return await __cache_get_and_put(connection, cache_info, key, value, key_hint, value_hint)


def __cache_get_and_put(connection, cache_info, key, value, key_hint, value_hint):
    query_struct = Query(
        OP_CACHE_GET_AND_PUT,
        [
            ('cache_info', CacheInfo),
            ('key', key_hint or AnyDataObject),
            ('value', value_hint or AnyDataObject),
        ],
    )
    return query_perform(
        query_struct, connection,
        query_params={
            'cache_info': cache_info,
            'key': key,
            'value': value,
        },
        response_config=[
            ('value', AnyDataObject),
        ],
        post_process_fun=__post_process_value_by_key('value')
    )


def cache_get_and_replace(connection: 'Connection', cache_info: CacheInfo, key: Any, value: Any,
                          key_hint: 'IgniteDataType' = None, value_hint: 'IgniteDataType' = None) -> 'APIResult':
    """
    Puts a value with a given key to cache, returning previous value
    for that key, if and only if there is a value currently mapped
    for that key.

    :param connection: connection to Ignite server,
    :param cache_info: cache meta info,
    :param key: key for the cache entry. Can be of any supported type,
    :param value: value for the key,
    :param key_hint: (optional) Ignite data type, for which the given key
     should be converted,
    :param value_hint: (optional) Ignite data type, for which the given value
     should be converted.
    :return: API result data object. Contains zero status and an old value
     or None on success, non-zero status and an error description otherwise.
    """
    return __cache_get_and_replace(connection, cache_info, key, key_hint, value, value_hint)


async def cache_get_and_replace_async(
        connection: 'AioConnection', cache_info: CacheInfo, key: Any, value: Any,
        key_hint: 'IgniteDataType' = None, value_hint: 'IgniteDataType' = None
) -> 'APIResult':
    """
    Async version of cache_get_and_replace.
    """
    return await __cache_get_and_replace(connection, cache_info, key, key_hint, value, value_hint)


def __cache_get_and_replace(connection, cache_info, key, key_hint, value, value_hint):
    query_struct = Query(
        OP_CACHE_GET_AND_REPLACE, [
            ('cache_info', CacheInfo),
            ('key', key_hint or AnyDataObject),
            ('value', value_hint or AnyDataObject),
        ]
    )
    return query_perform(
        query_struct, connection,
        query_params={
            'cache_info': cache_info,
            'key': key,
            'value': value,
        },
        response_config=[
            ('value', AnyDataObject),
        ],
        post_process_fun=__post_process_value_by_key('value')
    )


def cache_get_and_remove(
        connection: 'Connection', cache_info: CacheInfo, key: Any, key_hint: 'IgniteDataType' = None
) -> 'APIResult':
    """
    Removes the cache entry with specified key, returning the value.

    :param connection: connection to Ignite server,
    :param cache_info: cache meta info,
    :param key: key for the cache entry. Can be of any supported type,
    :param key_hint: (optional) Ignite data type, for which the given key
     should be converted,
    :return: API result data object. Contains zero status and an old value
     or None, non-zero status and an error description otherwise.
    """
    return __cache_get_and_remove(connection, cache_info, key, key_hint)


async def cache_get_and_remove_async(
        connection: 'AioConnection', cache_info: CacheInfo, key: Any, key_hint: 'IgniteDataType' = None
) -> 'APIResult':
    return await __cache_get_and_remove(connection, cache_info, key, key_hint)


def __cache_get_and_remove(connection, cache_info, key, key_hint):
    query_struct = Query(
        OP_CACHE_GET_AND_REMOVE, [
            ('cache_info', CacheInfo),
            ('key', key_hint or AnyDataObject),
        ]
    )
    return query_perform(
        query_struct, connection,
        query_params={
            'cache_info': cache_info,
            'key': key,
        },
        response_config=[
            ('value', AnyDataObject),
        ],
        post_process_fun=__post_process_value_by_key('value')
    )


def cache_put_if_absent(connection: 'Connection', cache_info: CacheInfo, key: Any, value: Any,
                        key_hint: 'IgniteDataType' = None, value_hint: 'IgniteDataType' = None) -> 'APIResult':
    """
    Puts a value with a given key to cache only if the key
    does not already exist.

    :param connection: connection to Ignite server,
    :param cache_info: cache meta info,
    :param key: key for the cache entry. Can be of any supported type,
    :param value: value for the key,
    :param key_hint: (optional) Ignite data type, for which the given key
     should be converted,
    :param value_hint: (optional) Ignite data type, for which the given value
     should be converted.
    :return: API result data object. Contains zero status on success,
     non-zero status and an error description otherwise.
    """
    return __cache_put_if_absent(connection, cache_info, key, value, key_hint, value_hint)


async def cache_put_if_absent_async(
        connection: 'AioConnection', cache_info: CacheInfo, key: Any, value: Any,
        key_hint: 'IgniteDataType' = None, value_hint: 'IgniteDataType' = None
) -> 'APIResult':
    """
    Async version of cache_put_if_absent.
    """
    return await __cache_put_if_absent(connection, cache_info, key, value, key_hint, value_hint)


def __cache_put_if_absent(connection, cache_info, key, value, key_hint, value_hint):
    query_struct = Query(
        OP_CACHE_PUT_IF_ABSENT,
        [
            ('cache_info', CacheInfo),
            ('key', key_hint or AnyDataObject),
            ('value', value_hint or AnyDataObject),
        ]
    )
    return query_perform(
        query_struct, connection,
        query_params={
            'cache_info': cache_info,
            'key': key,
            'value': value,
        },
        response_config=[
            ('success', Bool),
        ],
        post_process_fun=__post_process_value_by_key('success')
    )


def cache_get_and_put_if_absent(connection: 'Connection', cache_info: CacheInfo, key: Any, value: Any,
                                key_hint: 'IgniteDataType' = None, value_hint: 'IgniteDataType' = None) -> 'APIResult':
    """
    Puts a value with a given key to cache only if the key does not
    already exist.

    :param connection: connection to Ignite server,
    :param cache_info: cache meta info,
    :param key: key for the cache entry. Can be of any supported type,
    :param value: value for the key,
    :param key_hint: (optional) Ignite data type, for which the given key
     should be converted,
    :param value_hint: (optional) Ignite data type, for which the given value
     should be converted.
    :return: API result data object. Contains zero status and an old value
     or None on success, non-zero status and an error description otherwise.
    """
    return __cache_get_and_put_if_absent(connection, cache_info, key, value, key_hint, value_hint)


async def cache_get_and_put_if_absent_async(
        connection: 'AioConnection', cache_info: CacheInfo, key: Any, value: Any,
        key_hint: 'IgniteDataType' = None, value_hint: 'IgniteDataType' = None
) -> 'APIResult':
    """
    Async version of cache_get_and_put_if_absent.
    """
    return await __cache_get_and_put_if_absent(connection, cache_info, key, value, key_hint, value_hint)


def __cache_get_and_put_if_absent(connection, cache_info, key, value, key_hint, value_hint):
    query_struct = Query(
        OP_CACHE_GET_AND_PUT_IF_ABSENT,
        [
            ('cache_info', CacheInfo),
            ('key', key_hint or AnyDataObject),
            ('value', value_hint or AnyDataObject),
        ]
    )
    return query_perform(
        query_struct, connection,
        query_params={
            'cache_info': cache_info,
            'key': key,
            'value': value,
        },
        response_config=[
            ('value', AnyDataObject),
        ],
        post_process_fun=__post_process_value_by_key('value')
    )


def cache_replace(connection: 'Connection', cache_info: CacheInfo, key: Any, value: Any,
                  key_hint: 'IgniteDataType' = None, value_hint: 'IgniteDataType' = None) -> 'APIResult':
    """
    Puts a value with a given key to cache only if the key already exist.

    :param connection: connection to Ignite server,
    :param cache_info: cache meta info,
    :param key: key for the cache entry. Can be of any supported type,
    :param value: value for the key,
    :param key_hint: (optional) Ignite data type, for which the given key
     should be converted,
    :param value_hint: (optional) Ignite data type, for which the given value
     should be converted.
    :return: API result data object. Contains zero status and a boolean
     success code, or non-zero status and an error description if something
     has gone wrong.
    """
    return __cache_replace(connection, cache_info, key, value, key_hint, value_hint)


async def cache_replace_async(
        connection: 'AioConnection', cache_info: CacheInfo, key: Any, value: Any,
        key_hint: 'IgniteDataType' = None, value_hint: 'IgniteDataType' = None
) -> 'APIResult':
    """
    Async version of cache_replace.
    """
    return await __cache_replace(connection, cache_info, key, value, key_hint, value_hint)


def __cache_replace(connection, cache_info, key, value, key_hint, value_hint):
    query_struct = Query(
        OP_CACHE_REPLACE,
        [
            ('cache_info', CacheInfo),
            ('key', key_hint or AnyDataObject),
            ('value', value_hint or AnyDataObject),
        ]
    )
    return query_perform(
        query_struct, connection,
        query_params={
            'cache_info': cache_info,
            'key': key,
            'value': value,
        },
        response_config=[
            ('success', Bool),
        ],
        post_process_fun=__post_process_value_by_key('success')
    )


def cache_replace_if_equals(connection: 'Connection', cache_info: CacheInfo, key: Any, sample: Any, value: Any,
                            key_hint: 'IgniteDataType' = None, sample_hint: 'IgniteDataType' = None,
                            value_hint: 'IgniteDataType' = None) -> 'APIResult':
    """
    Puts a value with a given key to cache only if the key already exists
    and value equals provided sample.

    :param connection: connection to Ignite server,
    :param cache_info: cache meta info,
    :param key:  key for the cache entry,
    :param sample: a sample to compare the stored value with,
    :param value: new value for the given key,
    :param key_hint: (optional) Ignite data type, for which the given key
     should be converted,
    :param sample_hint: (optional) Ignite data type, for whic
     the given sample should be converted
    :param value_hint: (optional) Ignite data type, for which the given value
     should be converted,
    :return: API result data object. Contains zero status and a boolean
     success code, or non-zero status and an error description if something
     has gone wrong.
    """
    return __cache_replace_if_equals(connection, cache_info, key, sample, value, key_hint,
                                     sample_hint, value_hint)


async def cache_replace_if_equals_async(
        connection: 'AioConnection', cache_info: CacheInfo, key: Any, sample: Any, value: Any,
        key_hint: 'IgniteDataType' = None, sample_hint: 'IgniteDataType' = None, value_hint: 'IgniteDataType' = None
) -> 'APIResult':
    """
    Async version of cache_replace_if_equals.
    """
    return await __cache_replace_if_equals(connection, cache_info, key, sample, value, key_hint,
                                           sample_hint, value_hint)


def __cache_replace_if_equals(connection, cache_info, key, sample, value, key_hint, sample_hint, value_hint):
    query_struct = Query(
        OP_CACHE_REPLACE_IF_EQUALS,
        [
            ('cache_info', CacheInfo),
            ('key', key_hint or AnyDataObject),
            ('sample', sample_hint or AnyDataObject),
            ('value', value_hint or AnyDataObject),
        ]
    )
    return query_perform(
        query_struct, connection,
        query_params={
            'cache_info': cache_info,
            'key': key,
            'sample': sample,
            'value': value,
        },
        response_config=[
            ('success', Bool),
        ],
        post_process_fun=__post_process_value_by_key('success')
    )


def cache_clear(connection: 'Connection', cache_info: CacheInfo) -> 'APIResult':
    """
    Clears the cache without notifying listeners or cache writers.

    :param connection: connection to Ignite server,
    :param cache_info: cache meta info,
    :return: API result data object. Contains zero status on success,
     non-zero status and an error description otherwise.
    """
    return __cache_clear(connection, cache_info)


async def cache_clear_async(connection: 'AioConnection', cache_info: CacheInfo) -> 'APIResult':
    """
    Async version of cache_clear.
    """
    return await __cache_clear(connection, cache_info)


def __cache_clear(connection, cache_info):
    query_struct = Query(
        OP_CACHE_CLEAR,
        [
            ('cache_info', CacheInfo),
        ]
    )
    return query_perform(
        query_struct, connection,
        query_params={
            'cache_info': cache_info,
        },
    )


def cache_clear_key(
        connection: 'Connection', cache_info: CacheInfo, key: Any, key_hint: 'IgniteDataType' = None
) -> 'APIResult':
    """
    Clears the cache key without notifying listeners or cache writers.

    :param connection: connection to Ignite server,
    :param cache_info: cache meta info,
    :param key:  key for the cache entry,
    :param key_hint: (optional) Ignite data type, for which the given key
     should be converted,
    :return: API result data object. Contains zero status on success,
     non-zero status and an error description otherwise.
    """
    return __cache_clear_key(connection, cache_info, key, key_hint)


async def cache_clear_key_async(
        connection: 'AioConnection', cache_info: CacheInfo, key: Any, key_hint: 'IgniteDataType' = None
) -> 'APIResult':
    """
    Async version of cache_clear_key.
    """
    return await __cache_clear_key(connection, cache_info, key, key_hint)


def __cache_clear_key(connection, cache_info, key, key_hint):
    query_struct = Query(
        OP_CACHE_CLEAR_KEY,
        [
            ('cache_info', CacheInfo),
            ('key', key_hint or AnyDataObject),
        ]
    )
    return query_perform(
        query_struct, connection,
        query_params={
            'cache_info': cache_info,
            'key': key,
        },
    )


def cache_clear_keys(connection: 'Connection', cache_info: CacheInfo, keys: Iterable) -> 'APIResult':
    """
    Clears the cache keys without notifying listeners or cache writers.

    :param connection: connection to Ignite server,
    :param cache_info: cache meta info,
    :param keys: list of keys or tuples of (key, key_hint),
    :return: API result data object. Contains zero status on success,
     non-zero status and an error description otherwise.
    """
    return __cache_clear_keys(connection, cache_info, keys)


async def cache_clear_keys_async(connection: 'AioConnection', cache_info: CacheInfo, keys: Iterable) -> 'APIResult':
    """
    Async version of cache_clear_keys.
    """
    return await __cache_clear_keys(connection, cache_info, keys)


def __cache_clear_keys(connection, cache_info, keys):
    query_struct = Query(
        OP_CACHE_CLEAR_KEYS,
        [
            ('cache_info', CacheInfo),
            ('keys', AnyDataArray()),
        ]
    )
    return query_perform(
        query_struct, connection,
        query_params={
            'cache_info': cache_info,
            'keys': keys,
        },
    )


def cache_remove_key(
        connection: 'Connection', cache_info: CacheInfo, key: Any, key_hint: 'IgniteDataType' = None
) -> 'APIResult':
    """
    Clears the cache key without notifying listeners or cache writers.

    :param connection: connection to Ignite server,
    :param cache_info: cache meta info,
    :param key:  key for the cache entry,
    :param key_hint: (optional) Ignite data type, for which the given key
     should be converted
    :return: API result data object. Contains zero status and a boolean
     success code, or non-zero status and an error description if something
     has gone wrong.
    """
    return __cache_remove_key(connection, cache_info, key, key_hint)


async def cache_remove_key_async(
        connection: 'AioConnection', cache_info: CacheInfo, key: Any, key_hint: 'IgniteDataType' = None
) -> 'APIResult':
    """
    Async version of cache_remove_key.
    """
    return await __cache_remove_key(connection, cache_info, key, key_hint)


def __cache_remove_key(connection, cache_info, key, key_hint):
    query_struct = Query(
        OP_CACHE_REMOVE_KEY,
        [
            ('cache_info', CacheInfo),
            ('key', key_hint or AnyDataObject),
        ]
    )
    return query_perform(
        query_struct, connection,
        query_params={
            'cache_info': cache_info,
            'key': key,
        },
        response_config=[
            ('success', Bool),
        ],
        post_process_fun=__post_process_value_by_key('success')
    )


def cache_remove_if_equals(connection: 'Connection', cache_info: CacheInfo, key: Any, sample: Any,
                           key_hint: 'IgniteDataType' = None, sample_hint: 'IgniteDataType' = None) -> 'APIResult':
    """
    Removes an entry with a given key if provided value is equal to
    actual value, notifying listeners and cache writers.

    :param connection: connection to Ignite server,
    :param cache_info: cache meta info,
    :param key:  key for the cache entry,
    :param sample: a sample to compare the stored value with,
    :param key_hint: (optional) Ignite data type, for which the given key
     should be converted,
    :param sample_hint: (optional) Ignite data type, for whic
     the given sample should be converted,
    :return: API result data object. Contains zero status and a boolean
     success code, or non-zero status and an error description if something
     has gone wrong.
    """
    return __cache_remove_if_equals(connection, cache_info, key, sample, key_hint, sample_hint)


async def cache_remove_if_equals_async(
        connection: 'AioConnection', cache_info: CacheInfo, key: Any, sample: Any,
        key_hint: 'IgniteDataType' = None, sample_hint: 'IgniteDataType' = None
) -> 'APIResult':
    """
    Async version of cache_remove_if_equals.
    """
    return await __cache_remove_if_equals(connection, cache_info, key, sample, key_hint, sample_hint)


def __cache_remove_if_equals(connection, cache_info, key, sample, key_hint, sample_hint):
    query_struct = Query(
        OP_CACHE_REMOVE_IF_EQUALS,
        [
            ('cache_info', CacheInfo),
            ('key', key_hint or AnyDataObject),
            ('sample', sample_hint or AnyDataObject),
        ]
    )
    return query_perform(
        query_struct, connection,
        query_params={
            'cache_info': cache_info,
            'key': key,
            'sample': sample,
        },
        response_config=[
            ('success', Bool),
        ],
        post_process_fun=__post_process_value_by_key('success')
    )


def cache_remove_keys(connection: 'Connection', cache_info: CacheInfo, keys: Iterable) -> 'APIResult':
    """
    Removes entries with given keys, notifying listeners and cache writers.

    :param connection: connection to Ignite server,
    :param cache_info: cache meta info,
    :param keys: list of keys or tuples of (key, key_hint),
    :return: API result data object. Contains zero status on success,
     non-zero status and an error description otherwise.
    """
    return __cache_remove_keys(connection, cache_info, keys)


async def cache_remove_keys_async(connection: 'AioConnection', cache_info: CacheInfo, keys: Iterable) -> 'APIResult':
    """
    Async version of cache_remove_keys.
    """
    return await __cache_remove_keys(connection, cache_info, keys)


def __cache_remove_keys(connection, cache_info, keys):
    query_struct = Query(
        OP_CACHE_REMOVE_KEYS,
        [
            ('cache_info', CacheInfo),
            ('keys', AnyDataArray()),
        ]
    )
    return query_perform(
        query_struct, connection,
        query_params={
            'cache_info': cache_info,
            'keys': keys,
        },
    )


def cache_remove_all(connection: 'Connection', cache_info: CacheInfo) -> 'APIResult':
    """
    Removes all entries from cache_info, notifying listeners and cache writers.

    :param connection: connection to Ignite server,
    :param cache_info: cache meta info,
    :return: API result data object. Contains zero status on success,
     non-zero status and an error description otherwise.
    """
    return __cache_remove_all(connection, cache_info)


async def cache_remove_all_async(connection: 'AioConnection', cache_info: CacheInfo) -> 'APIResult':
    """
    Async version of cache_remove_all.
    """
    return await __cache_remove_all(connection, cache_info)


def __cache_remove_all(connection, cache_info):
    query_struct = Query(
        OP_CACHE_REMOVE_ALL,
        [
            ('cache_info', CacheInfo),
        ]
    )
    return query_perform(
        query_struct, connection,
        query_params={
            'cache_info': cache_info,
        },
    )


def cache_get_size(
        connection: 'Connection', cache_info: CacheInfo, peek_modes: Union[int, list, tuple] = None
) -> 'APIResult':
    """
    Gets the number of entries in cache.

    :param connection: connection to Ignite server,
    :param cache_info: cache meta info,
    :param peek_modes: (optional) limit count to near cache partition
     (PeekModes.NEAR), primary cache (PeekModes.PRIMARY), or backup cache
     (PeekModes.BACKUP). Defaults to pimary cache partitions (PeekModes.PRIMARY),
    :return: API result data object. Contains zero status and a number of
     cache entries on success, non-zero status and an error description
     otherwise.
    """
    return __cache_get_size(connection, cache_info, peek_modes)


async def cache_get_size_async(
        connection: 'AioConnection', cache_info: CacheInfo, peek_modes: Union[int, list, tuple] = None
) -> 'APIResult':
    return await __cache_get_size(connection, cache_info, peek_modes)


def __cache_get_size(connection, cache_info, peek_modes):
    if peek_modes is None:
        peek_modes = []
    elif not isinstance(peek_modes, (list, tuple)):
        peek_modes = [peek_modes]

    query_struct = Query(
        OP_CACHE_GET_SIZE,
        [
            ('cache_info', CacheInfo),
            ('peek_modes', ByteArray),
        ]
    )
    return query_perform(
        query_struct, connection,
        query_params={
            'cache_info': cache_info,
            'peek_modes': peek_modes,
        },
        response_config=[
            ('count', Long),
        ],
        post_process_fun=__post_process_value_by_key('count')
    )


def cache_local_peek(
        conn: 'Connection', cache_info: CacheInfo, key: Any, key_hint: 'IgniteDataType' = None,
        peek_modes: Union[int, list, tuple] = None
) -> 'APIResult':
    """
    Peeks at in-memory cached value using default optional peek mode.

    This method will not load value from any cache store or from a remote
    node.

    :param conn: connection: connection to Ignite server,
    :param cache_info: cache meta info,
    :param key: entry key,
    :param key_hint: (optional) Ignite data type, for which the given key
     should be converted,
    :param peek_modes: (optional) limit count to near cache partition
     (PeekModes.NEAR), primary cache (PeekModes.PRIMARY), or backup cache
     (PeekModes.BACKUP). Defaults to primary cache partitions (PeekModes.PRIMARY),
    :return: API result data object. Contains zero status and a peeked value
     (null if not found).
    """
    return __cache_local_peek(conn, cache_info, key, key_hint, peek_modes)


async def cache_local_peek_async(
        conn: 'AioConnection', cache_info: CacheInfo, key: Any, key_hint: 'IgniteDataType' = None,
        peek_modes: Union[int, list, tuple] = None,
) -> 'APIResult':
    """
    Async version of cache_local_peek.
    """
    return await __cache_local_peek(conn, cache_info, key, key_hint, peek_modes)


def __cache_local_peek(conn, cache_info, key, key_hint, peek_modes):
    if peek_modes is None:
        peek_modes = []
    elif not isinstance(peek_modes, (list, tuple)):
        peek_modes = [peek_modes]

    query_struct = Query(
        OP_CACHE_LOCAL_PEEK,
        [
            ('cache_info', CacheInfo),
            ('key', key_hint or AnyDataObject),
            ('peek_modes', ByteArray),
        ]
    )
    return query_perform(
        query_struct, conn,
        query_params={
            'cache_info': cache_info,
            'key': key,
            'peek_modes': peek_modes,
        },
        response_config=[
            ('value', AnyDataObject),
        ],
        post_process_fun=__post_process_value_by_key('value')
    )


def __post_process_value_by_key(key):
    def internal(result):
        if result.status == 0:
            result.value = result.value[key]

        return result
    return internal
