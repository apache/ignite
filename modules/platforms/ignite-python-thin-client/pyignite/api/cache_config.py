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
Set of functions to manipulate caches.

Ignite `cache` can be viewed as a named entity designed to store key-value
pairs. Each cache is split transparently between different Ignite partitions.

The choice of `cache` term is due to historical reasons. (Ignite initially had
only non-persistent storage tier.)
"""

from typing import Union

from pyignite.connection import Connection, AioConnection
from pyignite.datatypes.cache_config import get_cache_config_struct
from pyignite.datatypes.cache_properties import prop_map
from pyignite.datatypes import Int, prop_codes, Short, String, StringArray
from pyignite.queries import Query, ConfigQuery, query_perform
from pyignite.queries.op_codes import (
    OP_CACHE_GET_CONFIGURATION, OP_CACHE_CREATE_WITH_NAME, OP_CACHE_GET_OR_CREATE_WITH_NAME, OP_CACHE_DESTROY,
    OP_CACHE_GET_NAMES, OP_CACHE_CREATE_WITH_CONFIGURATION, OP_CACHE_GET_OR_CREATE_WITH_CONFIGURATION
)
from pyignite.utils import cache_id

from .result import APIResult
from ..datatypes.prop_codes import PROP_EXPIRY_POLICY
from ..exceptions import NotSupportedByClusterError
from ..queries.cache_info import CacheInfo


def compact_cache_config(cache_config: dict) -> dict:
    """
    This is to make cache config read/write-symmetrical.

    :param cache_config: dict of cache config properties,
     like {'is_onheapcache_enabled': 1},
    :return: the same dict, but with property codes as keys,
     like {PROP_IS_ONHEAPCACHE_ENABLED: 1}.
    """
    result = {}
    for k, v in cache_config.items():
        if k == 'length':
            continue
        prop_code = getattr(prop_codes, f'PROP_{k.upper()}')
        result[prop_code] = v
    return result


def cache_get_configuration(connection: 'Connection', cache_info: CacheInfo) -> 'APIResult':
    """
    Gets configuration for the given cache.

    :param connection: connection to Ignite server,
    :param cache_info: cache meta info,
    :return: API result data object. Result value is OrderedDict with
     the cache configuration parameters.
    """
    return __cache_get_configuration(connection, cache_info)


async def cache_get_configuration_async(connection: 'AioConnection', cache_info: CacheInfo) -> 'APIResult':
    """
    Async version of cache_get_configuration.
    """
    return await __cache_get_configuration(connection, cache_info)


def __post_process_cache_config(result):
    if result.status == 0:
        result.value = compact_cache_config(result.value['cache_config'])
    return result


def __cache_get_configuration(connection, cache_info):
    query_struct = Query(
        OP_CACHE_GET_CONFIGURATION,
        [
            ('cache_info', CacheInfo)
        ]
    )
    return query_perform(query_struct, connection,
                         query_params={
                             'cache_info': cache_info
                         },
                         response_config=[
                             ('cache_config', get_cache_config_struct(connection.protocol_context))
                         ],
                         post_process_fun=__post_process_cache_config
                         )


def cache_create(connection: 'Connection', name: str) -> 'APIResult':
    """
    Creates a cache with a given name. Returns error if a cache with specified
    name already exists.

    :param connection: connection to Ignite server,
    :param name: cache name,
    :return: API result data object. Contains zero status if a cache is
     created successfully, non-zero status and an error description otherwise.
    """

    return __cache_create_with_name(OP_CACHE_CREATE_WITH_NAME, connection, name)


async def cache_create_async(connection: 'AioConnection', name: str) -> 'APIResult':
    """
    Async version of cache_create.
    """

    return await __cache_create_with_name(OP_CACHE_CREATE_WITH_NAME, connection, name)


def cache_get_or_create(connection: 'Connection', name: str) -> 'APIResult':
    """
    Creates a cache with a given name. Does nothing if the cache exists.

    :param connection: connection to Ignite server,
    :param name: cache name,
    :return: API result data object. Contains zero status if a cache is
     created successfully, non-zero status and an error description otherwise.
    """

    return __cache_create_with_name(OP_CACHE_GET_OR_CREATE_WITH_NAME, connection, name)


async def cache_get_or_create_async(connection: 'AioConnection', name: str) -> 'APIResult':
    """
    Async version of cache_get_or_create.
    """
    return await __cache_create_with_name(OP_CACHE_GET_OR_CREATE_WITH_NAME, connection, name)


def __cache_create_with_name(op_code, conn, name):
    query_struct = Query(op_code, [('cache_name', String)])
    return query_perform(query_struct, conn, query_params={'cache_name': name})


def cache_destroy(connection: 'Connection', cache: Union[str, int]) -> 'APIResult':
    """
    Destroys cache with a given name.

    :param connection: connection to Ignite server,
    :param cache: name or ID of the cache,
    :return: API result data object.
    """
    return __cache_destroy(connection, cache)


async def cache_destroy_async(connection: 'AioConnection', cache: Union[str, int]) -> 'APIResult':
    """
    Async version of cache_destroy.
    """
    return await __cache_destroy(connection, cache)


def __cache_destroy(connection, cache):
    query_struct = Query(OP_CACHE_DESTROY, [('cache_id', Int)])

    return query_perform(query_struct, connection, query_params={'cache_id': cache_id(cache)})


def cache_get_names(connection: 'Connection') -> 'APIResult':
    """
    Gets existing cache names.

    :param connection: connection to Ignite server,
    :return: API result data object. Contains zero status and a list of cache
     names, non-zero status and an error description otherwise.
    """

    return __cache_get_names(connection)


async def cache_get_names_async(connection: 'AioConnection') -> 'APIResult':
    """
    Async version of cache_get_names.
    """
    return await __cache_get_names(connection)


def __post_process_cache_names(result):
    if result.status == 0:
        result.value = result.value['cache_names']
    return result


def __cache_get_names(connection):
    query_struct = Query(OP_CACHE_GET_NAMES)
    return query_perform(query_struct, connection,
                         response_config=[('cache_names', StringArray)],
                         post_process_fun=__post_process_cache_names)


def cache_create_with_config(connection: 'Connection', cache_props: dict) -> 'APIResult':
    """
    Creates cache with provided configuration. An error is returned
    if the name is already in use.

    :param connection: connection to Ignite server,
    :param cache_props: cache configuration properties to create cache with
     in form of dictionary {property code: python value}.
     You must supply at least name (PROP_NAME),
    :return: API result data object. Contains zero status if cache was created,
     non-zero status and an error description otherwise.
    """
    return __cache_create_with_config(OP_CACHE_CREATE_WITH_CONFIGURATION, connection, cache_props)


async def cache_create_with_config_async(connection: 'AioConnection', cache_props: dict) -> 'APIResult':
    """
    Async version of cache_create_with_config.
    """
    return await __cache_create_with_config(OP_CACHE_CREATE_WITH_CONFIGURATION, connection, cache_props)


def cache_get_or_create_with_config(connection: 'Connection', cache_props: dict) -> 'APIResult':
    """
    Creates cache with provided configuration. Does nothing if the name
    is already in use.

    :param connection: connection to Ignite server,
    :param cache_props: cache configuration properties to create cache with
     in form of dictionary {property code: python value}.
     You must supply at least name (PROP_NAME),
    :return: API result data object. Contains zero status if cache was created,
     non-zero status and an error description otherwise.
    """
    return __cache_create_with_config(OP_CACHE_GET_OR_CREATE_WITH_CONFIGURATION, connection, cache_props)


async def cache_get_or_create_with_config_async(connection: 'AioConnection', cache_props: dict) -> 'APIResult':
    """
    Async version of cache_get_or_create_with_config.
    """
    return await __cache_create_with_config(OP_CACHE_GET_OR_CREATE_WITH_CONFIGURATION, connection, cache_props)


def __cache_create_with_config(op_code, connection, cache_props):
    prop_types, prop_values = {}, {}
    is_expiry_policy_supported = connection.protocol_context.is_expiry_policy_supported()
    for i, prop_item in enumerate(cache_props.items()):
        prop_code, prop_value = prop_item
        if prop_code == PROP_EXPIRY_POLICY and not is_expiry_policy_supported:
            raise NotSupportedByClusterError("'ExpiryPolicy' API is not supported by the cluster")

        prop_name = 'property_{}'.format(i)
        prop_types[prop_name] = prop_map(prop_code)
        prop_values[prop_name] = prop_value
    prop_values['param_count'] = len(cache_props)

    following = [('param_count', Short)] + list(prop_types.items())
    query_struct = ConfigQuery(op_code, following)
    return query_perform(query_struct, connection, query_params=prop_values)
