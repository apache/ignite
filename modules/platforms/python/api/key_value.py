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

import ctypes

from connection import Connection
from datatypes import data_class, data_object, string_object
from queries.common import QueryHeader, ResponseHeader
from queries.op_codes import *
from .result import APIResult


def cache_put(
    conn: Connection, hash_code: int, key, value,
    binary=False, key_hint=None, value_hint=None
):
    """
    Puts a value with a given key to cache (overwriting existing value if any).

    :param conn: connection to Ignite server,
    :param hash_code: hash code of the cache. Can be obtained by applying
     the `hashcode()` function to the cache name,
    :param key: key for the cache entry. Can be of any supported type,
    :param value: value for the key,
    :param binary: pass True to keep the value in binary form. False
     by default,
    :param key_hint: (optional) Ignite data type, for which the given key
     should be converted,
    :param value_hint: (optional) Ignite data type, for which the given value
     should be converted.
    :return: API result data object. Contains zero status if a value
     is written, non-zero status and an error description otherwise.
    """
    value_class = data_class(value, tc_hint=value_hint)
    key_class = data_class(key, tc_hint=key_hint)
    query_class = type(
        'QueryClass',
        (QueryHeader,),
        {
            '_pack_': 1,
            '_fields_': [
                ('hash_code', ctypes.c_int),
                ('flag', ctypes.c_byte),
                ('key', key_class),
                ('value', value_class),
            ],
        },
    )
    query = query_class()
    query.op_code = OP_CACHE_PUT
    query.hash_code = hash_code
    query.flag = 1 if binary else 0
    query.key = key
    query.value = value
    conn.send(query)
    buffer = conn.recv(ctypes.sizeof(ResponseHeader))
    response_header = ResponseHeader.from_buffer_copy(buffer)
    result = APIResult(status=response_header.status_code)
    if result.status != 0:
        error_msg = string_object(conn)
        result.message = error_msg.get_attribute()
    return result


def cache_get(
    conn: Connection, hash_code: int, key,
    binary=False, key_hint=None
):
    """
    Retrieves a value from cache by key.

    :param conn: connection to Ignite server,
    :param hash_code: hash code of the cache. Can be obtained by applying
     the `hashcode()` function to the cache name,
    :param key: key for the cache entry. Can be of any supported type,
    :param binary: pass True to keep the value in binary form. False
     by default,
    :param key_hint: (optional) Ignite data type, for which the given key
     should be converted,
    :return: API result data object. Contains zero status and a value
     retrieved on success, non-zero status and an error description on failure.
    """
    key_class = data_class(key, tc_hint=key_hint)
    query_class = type(
        'QueryClass',
        (QueryHeader,),
        {
            '_pack_': 1,
            '_fields_': [
                ('hash_code', ctypes.c_int),
                ('flag', ctypes.c_byte),
                ('key', key_class),
            ],
        },
    )
    query = query_class()
    query.op_code = OP_CACHE_GET
    query.hash_code = hash_code
    query.flag = 1 if binary else 0
    query.key = key
    conn.send(query)
    buffer = conn.recv(ctypes.sizeof(ResponseHeader))
    response_header = ResponseHeader.from_buffer_copy(buffer)
    result = APIResult(status=response_header.status_code)
    if result.status != 0:
        error_msg = string_object(conn)
        result.message = error_msg.get_attribute()
    else:
        result_object = data_object(conn)
        result.value = result_object.get_attribute()
    return result
