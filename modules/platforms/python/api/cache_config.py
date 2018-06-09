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
from constants import *
from datatypes import string_class, string_object
from queries.common import QueryHeader, ResponseHeader
from queries.op_codes import *
from .result import APIResult


def cache_get_names(conn: Connection) -> APIResult:
    """
    Gets existing cache names.

    :param conn: connection to Ignite server,
    :return: API result data object. Contains zero status and a list of cache
    names, non-zero status and an error description otherwise.
    """
    query = QueryHeader()
    query.op_code = OP_CACHE_GET_NAMES
    conn.send(query)
    buffer = conn.recv(ctypes.sizeof(ResponseHeader))
    response_header = ResponseHeader.from_buffer_copy(buffer)
    result = APIResult(status=response_header.status_code)
    if result.status == 0:
        cache_count = int.from_bytes(
            conn.recv(ctypes.sizeof(ctypes.c_int)),
            byteorder=PROTOCOL_BYTE_ORDER
        )
        result.value = []
        for i in range(cache_count):
            cache_name = string_object(conn)
            result.value.append(cache_name.get_attribute())
    else:
        error_msg = string_object(conn)
        result.message = error_msg.get_attribute()
    return result


def cache_create(conn: Connection, name: str) -> APIResult:
    """
    Creates a cache with a given name. Returns error if a cache with specified
    name already exists.

    :param conn: connection to Ignite server,
    :param name: cache name,
    :return: API result data object. Contains zero status if a cache is
    created successfully, non-zero status and an error description otherwise.
    """
    query_class = type(
        'QueryClass',
        (QueryHeader,),
        {
            '_pack_': 1,
            '_fields_': [
                ('cache_name', string_class(name)),
            ],
        },
    )
    query = query_class()
    query.op_code = OP_CACHE_CREATE_WITH_NAME
    query.cache_name = name
    conn.send(query)
    buffer = conn.recv(ctypes.sizeof(ResponseHeader))
    response_header = ResponseHeader.from_buffer_copy(buffer)
    result = APIResult(status=response_header.status_code)
    if result.status != 0:
        error_msg = string_object(conn)
        result.message = error_msg.get_attribute()
    return result


def cache_get_or_create(conn: Connection, name: str) -> APIResult:
    """
    Creates a cache with a given name. Does nothing if the cache exists.

    :param conn: connection to Ignite server,
    :param name: cache name,
    :return: API result data object. Contains zero status if a cache is
    created successfully, non-zero status and an error description otherwise.
    """
    query_class = type(
        'QueryClass',
        (QueryHeader,),
        {
            '_pack_': 1,
            '_fields_': [
                ('cache_name', string_class(name)),
            ],
        },
    )
    query = query_class()
    query.op_code = OP_CACHE_GET_OR_CREATE_WITH_NAME
    query.cache_name = name
    conn.send(query)
    buffer = conn.recv(ctypes.sizeof(ResponseHeader))
    response_header = ResponseHeader.from_buffer_copy(buffer)
    result = APIResult(status=response_header.status_code)
    if result.status != 0:
        error_msg = string_object(conn)
        result.message = error_msg.get_attribute()
    return result


def cache_destroy(conn: Connection, hash_code: int):
    """
    Destroys cache with a given name.

    :param conn: connection to Ignite server,
    :param hash_code: hash code of the cache. Can be obtained by applying
     the `hashcode()` function to the cache name,
    :return: API result data object.
    """
    query_class = type(
        'QueryClass',
        (QueryHeader,),
        {
            '_pack_': 1,
            '_fields_': [
                ('hash_code', ctypes.c_int),
            ],
        },
    )
    query = query_class()
    query.op_code = OP_CACHE_DESTROY
    query.hash_code = hash_code
    conn.send(query)
    buffer = conn.recv(ctypes.sizeof(ResponseHeader))
    response_header = ResponseHeader.from_buffer_copy(buffer)
    result = APIResult(status=response_header.status_code)
    if result.status != 0:
        error_msg = string_object(conn)
        result.message = error_msg.get_attribute()
    return result
