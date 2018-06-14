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

from collections import OrderedDict
import ctypes
from random import randint

import attr

from connection import Connection
from constants import *
from datatypes import data_class, data_object, string_object
from queries import QueryHeader, ResponseHeader
from queries.op_codes import *
from .result import APIResult


from datatypes.primitive import Byte, Int
from datatypes.primitive_objects import IntObject
from datatypes.cache_config import PString


OP_SUCCESS = 0


@attr.s
class Query:
    op_code = None
    following = attr.ib(type=list)

    @classmethod
    def build_c_type(cls):
        return type(
            cls.__name__,
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('length', ctypes.c_int),
                    ('op_code', ctypes.c_short),
                    ('query_id', ctypes.c_long),
                ],
            },
        )

    def from_python(self, values: dict):
        buffer = b''

        header_class = self.build_c_type()
        header = header_class()
        header.op_code = self.op_code
        header.query_id = randint(MIN_LONG, MAX_LONG)

        for name, c_type in self.following:
            buffer += c_type.from_python(values[name])

        header.length = (
            len(buffer)
            + ctypes.sizeof(header_class)
            - ctypes.sizeof(ctypes.c_int)
        )
        return header.query_id, bytes(header) + buffer


class CachePutQuery(Query):
    op_code = OP_CACHE_PUT


class CacheGetQuery(Query):
    op_code = OP_CACHE_GET


@attr.s
class Response:
    following = attr.ib(type=list)

    @staticmethod
    def build_header():
        return type(
            'ResponseHeader',
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('length', ctypes.c_int),
                    ('query_id', ctypes.c_long),
                    ('status_code', ctypes.c_int),
                ],
            },
        )

    def parse(self, conn: Connection):
        header_class = self.build_header()
        buffer = conn.recv(ctypes.sizeof(header_class))
        header = header_class.from_buffer_copy(buffer)
        fields = []

        if header.status_code == OP_SUCCESS:
            for name, ignite_type in self.following:
                c_type, buffer_fragment = ignite_type.parse(conn)
                buffer += buffer_fragment
                fields.append((name, c_type))
        else:
            c_type, buffer_fragment = PString.parse(conn)
            buffer += buffer_fragment
            fields.append(('error_message', c_type))

        response_class = type(
            'Response',
            (header_class,),
            {
                '_pack_': 1,
                '_fields_': fields,
            }
        )
        return response_class, buffer

    def to_python(self, ctype_object):
        result = OrderedDict()

        for name, c_type in self.following:
            result[name] = c_type.to_python(getattr(ctype_object, name))

        return result if result else None


def cache_put(
    conn: Connection, hash_code: int, key, value,
    binary=False, key_hint=None, value_hint=None
) -> APIResult:
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

    query_struct = CachePutQuery([
        ('hash_code', Int),
        ('flag', Byte),
        ('key', key_hint or PString),
        ('value', value_hint or IntObject),
    ])

    _, send_buffer = query_struct.from_python({
        'hash_code': hash_code,
        'flag': 1 if binary else 0,
        'key': key,
        'value': value,
    })

    conn.send(send_buffer)

    response_struct = Response([])
    response_class, recv_buffer = response_struct.parse(conn)
    response = response_class.from_buffer_copy(recv_buffer)

    result = APIResult(
        status=response.status_code,
        query_id=response.query_id,
    )
    if hasattr(response, 'error_message'):
        result.message = response.error_message
    result.value = response_struct.to_python(response)
    return result


def cache_get(
    conn: Connection, hash_code: int, key,
    binary=False, key_hint=None
) -> APIResult:
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

    query_struct = CacheGetQuery([
        ('hash_code', Int),
        ('flag', Byte),
        ('key', key_hint or PString),
    ])

    _, send_buffer = query_struct.from_python({
        'hash_code': hash_code,
        'flag': 1 if binary else 0,
        'key': key,
    })

    conn.send(send_buffer)

    response_struct = Response([
        # TODO: create generic (smart) object type
        ('value', IntObject),
    ])
    response_class, recv_buffer = response_struct.parse(conn)
    response = response_class.from_buffer_copy(recv_buffer)

    result = APIResult(
        status=response.status_code,
        query_id=response.query_id,
    )
    if hasattr(response, 'error_message'):
        result.message = response.error_message
    result.value = response_struct.to_python(response)['value']
    return result
