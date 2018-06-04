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
from random import randint
import socket
from typing import Any

from constants import *
from datatypes import data_class
from .op_codes import *


class QueryHeader(ctypes.LittleEndianStructure):
    """
    Standard query header used throughout the Ignite Binary API.

    op_code field sets the query operation.
    Server returns query_id in response as it was given in query. It may help
    matching requests with responses in asynchronous apps.
    """
    _pack_ = 1
    _fields_ = [
        ('length', ctypes.c_int),
        ('op_code', ctypes.c_short),
        ('query_id', ctypes.c_long),
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.query_id = randint(MIN_LONG, MAX_LONG)
        self.length = ctypes.sizeof(self) - ctypes.sizeof(ctypes.c_int)
        # sadly, complex structure attributes can't have their own
        # __init__ methods, so we have to put type handling here.
        # It breaks the encapsulation principle, as well as my heart
        for attr_name in dir(self):
            attr = getattr(self, attr_name)
            if hasattr(attr, '_type_code'):
                attr.type_code = int.from_bytes(
                    getattr(attr, '_type_code'),
                    byteorder=PROTOCOL_BYTE_ORDER,
                )


class ResponseHeader(ctypes.LittleEndianStructure):
    """
    Standard response header.

    status_code == 0 means that operation was successful, and it also means
    that this header may conclude the server response or be followed with
    result data objects. Otherwise, the response continues with the extra
    string object holding the error message.
    """
    _pack_ = 1
    _fields_ = [
        ('length', ctypes.c_int),
        ('query_id', ctypes.c_long),
        ('status_code', ctypes.c_int),
    ]


def cache_put(
    conn: socket.socket,
    hash_code: int, key: Any, value: Any, binary: bool=False,
    key_hint: int=None, value_hint=None,
) -> None:
    """
    This code will be moved to another place, maybe to another class.
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

    # TODO this should be just “query.key = key; query.value = value”
    # TODO uniformly for all data types
    query.key.value = key
    query.value.value = value

    conn.send(query)
    buffer = conn.recv(ctypes.sizeof(ResponseHeader))
    response_header = ResponseHeader.from_buffer_copy(buffer)
    print(response_header)
    print(conn.recv(1000))


def cache_get(
    conn: socket.socket,
    hash_code: int, key: Any, binary: bool=False,
    key_hint: int=None,
) -> Any:
    """
    This code will be moved to another place, maybe to another class.
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
    print(buffer)
