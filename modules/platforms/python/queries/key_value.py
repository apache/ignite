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
        # sadly, data objects' __init__ methods are out of MRO,
        # so we have to implicitly run their init methods here
        for attr_name in dir(self):
            attr = self.__getattribute__(attr_name, original_method=True)
            if hasattr(attr, 'init'):
                attr.init()

    def __setattr__(self, key, value):
        attr = self.__getattribute__(key, original_method=True)
        if hasattr(attr, 'set_attribute'):
            attr.set_attribute(value)
        else:
            super().__setattr__(key, value)

    def __getattribute__(self, item, original_method=False):
        value = super().__getattribute__(item)
        if hasattr(value, 'get_attribute') and not original_method:
            return value.get_attribute()
        return value


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

    query.key = key
    query.value = value

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
