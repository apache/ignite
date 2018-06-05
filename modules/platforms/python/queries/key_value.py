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
import socket
from typing import Any, Dict

from constants import *
from datatypes import data_class, string_object
from .common import QueryHeader, ResponseHeader
from .op_codes import *


def cache_put(
    conn: socket.socket,
    hash_code: int, key: Any, value: Any, binary: bool=False,
    key_hint: int=None, value_hint=None,
) -> Dict:
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

    if response_header.status_code == 0:
        return {0: 'Success'}

    error_msg = string_object(conn)
    return {
        response_header.status_code: str(
            error_msg.data,
            encoding=PROTOCOL_STRING_ENCODING,
        ),
    }


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
    response_header = ResponseHeader.from_buffer_copy(buffer)
    if response_header.status_code == 0:
        # TODO: get result data
        return {0: 'Success'}

    error_msg = string_object(conn)
    return {
        response_header.status_code: str(
            error_msg.data,
            encoding=PROTOCOL_STRING_ENCODING,
        ),
    }
