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

from constants import *
from datatypes.class_configs import simple_type_config
from datatypes.type_codes import *


NoneType = type(None)


def init(self):
    self.type_code = int.from_bytes(
        getattr(self, '_type_code'),
        byteorder=PROTOCOL_BYTE_ORDER,
    )
    if hasattr(self, 'length'):
        self.length = (
            ctypes.sizeof(self)
            - ctypes.sizeof(ctypes.c_int)
            - ctypes.sizeof(ctypes.c_byte)
        )


def simple_type_get_attribute(self):
    if self._type_code == TC_BOOL:
        return bool(self.value)
    if self._type_code == TC_CHAR:
        return self.value.to_bytes(
            2,
            byteorder=PROTOCOL_BYTE_ORDER
        ).decode(
            PROTOCOL_CHAR_ENCODING
        )
    return self.value


def simple_type_set_attribute(self, value):
    if self._type_code == TC_BOOL:
        value = 1 if value else 0
    elif self._type_code == TC_CHAR:
        value = int.from_bytes(
            value.encode(PROTOCOL_CHAR_ENCODING),
            byteorder=PROTOCOL_BYTE_ORDER
        )
    self.value = value


def simple_data_class(python_var, tc_hint=None, **kwargs):
    python_type = type(python_var)
    if python_type is int:
        type_code = tc_hint or TC_LONG
    elif python_type is float:
        type_code = tc_hint or TC_DOUBLE
    elif python_type is bool:
        type_code = TC_BOOL
    elif python_type is str and len(python_var) == 1:
        type_code = TC_CHAR
    else:
        type_code = tc_hint
    assert type_code is not None, (
        f'Can not map python type {python_type} to simple data class.'
    )
    class_name, ctypes_type = simple_type_config[type_code]
    return type(
        class_name,
        (ctypes.LittleEndianStructure,),
        {
            '_pack_': 1,
            '_fields_': [
                ('type_code', ctypes.c_byte),
                ('value', ctypes_type),
            ],
            '_type_code': type_code,
            'init': init,
            'get_attribute': simple_type_get_attribute,
            'set_attribute': simple_type_set_attribute,
        },
    )


def simple_data_object(connection: socket.socket, initial=None):
    buffer = initial or connection.recv(1)
    type_code = buffer
    data_class = simple_data_class(None, tc_hint=type_code)
    buffer += connection.recv(ctypes.sizeof(simple_type_config[type_code][1]))
    data_object = data_class.from_buffer_copy(buffer)
    return data_object
