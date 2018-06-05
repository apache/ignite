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
from typing import Any, ByteString, Union

from constants import *
from datatypes.class_configs import simple_type_config, simple_types
from datatypes.type_codes import *


NoneType = type(None)


def data_class(
    python_var: Union[Any, None],
    tc_hint: ByteString=None,
    **kwargs,
) -> object:
    """
    Dispatcher function for data class creation functions.

    :param python_var: variable of native Python type to be converted
    to DataObject. The choice of data class depends on its type.
    If None, tc_hint is used,
    :param tc_hint: direct indicator of required data class type,
    :param kwargs: data class-specific arguments,
    :return: data class.
    """
    python_type = type(python_var)
    if python_type in (int, float) or (python_type is NoneType and tc_hint in simple_types):
        return simple_data_class(python_var, tc_hint, **kwargs)
    elif python_type in (str, bytes) or (python_type is NoneType and tc_hint == TC_STRING):
        return string_class(python_var, tc_hint, **kwargs)
    else:
        raise NotImplementedError('This data type is not supported.')


def init(self):
    self.type_code = int.from_bytes(
        getattr(self, '_type_code'),
        byteorder=PROTOCOL_BYTE_ORDER,
    )
    if hasattr(self, 'length'):
        self.length = ctypes.sizeof(self) - ctypes.sizeof(ctypes.c_int)


def simple_type_get_attribute(self):
    return self.value


def simple_type_set_attribute(self, value):
    self.value = value


def simple_data_class(python_var, tc_hint=None, **kwargs):
    python_type = type(python_var)
    if python_type is int:
        type_code = tc_hint or TC_LONG
    elif python_type is float:
        type_code = tc_hint or TC_DOUBLE
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


def string_get_attribute(self):
    try:
        return self.data.decode('utf-8')
    except UnicodeDecodeError:
        return self.data


def string_set_attribute(self, value):
    # warning: no length check is done on this stage
    if type(value) is bytes:
        self.data = value
    else:
        self.data = bytes(value, encoding='utf-8')


def string_class(python_var, length=None, **kwargs):
    # python_var is of type str or bytes
    if type(python_var) is bytes:
        length = len(python_var)
    elif python_var is not None:
        length = len(bytes(python_var, encoding='utf-8'))
    return type(
        'String',
        (ctypes.LittleEndianStructure,),
        {
            '_pack_': 1,
            '_fields_': [
                ('type_code', ctypes.c_byte),
                ('length', ctypes.c_int),
                ('data', ctypes.c_char * length),
            ],
            '_type_code': TC_STRING,
            'init': init,
            'get_attribute': string_get_attribute,
            'set_attribute': string_set_attribute,
        },
    )


def simple_data_object(connection: socket.socket):
    buffer = connection.recv(1)
    type_code = buffer
    data_class = simple_data_class(None, tc_hint=type_code)
    buffer += connection.recv(ctypes.sizeof(simple_type_config[type_code][1]))
    data_object = data_class.from_buffer_copy(buffer)
    return data_object


def string_object(connection: socket.socket):
    buffer = connection.recv(1)
    type_code = buffer
    assert type_code == TC_STRING, 'Can not create string: wrong type code.'
    length_buffer = connection.recv(4)
    length = int.from_bytes(length_buffer, byteorder='little')
    data_class = string_class(None, length)
    buffer += length_buffer + connection.recv(length)
    data_object = data_class.from_buffer_copy(buffer)
    return data_object
