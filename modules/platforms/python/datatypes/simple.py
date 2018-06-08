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
All data types than do not require special conversion between ctypes type
and python type. Most of Ignite primitives − all integers and floats −
fall into this category.
"""

import ctypes

from connection import Connection
from constants import *
from datatypes.class_configs import simple_type_config
from datatypes.type_codes import *


def init(self):
    self.type_code = int.from_bytes(
        getattr(self, '_type_code'),
        byteorder=PROTOCOL_BYTE_ORDER,
    )


def set_attribute(self, value):
    self.value = value


def simple_data_class(python_var, tc_hint=None, payload=False, **kwargs):
    python_type = type(python_var)
    if python_type is int:
        type_code = tc_hint or TC_LONG
    elif python_type is float:
        type_code = tc_hint or TC_DOUBLE
    else:
        type_code = tc_hint
    assert type_code is not None, (
        'Can not map python type {} to simple data class.'.format(python_type)
    )
    class_name, ctypes_type = simple_type_config[type_code]
    fields = [
        ('value', ctypes_type),
    ]
    if not payload:
        fields.insert(0, ('type_code', ctypes.c_byte))
    return type(
        class_name,
        (ctypes.LittleEndianStructure,),
        {
            '_pack_': 1,
            '_fields_': fields,
            '_type_code': type_code,
            'init': (lambda self: None) if payload else init,
            'get_attribute': lambda self: self.value,
            'set_attribute': set_attribute,
        },
    )


def simple_data_object(connection: Connection, initial=None, **kwargs):
    buffer = initial or connection.recv(1)
    type_code = buffer
    data_class = simple_data_class(None, tc_hint=type_code, **kwargs)
    buffer += connection.recv(ctypes.sizeof(simple_type_config[type_code][1]))
    data_object = data_class.from_buffer_copy(buffer)
    return data_object
