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
from datatypes.class_configs import array_type_mappings, array_types


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


def array_get_attribute(self):
    return [x for x in self.elements]


def array_set_attribute(self, value):
    iter_var = iter(value)
    for i in range(self.length):
        self.elements[i].set_atribute(iter_var.next())


def array_data_class(python_var, tc_hint, length=None, **kwargs):
    from datatypes import data_class

    if python_var:
        element_class = data_class(python_var[0], payload=True, **kwargs)
        length = len(python_var)
    else:
        element_class = data_class(
            None,
            tc_hint=array_type_mappings[tc_hint],
            payload=True
        )

    return type(
        element_class.__name__ + 'Array',
        (ctypes.LittleEndianStructure,),
        {
            '_pack_': 1,
            '_fields_': [
                ('type_code', ctypes.c_byte),
                ('length', ctypes.c_int),
                ('elements', element_class*length),
            ],
            'init': init,
            'get_attribute': array_get_attribute,
            'set_attribute': array_set_attribute,
        }
    )


def array_data_object(connection: socket.socket, initial=None, **kwargs):
    buffer = initial or connection.recv(1)
    type_code = buffer
    assert type_code in array_types, 'Can not create array: wrong type code.'
    length_buffer = connection.recv(ctypes.sizeof(ctypes.c_int))
    length = int.from_bytes(length_buffer, byteorder=PROTOCOL_BYTE_ORDER)
    data_class = array_data_class(None, tc_hint=type_code, length=length)
    buffer += length_buffer + connection.recv(
        ctypes.sizeof(data_class)
        - ctypes.sizeof(ctypes.c_byte)
        - ctypes.sizeof(ctypes.c_int)
    )
    data_object = data_class.from_buffer_copy(buffer)
    return data_object
