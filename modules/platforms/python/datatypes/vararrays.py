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
Arrays of elements of variable length.
"""

import ctypes
import socket

from constants import *
from .class_configs import (
    get_array_type_code_by_python,
    vararray_type_mappings,
    vararray_types,
)



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


def vararray_get_attribute(self):
    pass


def vararray_set_attribute(self, value):
    pass


def vararray_data_class(python_var, tc_hint=None, length=None, **kwargs):
    from datatypes import data_class

    element_class = None
    elements = []

    fields = [
        ('type_code', ctypes.c_byte),
        ('length', ctypes.c_int),
    ]

    type_code = tc_hint or get_array_type_code_by_python(python_var)
    element_type_code = vararray_type_mappings[type_code]

    if python_var:
        for i, py_elem in enumerate(python_var):
            element_class = data_class(
                py_elem,
                tc_hint=element_type_code,
                payload=True
            )
            elements.append(('element_{}'.format(i), element_class))
        fields.append(('elements', elements))
    # if no python type provided, returns array header type

    return type(
        data_class(
            None,
            tc_hint=element_type_code,
            length=1  # or any fake number, just to form a data class name
        ).__name__ + 'Array',
        (ctypes.LittleEndianStructure,),
        {
            '_pack_': 1,
            '_fields_': fields,
            '_type_code': type_code,
            'init': init,
            'get_attribute': vararray_get_attribute,
            'set_attribute': vararray_set_attribute,
        }
    )


def vararray_data_object(connection: socket.socket, initial=None, **kwargs):
    from datatypes import data_class, data_object

    buffer = initial or connection.recv(1)
    type_code = buffer
    assert type_code in vararray_types, (
        'Can not create array: wrong type code.'
    )
    vararray_class = vararray_data_class(None, tc_hint=type_code)
    buffer += connection.recv(ctypes.sizeof(vararray_class) - len(buffer))
    header = vararray_class.from_buffer_copy(buffer)
    elements = []
    element_type_code = vararray_type_mappings[type_code]

    for i in range(header.length):
        element_object = data_object(
            connection,
            initial=element_type_code,
        )
        element_class = data_class(
            None,
            tc_hint=element_type_code,
            length=element_object.length,
            payload=True,
        )
        elements.append(('element_{}'.format(i), element_class))
        buffer += bytes(element_object)[1:]

    elements_class = type(
        vararray_class.__name__+'Elements',
        (ctypes.LittleEndianStructure,),
        {
            '_pack_': 1,
            '_fields_': elements,
        }
    )
    final_class = type(
        vararray_class.__name__,
        (vararray_class,),
        {
            '_pack_': 1,
            '_fields_': [
                ('elements', elements_class),
            ],
            'init': init,
            'get_attribute': vararray_get_attribute,
            'set_attribute': vararray_set_attribute,
        }
    )
    data_object = final_class.from_buffer_copy(buffer)
    return data_object
