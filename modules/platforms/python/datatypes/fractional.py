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
Ignite decimal data type, named ‘Fractional’ to avoid collision
with standard Python decimal.Decimal class.
"""

import ctypes
from decimal import Decimal

from connection import Connection
from constants import *
from datatypes.type_codes import *


class FractionalBase(ctypes.LittleEndianStructure):
    _pack_ = 1
    _fields_ = [
        ('type_code', ctypes.c_byte),
        ('scale', ctypes.c_int),
        ('length', ctypes.c_int),
    ]


def init(self):
    self.type_code = int.from_bytes(
        getattr(self, '_type_code'),
        byteorder=PROTOCOL_BYTE_ORDER,
    )
    if hasattr(self, 'length'):
        self.length = ctypes.sizeof(self) - ctypes.sizeof(FractionalBase)


def fract_get_attribute(self) -> Decimal:
    sign = 1 if self.data[0] & 0x80 else 0
    data = bytes([self.data[0] & 0x7f]) + self.data[1:]
    result = Decimal(data.decode(PROTOCOL_STRING_ENCODING))
    result = result * Decimal('10') ** Decimal(self.scale)
    if sign:
        result = -result
    return result


def fract_set_attribute(self, value: Decimal):
    sign, digits, scale = value.normalize().as_tuple()
    data = bytearray([ord('0') + digit for digit in digits])
    if sign:
        data[0] |= 0x80
    else:
        data[0] &= 0x7f
    self.scale = scale
    self.data = bytes(data)


def fractional_class(python_var, length=None, **kwargs):
    # python_var is of type Decimal
    if isinstance(python_var, Decimal):
        python_var = python_var.normalize()
        _, digits, _ = python_var.as_tuple()
        length = len(digits)
    return type(
        'Fractional',
        (FractionalBase,),
        {
            '_pack_': 1,
            '_fields_': [
                ('data', ctypes.c_char * length),
            ],
            '_type_code': TC_DECIMAL,
            'init': init,
            'get_attribute': fract_get_attribute,
            'set_attribute': fract_set_attribute,
        },
    )


def fractional_object(connection: Connection, initial=None, **kwargs):
    buffer = initial or connection.recv(1)
    type_code = buffer
    assert type_code == TC_DECIMAL, 'Can not create decimal: wrong type code.'
    buffer += connection.recv(ctypes.sizeof(FractionalBase) - 1)
    header = FractionalBase.from_buffer_copy(buffer)
    length = header.length
    data_class = fractional_class(None, length=length, **kwargs)
    buffer += connection.recv(length)
    data_object = data_class.from_buffer_copy(buffer)
    return data_object
