#                   GridGain Community Edition Licensing
#                   Copyright 2019 GridGain Systems, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
# Restriction; you may not use this file except in compliance with the License. You may obtain a
# copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the specific language governing permissions
# and limitations under the License.
#
# Commons Clause Restriction
#
# The Software is provided to you by the Licensor under the License, as defined below, subject to
# the following condition.
#
# Without limiting other conditions in the License, the grant of rights under the License will not
# include, and the License does not grant to you, the right to Sell the Software.
# For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
# under the License to provide to third parties, for a fee or other consideration (including without
# limitation fees for hosting or consulting/ support services related to the Software), a product or
# service whose value derives, entirely or substantially, from the functionality of the Software.
# Any license notice or attribution required by the License must also include this Commons Clause
# License Condition notice.
#
# For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
# the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
# Edition software provided with this notice.

import ctypes

from pyignite.constants import *
from .base import IgniteDataType


__all__ = [
    'Primitive',
    'Byte', 'Short', 'Int', 'Long', 'Float', 'Double', 'Char', 'Bool',
]


class Primitive(IgniteDataType):
    """
    Ignite primitive type. Base type for the following types:

    - Byte,
    - Short,
    - Int,
    - Long,
    - Float,
    - Double,
    - Char,
    - Bool.
    """

    c_type = None

    @classmethod
    def parse(cls, client: 'Client'):
        return cls.c_type, client.recv(ctypes.sizeof(cls.c_type))

    @staticmethod
    def to_python(ctype_object, *args, **kwargs):
        return ctype_object

    @classmethod
    def from_python(cls, value):
        return bytes(cls.c_type(value))


class Byte(Primitive):
    c_type = ctypes.c_byte


class Short(Primitive):
    c_type = ctypes.c_short


class Int(Primitive):
    c_type = ctypes.c_int


class Long(Primitive):
    c_type = ctypes.c_longlong


class Float(Primitive):
    c_type = ctypes.c_float


class Double(Primitive):
    c_type = ctypes.c_double


class Char(Primitive):
    c_type = ctypes.c_short

    @classmethod
    def to_python(cls, ctype_object, *args, **kwargs):
        return ctype_object.value.to_bytes(
            ctypes.sizeof(cls.c_type),
            byteorder=PROTOCOL_BYTE_ORDER
        ).decode(PROTOCOL_CHAR_ENCODING)

    @classmethod
    def from_python(cls, value):
        if type(value) is str:
            value = value.encode(PROTOCOL_CHAR_ENCODING)
        # assuming either a bytes or an integer
        if type(value) is bytes:
            value = int.from_bytes(value, byteorder=PROTOCOL_BYTE_ORDER)
        # assuming a valid integer
        return value.to_bytes(
            ctypes.sizeof(cls.c_type),
            byteorder=PROTOCOL_BYTE_ORDER
        )


class Bool(Primitive):
    c_type = ctypes.c_bool
