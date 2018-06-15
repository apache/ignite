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
UTF-8-encoded human-readable strings.
"""

import ctypes
from connection import Connection

from constants import *
from .type_codes import *
from .null_object import Null


__all__ = [
    'PString',
    'PStringArray',
    'PStringArrayObject',
]


class PString:
    """ Pascal-style string: `c_int` counter, followed by count*bytes. """

    @staticmethod
    def build_c_type(length: int):
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
            },
        )

    @staticmethod
    def parse(conn: Connection):
        tc_type = conn.recv(ctypes.sizeof(ctypes.c_byte))
        # String or Null
        if tc_type == TC_NULL:
            return Null.build_c_type(), tc_type

        buffer = tc_type + conn.recv(ctypes.sizeof(ctypes.c_int))
        length = int.from_bytes(buffer[1:], byteorder=PROTOCOL_BYTE_ORDER)

        data_type = PString.build_c_type(length)
        buffer += conn.recv(ctypes.sizeof(data_type) - len(buffer))

        return data_type, buffer

    @staticmethod
    def to_python(ctype_object):
        length = getattr(ctype_object, 'length', None)
        if length is None:
            return None
        elif length > 0:
            return ctype_object.data.decode(PROTOCOL_STRING_ENCODING)
        else:
            return ''

    @staticmethod
    def from_python(value):
        if isinstance(value, str):
            value = value.encode(PROTOCOL_STRING_ENCODING)
        length = len(value)
        data_type = PString.build_c_type(length)
        data_object = data_type()
        data_object.type_code = int.from_bytes(
            TC_STRING,
            byteorder=PROTOCOL_BYTE_ORDER
        )
        data_object.length = length
        data_object.data = value
        return bytes(data_object)


class PStringArray:

    @classmethod
    def build_header_class(cls):
        return type(
            cls.__name__+'Header',
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('length', ctypes.c_int),
                ],
            }
        )

    @classmethod
    def parse(cls, conn: Connection):
        header_class = cls.build_header_class()
        buffer = conn.recv(ctypes.sizeof(header_class))
        header = header_class.from_buffer_copy(buffer)
        fields = []
        for i in range(header.length):
            c_type, buffer_fragment = PString.parse(conn)
            buffer += buffer_fragment
            fields.append(('element_{}'.format(i), c_type))

        final_class = type(
            cls.__name__,
            (header_class,),
            {
                '_pack_': 1,
                '_fields_': fields,
            }
        )
        return final_class, buffer

    @classmethod
    def to_python(cls, ctype_object):
        result = []
        for i in range(ctype_object.length):
            result.append(
                PString.to_python(
                    getattr(ctype_object, 'element_{}'.format(i))
                )
            )
        return result

    @classmethod
    def from_python(cls, value):
        header_class = cls.build_header_class()
        header = header_class()
        if hasattr(header, 'type_code'):
            header.type_code = TC_STRING_ARRAY
        header.length = len(value)
        buffer = bytes(header)

        for string in value:
            buffer += PString.from_python(string)
        return buffer


class PStringArrayObject(PStringArray):

    @classmethod
    def build_header_class(cls):
        return type(
            cls.__name__+'Header',
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('type_code', ctypes.c_byte),
                    ('length', ctypes.c_int),
                ],
            }
        )
