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
Null object.

There can't be null type, because null payload takes exactly 0 bytes.
"""

import ctypes
from io import SEEK_CUR

from .base import IgniteDataType
from .type_codes import TC_NULL


__all__ = ['Null', 'Nullable']

from ..constants import PROTOCOL_BYTE_ORDER


class Null(IgniteDataType):
    default = None
    pythonic = type(None)
    _object_c_type = None

    @classmethod
    def build_c_type(cls):
        if cls._object_c_type is None:
            cls._object_c_type = type(
                cls.__name__,
                (ctypes.LittleEndianStructure,),
                {
                    '_pack_': 1,
                    '_fields_': [
                        ('type_code', ctypes.c_byte),
                    ],
                },
            )
        return cls._object_c_type

    @classmethod
    def parse(cls, stream):
        stream.seek(ctypes.sizeof(ctypes.c_byte), SEEK_CUR)
        return cls.build_c_type()

    @classmethod
    def to_python(cls, ctypes_object, **kwargs):
        return None

    @classmethod
    def from_python(cls, stream, *args):
        stream.write(TC_NULL)


class Nullable(IgniteDataType):
    @classmethod
    def parse_not_null(cls, stream):
        raise NotImplementedError

    @classmethod
    async def parse_not_null_async(cls, stream):
        return cls.parse_not_null(stream)

    @classmethod
    def parse(cls, stream):
        is_null, null_type = cls.__check_null_input(stream)

        if is_null:
            return null_type

        return cls.parse_not_null(stream)

    @classmethod
    async def parse_async(cls, stream):
        is_null, null_type = cls.__check_null_input(stream)

        if is_null:
            return null_type

        return await cls.parse_not_null_async(stream)

    @classmethod
    def from_python_not_null(cls, stream, value, **kwargs):
        raise NotImplementedError

    @classmethod
    async def from_python_not_null_async(cls, stream, value, **kwargs):
        return cls.from_python_not_null(stream, value, **kwargs)

    @classmethod
    def from_python(cls, stream, value, **kwargs):
        if value is None:
            Null.from_python(stream)
        else:
            cls.from_python_not_null(stream, value, **kwargs)

    @classmethod
    async def from_python_async(cls, stream, value, **kwargs):
        if value is None:
            Null.from_python(stream)
        else:
            await cls.from_python_not_null_async(stream, value, **kwargs)

    @classmethod
    def to_python_not_null(cls, ctypes_object, **kwargs):
        raise NotImplementedError

    @classmethod
    async def to_python_not_null_async(cls, ctypes_object, **kwargs):
        return cls.to_python_not_null(ctypes_object, **kwargs)

    @classmethod
    def to_python(cls, ctypes_object, **kwargs):
        if cls.__is_null(ctypes_object):
            return None

        return cls.to_python_not_null(ctypes_object, **kwargs)

    @classmethod
    async def to_python_async(cls, ctypes_object, **kwargs):
        if cls.__is_null(ctypes_object):
            return None

        return await cls.to_python_not_null_async(ctypes_object, **kwargs)

    @classmethod
    def __check_null_input(cls, stream):
        type_len = ctypes.sizeof(ctypes.c_byte)

        if stream.slice(offset=type_len) == TC_NULL:
            stream.seek(type_len, SEEK_CUR)
            return True, Null.build_c_type()

        return False, None

    @classmethod
    def __is_null(cls, ctypes_object):
        return ctypes_object.type_code == int.from_bytes(TC_NULL, byteorder=PROTOCOL_BYTE_ORDER)
