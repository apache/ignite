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
import decimal
from datetime import datetime, timedelta
import uuid

from connection import Connection
from constants import *
from .primitive_arrays import *
from .primitive_objects import *
from .strings import *
from .standard import *
from .null_object import *
from .type_codes import *


__all__ = ['AnyDataObject']


tc_map = {
    TC_NULL: Null,

    TC_BYTE: ByteObject,
    TC_SHORT: ShortObject,
    TC_INT: IntObject,
    TC_LONG: LongObject,
    TC_FLOAT: FloatObject,
    TC_DOUBLE: DoubleObject,
    TC_CHAR: CharObject,
    TC_BOOL: BoolObject,

    TC_UUID: UUIDObject,
    TC_DATE: DateObject,
    TC_TIMESTAMP: TimestampObject,
    TC_TIME: TimeObject,

    TC_BYTE_ARRAY: ByteArrayObject,
    TC_SHORT_ARRAY: ShortArrayObject,
    TC_INT_ARRAY: IntArrayObject,
    TC_LONG_ARRAY: LongArrayObject,
    TC_FLOAT_ARRAY: FloatArrayObject,
    TC_DOUBLE_ARRAY: DoubleArrayObject,
    TC_CHAR_ARRAY: CharArrayObject,
    TC_BOOL_ARRAY: BoolArrayObject,

    TC_STRING: String,
    TC_STRING_ARRAY: StringArrayObject,
}


class PrefetchConnection(Connection):
    prefetch = None
    conn = None

    def __init__(self, conn: Connection, prefetch: bytes=b''):
        super().__init__()
        self.conn = conn
        self.prefetch = prefetch

    def recv(self, buffersize, flags=None):
        pref_size = len(self.prefetch)
        if buffersize > pref_size:
            result = self.prefetch + self.conn.recv(
                buffersize-pref_size, flags)
            self.prefetch = b''
            return result
        else:
            result = self.prefetch[:buffersize]
            self.prefetch = self.prefetch[buffersize:]
            return result


class AnyDataObject:

    @staticmethod
    def get_subtype(iterable, allow_none=False):
        # arrays of these types can contain Null objects
        object_array_python_types = [
            str,
            datetime,
            timedelta,
            decimal.Decimal,
            uuid.UUID,
        ]

        iterator = iter(iterable)
        type_first = type(None)
        try:
            while isinstance(None, type_first):
                type_first = type(next(iterator))
        except StopIteration:
            raise TypeError(
                'Can not represent an empty iterable '
                'or an iterable of `NoneType` in Ignite type.'
            )

        if type_first in object_array_python_types:
            allow_none = True

        if all([
            isinstance(x, type_first)
            or ((x is None) and allow_none) for x in iterator
        ]):
            return type_first

    @staticmethod
    def is_iterable(value):
        try:
            iter(value)
            return True
        except TypeError:
            return False

    @classmethod
    def parse(cls, conn: Connection):
        type_code = conn.recv(ctypes.sizeof(ctypes.c_byte))
        data_class = tc_map[type_code]
        return data_class.parse(PrefetchConnection(conn, prefetch=type_code))

    @classmethod
    def to_python(cls, ctype_object):
        type_code = ctype_object.type_code.to_bytes(
            ctypes.sizeof(ctypes.c_byte),
            byteorder=PROTOCOL_BYTE_ORDER
        )
        data_class = tc_map[type_code]
        return data_class.to_python(ctype_object)

    @classmethod
    def from_python(cls, value):
        python_map = {
            int: LongObject,
            float: DoubleObject,
            str: String,
            bytes: String,
            bool: BoolObject,
            type(None): Null,
            uuid.UUID: UUIDObject,
            datetime: DateObject,
            timedelta: TimeObject,
        }

        python_array_map = {
            int: LongArrayObject,
            float: DoubleArrayObject,
            str: StringArrayObject,
            bytes: StringArrayObject,
            bool: BoolArrayObject,
        }

        value_type = type(value)
        if cls.is_iterable(value) and value_type is not str:
            value_subtype = cls.get_subtype(value)
            if value_subtype in python_array_map:
                return python_array_map[value_subtype].from_python(value)
            raise TypeError(
                'Type `array of {}` is invalid'.format(value_subtype)
            )

        if value_type in python_map:
            return python_map[value_type].from_python(value)
        raise TypeError(
            'Type `{}` is invalid.'.format(value_type)
        )
