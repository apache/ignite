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
from datetime import datetime, timedelta
import uuid

from connection import Connection
from constants import *
from .type_codes import *
from .null_object import Null


__all__ = [
    'UUIDObject', 'TimestampObject', 'DateObject', 'TimeObject',
]


class FixedSizeStandardObject:
    tc_type = None

    @classmethod
    def build_c_type(cls):
        raise NotImplementedError('')

    @classmethod
    def parse(cls, conn: Connection):
        tc_type = conn.recv(ctypes.sizeof(ctypes.c_byte))

        if tc_type == TC_NULL:
            return Null.build_c_type(), tc_type

        c_type = cls.build_c_type()
        buffer = tc_type + conn.recv(ctypes.sizeof(c_type) - len(tc_type))
        return c_type, buffer


class UUIDObject(FixedSizeStandardObject):
    tc_type = TC_UUID

    @classmethod
    def build_c_type(cls):
        return type(
            cls.__name__,
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('type_code', ctypes.c_byte),
                    ('value', ctypes.c_byte * 16),
                ],
            }
        )

    @classmethod
    def from_python(cls, value: uuid.UUID):
        data_type = cls.build_c_type()
        data_object = data_type()
        data_object.type_code = int.from_bytes(
            cls.tc_type,
            byteorder=PROTOCOL_BYTE_ORDER
        )
        for i, byte in enumerate(bytearray(value.bytes)):
            data_object.value[i] = byte
        return bytes(data_object)

    @classmethod
    def to_python(cls, ctypes_object):
        return uuid.UUID(bytes=bytes(ctypes_object.value))


class TimestampObject(FixedSizeStandardObject):
    tc_type = TC_TIMESTAMP

    @classmethod
    def build_c_type(cls):
        return type(
            cls.__name__,
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('type_code', ctypes.c_byte),
                    ('epoch', ctypes.c_long),
                    ('fraction', ctypes.c_int),
                ],
            }
        )

    @classmethod
    def from_python(cls, value: tuple):
        data_type = cls.build_c_type()
        data_object = data_type()
        data_object.type_code = int.from_bytes(
            cls.tc_type,
            byteorder=PROTOCOL_BYTE_ORDER
        )
        data_object.epoch = int(value[0].timestamp() * 1000)
        data_object.fraction = value[1]
        return bytes(data_object)

    @classmethod
    def to_python(cls, ctypes_object):
        return (
            datetime.fromtimestamp(ctypes_object.epoch/1000),
            ctypes_object.fraction
        )


class DateObject(FixedSizeStandardObject):
    tc_type = TC_DATE

    @classmethod
    def build_c_type(cls):
        return type(
            cls.__name__,
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('type_code', ctypes.c_byte),
                    ('epoch', ctypes.c_long),
                ],
            }
        )

    @classmethod
    def from_python(cls, value: datetime):
        data_type = cls.build_c_type()
        data_object = data_type()
        data_object.type_code = int.from_bytes(
            cls.tc_type,
            byteorder=PROTOCOL_BYTE_ORDER
        )
        data_object.epoch = int(value.timestamp() * 1000)
        return bytes(data_object)

    @classmethod
    def to_python(cls, ctypes_object):
        return datetime.fromtimestamp(ctypes_object.epoch/1000)


class TimeObject(FixedSizeStandardObject):
    tc_type = TC_TIME

    @classmethod
    def build_c_type(cls):
        return type(
            cls.__name__,
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('type_code', ctypes.c_byte),
                    ('value', ctypes.c_long),
                ],
            }
        )

    @classmethod
    def from_python(cls, value: timedelta):
        data_type = cls.build_c_type()
        data_object = data_type()
        data_object.type_code = int.from_bytes(
            cls.tc_type,
            byteorder=PROTOCOL_BYTE_ORDER
        )
        data_object.value = int(value.total_seconds() * 1000)
        return bytes(data_object)

    @classmethod
    def to_python(cls, ctypes_object):
        return timedelta(milliseconds=ctypes_object.value)
