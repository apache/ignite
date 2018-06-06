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
from datetime import date, datetime, time
from uuid import UUID

from .type_codes import *


simple_type_config = {
    TC_BYTE: ('Byte', ctypes.c_byte),
    TC_SHORT: ('Short', ctypes.c_short),
    TC_INT: ('Int', ctypes.c_int),
    TC_LONG: ('Long', ctypes.c_long),
    TC_FLOAT: ('Float', ctypes.c_float),
    TC_DOUBLE: ('Double', ctypes.c_double),
}

simple_types = list(simple_type_config.keys())

simple_python_types = (int, float)


class Timestamp(ctypes.LittleEndianStructure):
    _pack_ = 1
    _fields_ = [
        ('epoch', ctypes.c_long),
        ('fraction', ctypes.c_int),
    ]


class Enum(ctypes.LittleEndianStructure):
    _pack_ = 1
    _fields_ = [
        ('type_id', ctypes.c_int),
        ('ordinal', ctypes.c_int),
    ]


standard_type_config = {
    TC_CHAR: ('Char', ctypes.c_short),
    TC_BOOL: ('Bool', ctypes.c_byte),
    TC_TIME: ('Time', ctypes.c_long),
    TC_DATE: ('Date', ctypes.c_long),
    TC_UUID: ('UUID', ctypes.c_byte*16),
    TC_TIMESTAMP: ('Timestamp', Timestamp),
    TC_ENUM: ('Enum', Enum),
}

standard_from_python = {
    bool: TC_BOOL,
    str: TC_CHAR,
    date: TC_DATE,
    datetime: TC_DATE,
    time: TC_TIME,
    UUID: TC_UUID,
    tuple: TC_ENUM,
}

standard_types = list(standard_type_config.keys())

standard_python_types = list(standard_from_python.keys())
