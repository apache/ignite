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
from decimal import Decimal
from datetime import date, datetime, timedelta
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
    TC_UUID: ('Uuid', ctypes.c_byte*16),
    TC_TIMESTAMP: ('Timestamp', Timestamp),
    TC_ENUM: ('Enum', Enum),
}

standard_from_python = {
    bool: TC_BOOL,
    str: TC_CHAR,
    date: TC_DATE,
    datetime: TC_DATE,
    timedelta: TC_TIME,
    UUID: TC_UUID,
    tuple: TC_ENUM,
    list: TC_ENUM,
}

standard_types = list(standard_type_config.keys())

standard_python_types = list(standard_from_python.keys())

# chose array element type code based on array type code
array_type_mappings = {
    TC_BYTE_ARRAY: TC_BYTE,
    TC_SHORT_ARRAY: TC_SHORT,
    TC_INT_ARRAY: TC_INT,
    TC_LONG_ARRAY: TC_LONG,
    TC_FLOAT_ARRAY: TC_FLOAT,
    TC_DOUBLE_ARRAY: TC_DOUBLE,
    TC_CHAR_ARRAY: TC_CHAR,
    TC_BOOL_ARRAY: TC_BOOL,
    TC_UUID_ARRAY: TC_UUID,
    TC_DATE_ARRAY: TC_DATE,
    TC_TIME_ARRAY: TC_TIME,
    TC_TIMESTAMP_ARRAY: TC_TIMESTAMP,
    TC_ENUM_ARRAY: TC_ENUM,
}

array_types = list(array_type_mappings.keys())

vararray_type_mappings = {
    TC_STRING_ARRAY: TC_STRING,
    TC_DECIMAL_ARRAY: TC_DECIMAL,
}

vararray_types = list(vararray_type_mappings.keys())

# chose array type code based on pythonic type of data element
array_from_python = {
    # simple types
    int: TC_LONG_ARRAY,
    float: TC_FLOAT_ARRAY,
    # fixed-length types
    bool: TC_BOOL_ARRAY,
    date: TC_DATE_ARRAY,
    datetime: TC_DATE_ARRAY,
    timedelta: TC_TIME_ARRAY,
    UUID: TC_UUID_ARRAY,
    tuple: TC_ENUM_ARRAY,
    list: TC_ENUM_ARRAY,
    # variable-length types
    Decimal: TC_DECIMAL_ARRAY,
    str: TC_STRING_ARRAY,
}


def get_array_type_code_by_python(python_var):
    iter_var = iter(python_var)
    element_type = type(next(iter_var))
    return array_from_python[element_type]
