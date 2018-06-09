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
from random import randint

import attr

from constants import *


def hashcode(string: str) -> int:
    """
    Calculate hash code used for referencing cache bucket name
    in Ignite binary API.

    :param string: bucket name (or other string identifier),
    :return: hash code.
    """
    result = 0
    for char in string:
        result = int(
            (((31 * result + ord(char)) ^ 0x80000000) & 0xFFFFFFFF)
            - 0x80000000
        )
    return result


class QueryHeader(ctypes.LittleEndianStructure):
    """
    Standard query header used throughout the Ignite Binary API.

    op_code field sets the query operation.
    Server returns query_id in response as it was given in query. It may help
    matching requests with responses in asynchronous apps.
    """
    _pack_ = 1
    _fields_ = [
        ('length', ctypes.c_int),
        ('op_code', ctypes.c_short),
        ('query_id', ctypes.c_long),
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.query_id = randint(MIN_LONG, MAX_LONG)
        self.length = ctypes.sizeof(self) - ctypes.sizeof(ctypes.c_int)
        # sadly, data objects' __init__ methods are out of MRO,
        # so we have to implicitly run their init methods here
        for attr_name in dir(self):
            attr = self.__getattribute__(attr_name, original_method=True)
            if hasattr(attr, 'init'):
                attr.init()

    def __setattr__(self, key, value):
        attr = self.__getattribute__(key, original_method=True)
        if hasattr(attr, 'set_attribute'):
            attr.set_attribute(value)
        else:
            super().__setattr__(key, value)

    def __getattribute__(self, item, original_method=False):
        value = super().__getattribute__(item)
        if hasattr(value, 'get_attribute') and not original_method:
            return value.get_attribute()
        return value


class ResponseHeader(ctypes.LittleEndianStructure):
    """
    Standard response header.

    status_code == 0 means that operation was successful, and it also means
    that this header may conclude the server response or be followed with
    result data objects. Otherwise, the response continues with the extra
    string object holding the error message.
    """
    _pack_ = 1
    _fields_ = [
        ('length', ctypes.c_int),
        ('query_id', ctypes.c_long),
        ('status_code', ctypes.c_int),
    ]
