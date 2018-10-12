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

from .type_codes import TC_NULL


__all__ = ['Null']


class Null:
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
    def parse(cls, client: 'Client'):
        buffer = client.recv(ctypes.sizeof(ctypes.c_byte))
        data_type = cls.build_c_type()
        return data_type, buffer

    @staticmethod
    def to_python(*args, **kwargs):
        return None

    @staticmethod
    def from_python(*args):
        return TC_NULL

