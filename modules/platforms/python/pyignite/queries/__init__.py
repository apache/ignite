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
This module is a source of some basic information about the binary protocol.

Most importantly, it contains `Query` and `Response` base classes.
"""

from collections import OrderedDict
import ctypes
from random import randint

import attr

from pyignite.connection import Connection
from pyignite.constants import *
from pyignite.datatypes.standard import String
from .op_codes import *


@attr.s
class Query:
    op_code = None
    following = attr.ib(type=list, factory=list)
    query_id = attr.ib(type=int, default=None)

    @classmethod
    def build_c_type(cls):
        return type(
            cls.__name__,
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('length', ctypes.c_int),
                    ('op_code', ctypes.c_short),
                    ('query_id', ctypes.c_long),
                ],
            },
        )

    def from_python(self, values: dict=None):
        if values is None:
            values = {}
        buffer = b''

        header_class = self.build_c_type()
        header = header_class()
        header.op_code = self.op_code
        if self.query_id is None:
            header.query_id = randint(MIN_LONG, MAX_LONG)

        for name, c_type in self.following:
            buffer += c_type.from_python(values[name])

        header.length = (
            len(buffer)
            + ctypes.sizeof(header_class)
            - ctypes.sizeof(ctypes.c_int)
        )
        return header.query_id, bytes(header) + buffer


class ConfigQuery(Query):
    """
    This is a special query, used for creating caches with configuration.
    """

    @classmethod
    def build_c_type(cls):
        return type(
            cls.__name__,
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('length', ctypes.c_int),
                    ('op_code', ctypes.c_short),
                    ('query_id', ctypes.c_long),
                    ('config_length', ctypes.c_int),
                ],
            },
        )

    def from_python(self, values: dict = None):
        if values is None:
            values = {}
        buffer = b''

        header_class = self.build_c_type()
        header = header_class()
        header.op_code = self.op_code
        if self.query_id is None:
            header.query_id = randint(MIN_LONG, MAX_LONG)

        for name, c_type in self.following:
            buffer += c_type.from_python(values[name])

        header.length = (
            len(buffer)
            + ctypes.sizeof(header_class)
            - ctypes.sizeof(ctypes.c_int)
        )
        header.config_length = header.length - ctypes.sizeof(header_class)
        return header.query_id, bytes(header) + buffer


@attr.s
class Response:
    following = attr.ib(type=list, factory=list)

    @staticmethod
    def build_header():
        return type(
            'ResponseHeader',
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('length', ctypes.c_int),
                    ('query_id', ctypes.c_long),
                    ('status_code', ctypes.c_int),
                ],
            },
        )

    def parse(self, conn: Connection):
        header_class = self.build_header()
        buffer = conn.recv(ctypes.sizeof(header_class))
        header = header_class.from_buffer_copy(buffer)
        fields = []

        if header.status_code == OP_SUCCESS:
            for name, ignite_type in self.following:
                c_type, buffer_fragment = ignite_type.parse(conn)
                buffer += buffer_fragment
                fields.append((name, c_type))
        else:
            c_type, buffer_fragment = String.parse(conn)
            buffer += buffer_fragment
            fields.append(('error_message', c_type))

        response_class = type(
            'Response',
            (header_class,),
            {
                '_pack_': 1,
                '_fields_': fields,
            }
        )
        return response_class, buffer

    def to_python(self, ctype_object):
        result = OrderedDict()

        for name, c_type in self.following:
            result[name] = c_type.to_python(getattr(ctype_object, name))

        return result if result else None
