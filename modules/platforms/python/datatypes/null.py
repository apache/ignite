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
Null type.
"""

import ctypes
import socket

from .type_codes import *
from .simple import init


def null_class(*args, **kwargs):
    return type(
        'Null',
        (ctypes.LittleEndianStructure,),
        {
            '_pack_': 1,
            '_fields_': [
                ('type_code', ctypes.c_byte),
            ],
            '_type_code': TC_NULL,
            'init': init,
            'get_attribute': lambda self: None,
            'set_attribute': lambda self: None,
        },
    )


def null_object(connection: socket.socket, initial=None, **kwargs):
    buffer = initial or connection.recv(1)
    return null_class().from_buffer_copy(buffer)
