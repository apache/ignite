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

from pyignite.constants import *


OP_HANDSHAKE = 1


class HandshakeRequest(ctypes.LittleEndianStructure):
    _pack_ = 1
    _fields_ = [
        ('length', ctypes.c_int),
        ('op_code', ctypes.c_byte),
        ('version_major', ctypes.c_short),
        ('version_minor', ctypes.c_short),
        ('version_patch', ctypes.c_short),
        ('client_code', ctypes.c_byte),
    ]

    def __init__(self):
        super().__init__()
        self.length = 8
        self.op_code = OP_HANDSHAKE
        self.version_major = PROTOCOL_VERSION_MAJOR
        self.version_minor = PROTOCOL_VERSION_MINOR
        self.version_patch = PROTOCOL_VERSION_PATCH
        self.client_code = 2


def read_response(conn):
    buffer = conn.recv(4)
    length = int.from_bytes(buffer, byteorder='little')
    buffer += conn.recv(length)
    op_code = int.from_bytes(buffer[4:5], byteorder='little')
    fields = [
        ('length', ctypes.c_int),
        ('op_code', ctypes.c_byte),
    ]
    if op_code == 0:
        fields += [
            ('version_major', ctypes.c_short),
            ('version_minor', ctypes.c_short),
            ('version_patch', ctypes.c_short),
        ]
    response_class = type(
        'HandshakeResponse',
        (ctypes.LittleEndianStructure,),
        {
            '_pack_': 1,
            '_fields_': fields,
        },
    )
    return response_class.from_buffer_copy(buffer)
