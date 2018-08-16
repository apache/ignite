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
from typing import Optional

from pyignite.constants import *


OP_HANDSHAKE = 1


class HandshakeRequest:
    """ Handshake request. """
    handshake_struct = None
    username = None
    password = None

    def __init__(
        self, username: Optional[str]=None, password: Optional[str]=None
    ):
        from pyignite.datatypes.standard import String
        from pyignite.datatypes.internal import Struct
        from pyignite.datatypes.primitive import Byte, Int, Short

        fields = [
            ('length', Int),
            ('op_code', Byte),
            ('version_major', Short),
            ('version_minor', Short),
            ('version_patch', Short),
            ('client_code', Byte),
        ]
        if username and password:
            self.username = username
            self.password = password
            fields.extend([
                ('username', String),
                ('password', String),
            ])
        self.handshake_struct = Struct(fields)

    def __bytes__(self) -> bytes:
        handshake_data = {
            'length': 8,
            'op_code': OP_HANDSHAKE,
            'version_major': PROTOCOL_VERSION_MAJOR,
            'version_minor': PROTOCOL_VERSION_MINOR,
            'version_patch': PROTOCOL_VERSION_PATCH,
            'client_code': 2,
        }
        if self.username and self.password:
            handshake_data.update({
                'username': self.username,
                'password': self.password,
            })
            handshake_data['length'] += sum([
                10,
                len(self.username),
                len(self.password),
            ])
        return self.handshake_struct.from_python(handshake_data)


def read_response(client):
    buffer = client.recv(4)
    length = int.from_bytes(buffer, byteorder='little')
    buffer += client.recv(length)
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
