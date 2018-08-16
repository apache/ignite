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
    """
    Handshake request have dynamic fields, so unfortunately it can not be
    a ctypes.Structure descendant.
    """
    c_type = None
    username = None
    password = None
    fields = [
        ('length', ctypes.c_int),
        ('op_code', ctypes.c_byte),
        ('version_major', ctypes.c_short),
        ('version_minor', ctypes.c_short),
        ('version_patch', ctypes.c_short),
        ('client_code', ctypes.c_byte),
    ]

    def __init__(
        self, username: Optional[str]=None, password: Optional[str]=None
    ):
        fields = self.fields.copy()
        if username and password:
            from pyignite.datatypes import String

            username_class = String.build_c_type(len(username))
            password_class = String.build_c_type(len(password))
            self.username = username
            self.password = password
            fields.extend([
                ('username', username_class),
                ('password', password_class),
            ])
        self.c_type = type(
            self.__class__.__name__,
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': fields,
            }
        )

    def __bytes__(self):
        from pyignite.datatypes import String

        request = self.c_type()
        request.length = (
            ctypes.sizeof(self.c_type) - ctypes.sizeof(ctypes.c_int)
        )
        request.op_code = OP_HANDSHAKE
        request.version_major = PROTOCOL_VERSION_MAJOR
        request.version_minor = PROTOCOL_VERSION_MINOR
        request.version_patch = PROTOCOL_VERSION_PATCH
        request.client_code = 2
        if hasattr(request, 'username') and hasattr(request, 'password'):
            type_code = int.from_bytes(
                String.type_code,
                byteorder=PROTOCOL_BYTE_ORDER
            )
            request.username.type_code = request.password.type_code = type_code
            request.username.length = len(self.username)
            request.username.data = bytes(
                self.username, encoding=PROTOCOL_STRING_ENCODING
            )
            request.password.length = len(self.password)
            request.password.data = bytes(
                self.password, encoding=PROTOCOL_STRING_ENCODING
            )
        return bytes(request)


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
