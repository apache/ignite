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

from typing import Optional

from pyignite.constants import *
from pyignite.datatypes import Byte, Int, Short, String
from pyignite.datatypes.internal import Struct

OP_HANDSHAKE = 1


class HandshakeRequest:
    """ Handshake request. """
    handshake_struct = None
    username = None
    password = None

    def __init__(
        self, username: Optional[str]=None, password: Optional[str]=None
    ):
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
            'client_code': 2,  # fixed value defined by protocol
        }
        if self.username and self.password:
            handshake_data.update({
                'username': self.username,
                'password': self.password,
            })
            handshake_data['length'] += sum([
                10,  # each `String` header takes 5 bytes
                len(self.username),
                len(self.password),
            ])
        return self.handshake_struct.from_python(handshake_data)


def read_response(client):
    response_start = Struct([
        ('length', Int),
        ('op_code', Byte),
    ])
    start_class, start_buffer = response_start.parse(client)
    start = start_class.from_buffer_copy(start_buffer)
    data = response_start.to_python(start)
    if data['op_code'] == 0:
        response_end = Struct([
            ('version_major', Short),
            ('version_minor', Short),
            ('version_patch', Short),
            ('message', String),
        ])
        end_class, end_buffer = response_end.parse(client)
        end = end_class.from_buffer_copy(end_buffer)
        data.update(response_end.to_python(end))
    return data
