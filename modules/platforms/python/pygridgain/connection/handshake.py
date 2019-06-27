#
# Copyright 2019 GridGain Systems, Inc. and Contributors.
#
# Licensed under the GridGain Community Edition License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from typing import Optional, Tuple

from pygridgain.datatypes import Byte, Int, Short, String
from pygridgain.datatypes.internal import Struct

OP_HANDSHAKE = 1


class HandshakeRequest:
    """ Handshake request. """
    handshake_struct = None
    username = None
    password = None
    protocol_version = None

    def __init__(
        self, protocol_version: Tuple[int, int, int],
        username: Optional[str] = None, password: Optional[str] = None
    ):
        fields = [
            ('length', Int),
            ('op_code', Byte),
            ('version_major', Short),
            ('version_minor', Short),
            ('version_patch', Short),
            ('client_code', Byte),
        ]
        self.protocol_version = protocol_version
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
            'version_major': self.protocol_version[0],
            'version_minor': self.protocol_version[1],
            'version_patch': self.protocol_version[2],
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
