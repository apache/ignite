#                   GridGain Community Edition Licensing
#                   Copyright 2019 GridGain Systems, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
# Restriction; you may not use this file except in compliance with the License. You may obtain a
# copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the specific language governing permissions
# and limitations under the License.
#
# Commons Clause Restriction
#
# The Software is provided to you by the Licensor under the License, as defined below, subject to
# the following condition.
#
# Without limiting other conditions in the License, the grant of rights under the License will not
# include, and the License does not grant to you, the right to Sell the Software.
# For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
# under the License to provide to third parties, for a fee or other consideration (including without
# limitation fees for hosting or consulting/ support services related to the Software), a product or
# service whose value derives, entirely or substantially, from the functionality of the Software.
# Any license notice or attribution required by the License must also include this Commons Clause
# License Condition notice.
#
# For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
# the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
# Edition software provided with this notice.

"""
This module contains some constants, used internally throughout the API.
"""

import ssl


__all__ = [
    'PROTOCOL_VERSION_MAJOR', 'PROTOCOL_VERSION_MINOR',
    'PROTOCOL_VERSION_PATCH', 'MAX_LONG', 'MIN_LONG', 'MAX_INT', 'MIN_INT',
    'PROTOCOL_BYTE_ORDER', 'PROTOCOL_STRING_ENCODING',
    'PROTOCOL_CHAR_ENCODING', 'SSL_DEFAULT_VERSION', 'SSL_DEFAULT_CIPHERS',
    'FNV1_OFFSET_BASIS', 'FNV1_PRIME',
    'IGNITE_DEFAULT_HOST', 'IGNITE_DEFAULT_PORT',
]

PROTOCOL_VERSION_MAJOR = 1
PROTOCOL_VERSION_MINOR = 2
PROTOCOL_VERSION_PATCH = 0

MAX_LONG = 9223372036854775807
MIN_LONG = -9223372036854775808
MAX_INT = 2147483647
MIN_INT = -2147483648

PROTOCOL_BYTE_ORDER = 'little'
PROTOCOL_STRING_ENCODING = 'utf-8'
PROTOCOL_CHAR_ENCODING = 'utf-16le'

SSL_DEFAULT_VERSION = ssl.PROTOCOL_TLSv1_1
SSL_DEFAULT_CIPHERS = ssl._DEFAULT_CIPHERS

FNV1_OFFSET_BASIS = 0x811c9dc5
FNV1_PRIME = 0x01000193

IGNITE_DEFAULT_HOST = 'localhost'
IGNITE_DEFAULT_PORT = 10800
