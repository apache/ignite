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
"""
This module contains some constants, used internally throughout the API.
"""

import ssl


__all__ = [
    'PROTOCOLS', 'MAX_LONG', 'MIN_LONG', 'MAX_INT', 'MIN_INT',
    'PROTOCOL_BYTE_ORDER', 'PROTOCOL_STRING_ENCODING',
    'PROTOCOL_CHAR_ENCODING', 'SSL_DEFAULT_VERSION', 'SSL_DEFAULT_CIPHERS',
    'FNV1_OFFSET_BASIS', 'FNV1_PRIME', 'DEFAULT_HOST', 'DEFAULT_PORT',
    'RHF_ERROR', 'RHF_TOPOLOGY_CHANGED', 'AFFINITY_DELAY', 'AFFINITY_RETRIES',
    'RECONNECT_BACKOFF_SEQUENCE',
]

PROTOCOLS = {
    (1, 4, 0),
    (1, 3, 0),
    (1, 2, 0),
}

PROTOCOL_VERSION_MAJOR = 1
PROTOCOL_VERSION_MINOR = 4
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

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 10800

# response header flags
RHF_ERROR = 1
RHF_TOPOLOGY_CHANGED = 2

AFFINITY_DELAY = 0.01
AFFINITY_RETRIES = 32

RECONNECT_BACKOFF_SEQUENCE = [0, 1, 1, 2, 3, 5, 8, 13]
