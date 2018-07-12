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
This module contains some constants, used internally throughout the API.
"""

import ssl


__all__ = [
    'PROTOCOL_VERSION_MAJOR', 'PROTOCOL_VERSION_MINOR',
    'PROTOCOL_VERSION_PATCH', 'MAX_LONG', 'MIN_LONG', 'MAX_INT', 'MIN_INT',
    'PROTOCOL_BYTE_ORDER', 'PROTOCOL_STRING_ENCODING',
    'PROTOCOL_CHAR_ENCODING', 'SSL_DEFAULT_VERSION', 'SSL_DEFAULT_CIPHERS',
    'FNV1_OFFSET_BASIS', 'FNV1_PRIME',
]

PROTOCOL_VERSION_MAJOR = 1
PROTOCOL_VERSION_MINOR = 1
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
