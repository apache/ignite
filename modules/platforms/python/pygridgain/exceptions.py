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
from typing import Tuple
from socket import error as SocketError


class ParseError(Exception):
    """
    This exception is raised, when `pygridgain` is unable to build a query to,
    or parse a response from, GridGain node.
    """
    pass


class HandshakeError(SocketError):
    """
    This exception is raised on GridGain binary protocol handshake failure,
    as defined in
    https://apacheignite.readme.io/docs/binary-client-protocol#section-handshake
    """

    def __init__(self, expected_version: Tuple[int, int, int], message: str):
        self.expected_version = expected_version
        self.message = message


class ReconnectError(Exception):
    """
    This exception is raised by `Client.reconnect` method, when no more
    nodes are left to connect to. It is not meant to be an error, but rather
    a flow control tool, similar to `StopIteration`.
    """
    pass


class ParameterError(Exception):
    """
    This exception represents the parameter validation error in any
    `pygridgain` method.
    """
    pass


class CacheError(Exception):
    """
    This exception is raised, whenever any remote Thin client operation
    returns an error.
    """
    pass


class BinaryTypeError(CacheError):
    """
    A remote error in operation with Complex Object registry.
    """
    pass


class CacheCreationError(CacheError):
    """
    This exception is raised, when any complex operation failed
    on cache creation phase.
    """
    pass


class SQLError(CacheError):
    """
    An error in SQL query.
    """
    pass


connection_errors = (IOError, OSError)
