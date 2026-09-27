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

from typing import Tuple
from socket import error as SocketError


class ParseError(Exception):
    """
    This exception is raised, when `pyignite` is unable to build a query to,
    or parse a response from, Ignite node.
    """
    pass


class AuthenticationError(Exception):
    """
    This exception is raised on authentication failure.
    """

    def __init__(self, message: str):
        self.message = message


class HandshakeError(SocketError):
    """
    This exception is raised on Ignite binary protocol handshake failure,
    as defined in
    https://ignite.apache.org/docs/latest/binary-client-protocol/binary-client-protocol#connection-handshake
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
    This exception represents the parameter validation error in any `pyignite`
    method.
    """
    pass


class CacheError(Exception):
    """
    This exception is raised, whenever any remote Thin client cache operation
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


class ClusterError(Exception):
    """
    This exception is raised, whenever any remote Thin client cluster operation
    returns an error.
    """
    pass


class NotSupportedByClusterError(Exception):
    """
    This exception is raised, whenever cluster does not supported specific
    operation probably because it is outdated.
    """
    pass


class NotSupportedError(Exception):
    """
    This exception is raised, whenever client does not support specific
    operation.
    """
    pass


connection_errors = (IOError, OSError, EOFError)
