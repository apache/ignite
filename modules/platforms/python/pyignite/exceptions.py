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

from socket import error as SocketError


class ParseError(Exception):
    """
    This exception is raised, when `pyignite` is unable to build a query to,
    or parse a response from, Ignite node.
    """
    pass


class HandshakeError(SocketError):
    """
    This exception is raised on Ignite binary protocol handshake failure,
    as defined in
    https://apacheignite.readme.io/docs/binary-client-protocol#section-handshake
    """
    pass


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
