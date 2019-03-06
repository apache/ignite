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


class RoundRobin:
    """
    Round-robin generator for use with `Client.connect()`. Cycles a node
    list until a maximum number of reconnects is reached (if set).
    """

    def __init__(self, nodes: list, max_reconnects: int=None):
        """
        :param nodes: list of two-tuples of (host, port) format,
        :param max_reconnects: (optional) maximum number of reconnect attempts.
         defaults to None (cycle nodes infinitely).
        """
        self.nodes = nodes
        self.max_reconnects = max_reconnects
        self.node_index = 0
        self.reconnects = 0

    def __iter__(self) -> 'RoundRobin':
        return self

    def __next__(self) -> tuple:
        if self.max_reconnects is not None:
            if self.reconnects >= self.max_reconnects:
                raise StopIteration
            else:
                self.reconnects += 1

        if self.node_index >= len(self.nodes):
            self.node_index = 0
        node = self.nodes[self.node_index]
        self.node_index += 1
        return node
