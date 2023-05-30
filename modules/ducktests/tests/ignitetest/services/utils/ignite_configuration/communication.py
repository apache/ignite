# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License

"""
Module contains classes and utility methods to create communication configuration for ignite nodes.
"""

from abc import ABCMeta, abstractmethod


class CommunicationSpi(metaclass=ABCMeta):
    """
    Abstract class for CommunicationSpi.
    """
    @property
    @abstractmethod
    def type(self):
        """
        Type of CommunicationSpi.
        """

    @property
    @abstractmethod
    def class_name(self):
        """
        Class name of CommunicationSpi.
        """


class TcpCommunicationSpi(CommunicationSpi):
    """
    TcpCommunicationSpi.
    """

    def __init__(self,
                 local_port=47100,
                 local_port_range=100,
                 idle_connection_timeout: int = None,
                 socket_write_timeout: int = None,
                 selectors_count: int = None,
                 connections_per_node: int = None,
                 use_paired_connections: bool = None,
                 message_queue_limit: int = None,
                 unacknowledged_messages_buffer_size: int = None):
        self.local_port = local_port
        self.local_port_range = local_port_range
        self.idle_connection_timeout: int = idle_connection_timeout
        self.socket_write_timeout: int = socket_write_timeout
        self.selectors_count: int = selectors_count
        self.connections_per_node: int = connections_per_node
        self.use_paired_connections: bool = use_paired_connections
        self.message_queue_limit: int = message_queue_limit
        self.unacknowledged_messages_buffer_size: int = unacknowledged_messages_buffer_size

    @property
    def class_name(self):
        return "org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi"

    @property
    def type(self):
        return "TCP"
