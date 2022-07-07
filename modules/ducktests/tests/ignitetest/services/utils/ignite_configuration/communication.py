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


class TcpCommunicationSpi(CommunicationSpi):
    """
    TcpCommunicationSpi.
    """
    def __init__(self, port=47100, port_range=100):
        self.port = port
        self.port_range = port_range

    @property
    def type(self):
        return "TCP"
