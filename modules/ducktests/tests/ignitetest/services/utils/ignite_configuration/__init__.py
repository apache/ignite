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
# limitations under the License.

"""
This module contains IgniteConfiguration classes and utilities.
"""

from typing import NamedTuple

from ignitetest.services.utils.ignite_configuration.data_storage import DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import DiscoverySpi, TcpDiscoverySpi
from ignitetest.utils.version import IgniteVersion, DEV_BRANCH


class IgniteConfiguration(NamedTuple):
    """
    Ignite configuration.
    """
    discovery_spi: DiscoverySpi = TcpDiscoverySpi()
    version: IgniteVersion = DEV_BRANCH
    cluster_state: str = 'ACTIVE'
    client_mode: bool = False
    consistent_id: str = None
    failure_detection_timeout: int = 10000
    properties: str = None
    data_storage: DataStorageConfiguration = None
    caches: list = []


class IgniteClientConfiguration(IgniteConfiguration):
    """
    Ignite client configuration.
    """
    client_mode = True
