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
This module contains classes and utilities for Ignite ConnectorConfiguration.
"""

from typing import NamedTuple

from ignitetest.services.utils.ssl.ssl_params import SslParams


class ThinClientConfiguration(NamedTuple):
    max_active_compute_tasks_per_connection: int = 0
    max_active_tx_per_connection: int = 100


class ClientConnectorConfiguration(NamedTuple):
    """
    Ignite ClientConnectorConfiguration.
    Used to configure thin client properties.
    """
    port: int = 10800
    ssl_enabled: bool = False
    use_ignite_ssl_context_factory: bool = True
    ssl_client_auth: bool = False
    ssl_params: SslParams = None
    thin_client_configuration: ThinClientConfiguration = None
    thread_pool_size: int = None
