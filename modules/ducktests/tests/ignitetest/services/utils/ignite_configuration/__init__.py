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
import socket
from typing import NamedTuple

from ignitetest.services.utils import IgniteServiceType
from ignitetest.services.utils.ignite_configuration.communication import CommunicationSpi, TcpCommunicationSpi
from ignitetest.services.utils.ssl.client_connector_configuration import ClientConnectorConfiguration
from ignitetest.services.utils.ssl.connector_configuration import ConnectorConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import DiscoverySpi, TcpDiscoverySpi
from ignitetest.services.utils.ssl.ssl_params import SslParams, is_ssl_enabled, get_ssl_params, IGNITE_CLIENT_ALIAS, \
    IGNITE_SERVER_ALIAS
from ignitetest.utils.version import IgniteVersion, DEV_BRANCH


# pylint: disable=no-member
class IgniteConfiguration(NamedTuple):
    """
    Ignite configuration.
    """
    discovery_spi: DiscoverySpi = TcpDiscoverySpi()
    communication_spi: CommunicationSpi = TcpCommunicationSpi()
    version: IgniteVersion = DEV_BRANCH
    cluster_state: str = 'ACTIVE'
    client_mode: bool = False
    consistent_id: str = None
    failure_detection_timeout: int = 10000
    sys_worker_blocked_timeout: int = 10000
    properties: str = None
    data_storage: DataStorageConfiguration = None
    caches: list = []
    local_host: str = None
    ssl_params: SslParams = None
    connector_configuration: ConnectorConfiguration = None
    client_connector_configuration: ClientConnectorConfiguration = None
    auth_enabled: bool = False
    plugins: list = []
    metric_exporter: str = None
    rebalance_thread_pool_size: int = None
    rebalance_batch_size: int = None
    rebalance_batches_prefetch_count: int = None
    rebalance_throttle: int = None

    def __prepare_ssl(self, test_globals, shared_root):
        """
        Updates ssl configuration from globals.
        """
        ssl_params = None
        if self.ssl_params is None and is_ssl_enabled(test_globals):
            ssl_params = get_ssl_params(
                test_globals,
                shared_root,
                IGNITE_CLIENT_ALIAS if self.client_mode else IGNITE_SERVER_ALIAS
            )
        if ssl_params:
            return self._replace(ssl_params=ssl_params,
                                 connector_configuration=ConnectorConfiguration(ssl_enabled=True,
                                                                                ssl_params=ssl_params))
        return self

    def __prepare_discovery(self, node, cluster):
        """
        Updates discovery configuration based on current environment.
        """
        if not self.consistent_id:
            config = self._replace(consistent_id=node.account.externally_routable_ip)
        else:
            config = self

        config = config._replace(local_host=socket.gethostbyname(node.account.hostname))
        config.discovery_spi.prepare_on_start(cluster=cluster)

        return config

    # pylint: disable=protected-access
    def prepare_for_env(self, test_globals, shared_root, node, cluster):
        """
        Updates configuration based on current environment.
        """
        return self.__prepare_ssl(test_globals, shared_root).__prepare_discovery(node, cluster)

    @property
    def service_type(self):
        """
        Application mode.
        """
        return IgniteServiceType.NODE


class IgniteClientConfiguration(IgniteConfiguration):
    """
    Ignite client configuration.
    """
    client_mode = True


class IgniteThinClientConfiguration(NamedTuple):
    """
    Thin client configuration.
    """
    addresses: str = None
    version: IgniteVersion = DEV_BRANCH
    ssl_params: SslParams = None

    def __prepare_ssl(self, test_globals):
        """
        Updates ssl configuration from globals.
        """
        ssl_params = None
        if self.ssl_params is None and is_ssl_enabled(test_globals):
            ssl_params = get_ssl_params(test_globals, IGNITE_CLIENT_ALIAS)
        if ssl_params:
            return self._replace(ssl_params=ssl_params)
        return self

    # pylint: disable=unused-argument,protected-access
    def prepare_for_env(self, test_globals, node, cluster):
    # pylint: disable=unused-argument
    def prepare_for_env(self, test_globals, shared_root, node, cluster):
        """
        Updates configuration based on current environment.
        """
        return self.__prepare_ssl(test_globals)

    @property
    def service_type(self):
        """
        Application mode.
        """
        return IgniteServiceType.THIN_CLIENT
