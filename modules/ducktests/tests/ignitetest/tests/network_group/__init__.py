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

from abc import ABC, abstractmethod
from typing import Dict, List

from ducktape.services.service import Service

from ignitetest.services.network_group.configuration import NetworkGroupStore
from ignitetest.services.network_group.manager import NetworkGroupManager
from ignitetest.utils.ignite_test import IgniteTest


class NetworkGroupAbstractTest(IgniteTest, ABC):
    """
    Abstract test for network-controlled tests.
    Ensures network is deployed before the test and cleaned up after.
    """
    def _configure_services(self, **kwargs):
        pass

    def _configure_network_group_store(self, **kwargs) -> NetworkGroupStore:
        return NetworkGroupStore()

    def _configure_network_group_registry(self, **kwargs) -> Dict[str, List[Service]]:
        return {}

    @abstractmethod
    def _run(self, network_mgr: NetworkGroupManager, **kwargs):
        pass

    def configure_network_and_run(self, **kwargs):
        self.logger.debug("Deploying network topology...")

        self._configure_services(**kwargs)

        grp_store = self._configure_network_group_store(**kwargs)
        grp_registry = self._configure_network_group_registry(**kwargs)

        try:
            with NetworkGroupManager(self.logger, grp_store, grp_registry) as network_mgr:
                self.logger.debug("Network configuration complete. Starting test logic ...")

                self._run(network_mgr, **kwargs)
        finally:
            # NetworkGroupManager.__exit__ has restored the network by this point.
            self.logger.debug("Network is restored.")