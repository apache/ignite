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
This module contains cache create/destroy tests that checks if
"""

from ducktape.mark.resource import cluster

from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.ignite_execution_exception import IgniteExecutionException
from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.spark import SparkService
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.services.zk.zookeeper import ZookeeperService
from ignitetest.utils import ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion

# pylint: disable=W0223
class CacheDestroyTest(IgniteTest):
    """
    https://sbtatlas.sigma.sbrf.ru/jira/browse/IGN-1794

    """
    NUM_NODES = 1
    CACHE_NAME = "TEST01"
    CACHES_AMOUNT = 10

    @cluster(num_nodes=NUM_NODES+1)
    @ignite_versions(str(DEV_BRANCH), str(LATEST_2_8))
    def test(self, ignite_version):

        node_config = IgniteConfiguration(version=IgniteVersion(ignite_version))

        ignites = IgniteService(self.test_context, config=node_config, num_nodes=self.NUM_NODES)
        ignites.start()

        # This client just put some data to the cache.
        app_config = node_config._replace(client_mode=True, discovery_spi=from_ignite_cluster(ignites))
        IgniteApplicationService(self.test_context, config=app_config,
                                 java_class_name="org.apache.ignite.internal.ducktest.tests.CreateDestroyCache",
                                 params={"cacheName": self.CACHE_NAME, "caches_number": self.CACHES_AMOUNT}).run()

