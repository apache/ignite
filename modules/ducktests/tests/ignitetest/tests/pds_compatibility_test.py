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
This module contains tests that checks that dev version compatible with LATEST
"""

import time
from enum import IntEnum

from ducktape.mark import matrix
from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.utils import cluster, versions_pair
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, LATEST, IgniteVersion
from ignitetest.utils.enum import constructible


@constructible
class LoadType(IntEnum):
    """
    Load type.
    """
    DICTIONARY_CACHE = 1
    SQL_CACHE = 2


# pylint: disable=W0223
# pylint: disable=no-member
class PdsCompatibilityTest(IgniteTest):
    """
    Saves data using previous version of ignite and then load this data using actual version.

    DictianaryCacheApplication - create <Long,String> replicated cache
    SqlCacheApplication - create pojo cache with index
    TODO: VariablesCacheApplication - create caches for different simple Java library objects

    """

    DATA_AMOUNT = 100

    DICTIONARY_APP_CLASS = "org.apache.ignite.internal.ducktest.tests.pds_compatibility_test.DictionaryCacheApplication"
    DICTIONARY_CHECK_CLASS = "org.apache.ignite.internal.ducktest.tests.pds_compatibility_test." \
                             "DictionaryCacheApplicationCheck"
    SQL_APP_CLASS = "org.apache.ignite.internal.ducktest.tests.pds_compatibility_test.SqlCacheApplication"
    SQL_CHECK_CLASS = "org.apache.ignite.internal.ducktest.tests.pds_compatibility_test.SqlCacheApplicationCheck"

    @cluster(num_nodes=5)
    @versions_pair(str(DEV_BRANCH), str(LATEST))
    @matrix(load_type=[LoadType.DICTIONARY_CACHE, LoadType.SQL_CACHE])
    def test_pds_compatibility(self, ignite_version_1, ignite_version_2, load_type):
        """
        Saves data using previous version of ignite and then load this data using actual version.
        """

        l_type = LoadType.construct_from(load_type)
        if l_type == LoadType.DICTIONARY_CACHE:
            application_class = self.DICTIONARY_APP_CLASS
            check_class = self.DICTIONARY_CHECK_CLASS
        elif l_type == LoadType.SQL_CACHE:
            application_class = self.SQL_APP_CLASS
            check_class = self.SQL_CHECK_CLASS

        num_nodes = len(self.test_context.cluster) - 1

        # Start server with ignite_version_2

        server_configuration = IgniteConfiguration(cluster_state="INACTIVE",
                                                   version=IgniteVersion(ignite_version_1),
                                                   data_storage=DataStorageConfiguration(
                                                       default=DataRegionConfiguration(name='persistent',
                                                                                       persistent=True)
                                                   ))
        ignite = IgniteService(self.test_context, server_configuration, num_nodes=num_nodes)

        # TODO: Remove after merge: Start on the same nodes fix
        running_nodes_tmp = ignite.nodes.copy()

        ignite.start(True)

        control_utility = ControlUtility(ignite, self.test_context)
        control_utility.activate()

        # This client just put some data to the cache.
        app_config = server_configuration._replace(client_mode=True, discovery_spi=from_ignite_cluster(ignite))
        app = IgniteApplicationService(self.test_context, config=app_config,
                                       java_class_name=application_class,
                                       params={"cacheName": "test-cache", "range": self.DATA_AMOUNT})

        app.start()

        app.await_event("Cache created",
                        timeout_sec=120,
                        from_the_beginning=True,
                        backoff_sec=1)

        app.stop()
        app.free()

        control_utility.deactivate()

        ignite.stop()
        ignite.free()

        # Start server with ignite_version_2

        server_configuration = IgniteConfiguration(cluster_state="INACTIVE",
                                                   version=IgniteVersion(ignite_version_2),
                                                   data_storage=DataStorageConfiguration(
                                                       default=DataRegionConfiguration(name='persistent',
                                                                                       persistent=True),
                                                       regions=[
                                                           DataRegionConfiguration(name='in-memory', persistent=False,
                                                                                   max_size=100 * 1024 * 1024)]
                                                   ))
        ignite = IgniteService(self.test_context, server_configuration, num_nodes=num_nodes)

        ignite.nodes = running_nodes_tmp

        ignite.start(False)

        # # TODO: Remove after merge: IGNITE-13829: Added log rotation to ducktape-tests
        time.sleep(2)

        control_utility = ControlUtility(ignite, self.test_context)
        control_utility.activate()

        # Start client

        app_config = server_configuration._replace(client_mode=True, discovery_spi=from_ignite_cluster(ignite))
        app = IgniteApplicationService(self.test_context, config=app_config,
                                       java_class_name=check_class,
                                       params={"cacheName": "test-cache", "range": self.DATA_AMOUNT})

        app.start()

        app.await_event("Cache checked",
                        timeout_sec=120,
                        from_the_beginning=True,
                        backoff_sec=1)

        app.stop()
        ignite.stop()
