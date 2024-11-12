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
Module contains snapshot test.
"""
from ducktape.mark import parametrize

from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.services.utils.jvm_utils import java_major_version
from ignitetest.services.utils.ssl.ssl_params import is_ssl_enabled
from ignitetest.utils import cluster, ignite_versions, ignore_if
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import IgniteVersion, LATEST, DEV_BRANCH, OLDEST


class PersistenceUpgradeTest(IgniteTest):
    """
    Test checks persistence upgrade.
    """

    @cluster(num_nodes=1)
    @ignite_versions(str(OLDEST))
    @ignore_if(lambda _, globals: is_ssl_enabled(globals))
    @parametrize(versions=[str(OLDEST), str(LATEST), str(DEV_BRANCH)])
    def upgrade_test(self, versions, ignite_version):
        """
        Basic upgrade test.
        """
        versions = sorted(list(map(IgniteVersion, versions)))

        self.logger.info(f"Testing: {versions}")

        service = IgniteApplicationService(
            self.test_context,
            config=None,  # will be defined later.
            java_class_name="org.apache.ignite.internal.ducktest.tests.persistence_upgrade_test."
                            "DataLoaderAndCheckerApplication"
        )

        java_version = service.java_version()
        if java_major_version(java_version) > 11:
            return f"Skipped on java {java_version}"

        for version in versions:
            service.config = IgniteConfiguration(
                data_storage=DataStorageConfiguration(default=DataRegionConfiguration(persistence_enabled=True)),
                version=version
            )

            service.params = {"check": service.stopped}

            service.start(clean=False)

            control_utility = ControlUtility(service)
            control_utility.activate()

            service.stop()
