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
from ducktape.errors import TimeoutError

from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.utils import cluster
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import IgniteVersion, LATEST, DEV_BRANCH


class PersistenceUpgradeTest(IgniteTest):
    """
    Test checks persistence upgrade.
    """

    @cluster(num_nodes=1)
    @parametrize(versions=[str(LATEST), str(DEV_BRANCH)])
    def upgrade_test(self, versions):
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

        for version in versions:
            service.config = IgniteConfiguration(
                data_storage=DataStorageConfiguration(default=DataRegionConfiguration(persistent=True)),
                version=version
            )

            service.params = {"check": service.stopped}

            service.start(clean=False)

            control_utility = ControlUtility(service)
            control_utility.activate()

            service.await_event("Checked" if service.params.get("check") else "Prepared", 60, True)

            control_utility.idle_verify()
            control_utility.validate_indexes()

            control_utility.deactivate()

            check_msg_not_exist(service, "Started indexes rebuilding")

            service.stop()


def check_msg_not_exist(service, evt_message):
    """
    Check a message not exist in log on all nodes
    :param service: Service.
    :param evt_message: Event message.
    """
    for node in service.nodes:
        try:
            service.await_event_on_node(evt_message, node, 0.1, from_the_beginning=True)

            raise RuntimeError(f"Message '{evt_message}' exist on node {node.name}.")

        except TimeoutError:
            pass
