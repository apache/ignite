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
Module contains discovery tests.
"""

from ducktape.mark.resource import cluster
from ducktape.tests.status import FAIL

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.utils import ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion


# pylint: disable=W0223
class SnapshotTest(IgniteTest):
    """
    Test Snapshot.
    """
    NUM_NODES = 4

    SNAPSHOT_NAME = "test_snap"

    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(DEV_BRANCH))
    def snapshot_test(self, ignite_version):
        """
        Test Snapshot.
        1. Start of ignite cluster.
        2. Activate cluster.
        3. Load.
        4. Idle verify dump_1.
        5. Snapshot.
        6. Load.
        7. Idle verify dump_2.
        8. Сhecking that dump_1 and dump_2 are different.
        9. Stop ignite and replace db from snapshot.
        10. Run ignite cluster.
        11. Idle verify dump_3.
        12. Checking the equality of dump_1 and dump_3.
        """
        data_storage = DataStorageConfiguration(default=DataRegionConfiguration(persistent=True))

        ignite_config = IgniteConfiguration(
            version=IgniteVersion(ignite_version),
            data_storage=data_storage
        )

        service = IgniteService(self.test_context, ignite_config, num_nodes=self.NUM_NODES - 1)
        service.start()

        control_utility = ControlUtility(service, self.test_context)
        control_utility.activate()

        client_config = IgniteConfiguration(
            client_mode=True,
            version=IgniteVersion(ignite_version),
            discovery_spi=from_ignite_cluster(service),
        )

        streamer = IgniteApplicationService(
            self.test_context,
            client_config,
            java_class_name="org.apache.ignite.internal.ducktest.tests.UuidStreamerApplication",
            params={
                "cacheName": "test-cache",
                "iterSize": 512 * 1024
            }
        )

        load(streamer, duration=300)

        node = service.nodes[0]

        control_utility.idle_verify(check_assert=True)
        control_utility.validate_indexes(check_assert=True)
        dump_1 = control_utility.idle_verify_dump(node, return_path=True)

        self.logger.info(f'Path to dump_1 on {node.account.externally_routable_ip}={dump_1}')

        control_utility.snapshot_create(self.SNAPSHOT_NAME)

        load(streamer)

        dump_2 = control_utility.idle_verify_dump(node, return_path=True)

        self.logger.info(f'Path to dump_2 on {node.account.externally_routable_ip}={dump_2}')

        diff = node.account.ssh_output(f'diff {dump_1} {dump_2}', allow_fail=True)
        assert len(diff) != 0

        service.stop()

        service.rename_db(new_db_name='old_db')
        service.restore_from_snapshot(self.SNAPSHOT_NAME)

        service.restart()

        control_utility.activate()

        control_utility.idle_verify(check_assert=True)
        control_utility.validate_indexes(check_assert=True)
        dump_3 = control_utility.idle_verify_dump(node, return_path=True)

        self.logger.info(f'Path to dump_3 on {node.account.externally_routable_ip}={dump_3}')

        diff = node.account.ssh_output(f'diff {dump_1} {dump_3}', allow_fail=True)
        assert len(diff) == 0, diff

    def copy_service_logs(self, test_status):
        """
        Copy logs from service nodes to the results directory.
        If the the test failed, root directory will be collected too.
        """
        super().copy_service_logs(test_status=test_status)

        if test_status == FAIL:
            self.copy_ignite_root_dir()


def load(service_load: IgniteApplicationService, duration: int = 60):
    """
    Load.
    """
    service_load.start()
    try:
        service_load.await_stopped(duration)
    except (AssertionError, TimeoutError):
        service_load.stop()
