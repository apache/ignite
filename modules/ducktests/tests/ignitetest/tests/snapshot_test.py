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

import time
import re

from ducktape.mark.resource import cluster

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.services.utils.ignite_persistence import IgnitePersistenceAware
from ignitetest.utils import ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion


# pylint: disable=W0223
class SnapshotTest(IgniteTest):
    """
    Test Snapshot.
    """
    NUM_NODES = 3

    SNAPSHOT_NAME = "test_snap"

    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(DEV_BRANCH))
    def snapshot_test(self, ignite_version):
        """
        Test Snapshot.
        1. Start of ignite cluster.
        2. Activate cluster.
        3. Load.
        4. Idle verify dump1.
        5. Snapshot.
        6. Load.
        7. Idle verify dump2.
        8. Сhecking that dump1 and dump2 are differentю
        9. Stop ignite and replace db from snapshot
        10. Run ignite cluster.
        11. Idle verify dump3.
        12. Checking the equality of dump1 and dump2.
        """
        data_cfg = DataStorageConfiguration(default=DataRegionConfiguration(persistent=True))

        ignite_config = IgniteConfiguration(
            version=IgniteVersion(ignite_version),
            data_storage=data_cfg,
            snapshot_path=IgnitePersistenceAware.SNAPSHOT
        )

        service = IgniteService(self.test_context, ignite_config, num_nodes=self.NUM_NODES - 1)
        service.start()

        control_utility = ControlUtility(service, self.test_context)
        control_utility.activate()

        client_config = ignite_config._replace(client_mode=True, data_storage=None,
                                               discovery_spi=from_ignite_cluster(service, slice(0, self.NUM_NODES - 1)))

        streamer = IgniteApplicationService(
            self.test_context,
            client_config,
            java_class_name="org.apache.ignite.internal.ducktest.tests.UuidStreamerApplication",
            params={"cacheName": "test-cache"})

        streamer.start()

        load(streamer)

        node = service.nodes[0]

        check_idle_verify(control_utility)
        check_validate_indexes(control_utility)
        dump_1 = get_dump_path(control_utility, node)

        self.logger.warn("path to dump_1=" + dump_1)

        data = control_utility.snapshot_create(self.SNAPSHOT_NAME)

        self.logger.warn(data)

        load(streamer)

        dump_2 = get_dump_path(control_utility, node)

        self.logger.warn("path to dump_2=" + dump_2)

        diff = node.account.ssh_output(f'diff {dump_1} {dump_2}', allow_fail=True)
        assert len(diff) != 0, diff

        service.rename_db(new_db_name='old_db')
        service.copy_snap_to_db(self.SNAPSHOT_NAME)

        service.run()

        control_utility.activate()

        check_idle_verify(control_utility)
        check_validate_indexes(control_utility)
        dump_3 = get_dump_path(control_utility, node)

        self.logger.warn("path to dump_3=" + dump_2)

        diff = node.account.ssh_output(f'diff {dump_1} {dump_3}', allow_fail=True)
        assert len(diff) == 0, diff

    def tearDown(self):
        """
        Copy work directory to result.
        """
        self.copy_ignite_workdir()


def get_dump_path(control_utility: ControlUtility, node):
    """
    Control utils idle verify dump.
    @return: path to dump file on node.
    """
    data = control_utility.idle_verify_dump(node)
    assert 'VisorIdleVerifyDumpTask successfully' in data
    match = re.search(r'/.*.txt', data)
    return match[0]


def check_validate_indexes(control_utility: ControlUtility):
    """
    Check indexes.
    """
    data = control_utility.validate_indexes()
    assert 'no issues found.' in data, data


def check_idle_verify(control_utility: ControlUtility):
    """
    Check idle verify.
    """
    data = control_utility.idle_verify()
    assert 'idle_verify check has finished, no conflicts have been found.' in data, data


def load(service_load, duration: int = 30):
    """
    Load.
    """
    service_load.start()
    time.sleep(duration)
    service_load.stop()
