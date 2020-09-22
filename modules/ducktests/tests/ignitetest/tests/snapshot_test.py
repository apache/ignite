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
    1. Start of ignite cluster.
    2. Activate cluster.
    3. Load.
    """
    NUM_NODES = 3

    SNAPSHOT_NAME = "test_snap"

    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(DEV_BRANCH))
    def snapshot_test(self, ignite_version):
        data_cfg = DataStorageConfiguration(default=DataRegionConfiguration(persistent=True))

        ignite_config = IgniteConfiguration(
            version=IgniteVersion(ignite_version),
            data_storage=data_cfg,
            snapshot_path=IgnitePersistenceAware.SNAPSHOT
        )

        service = IgniteService(self.test_context, ignite_config, num_nodes=self.NUM_NODES - 1)

        control_utility = ControlUtility(service, self.test_context)

        service.start()

        control_utility.activate()

        node = service.nodes[0]

        whoami = node.account.ssh_output('whoami', allow_fail=True)
        self.logger.warn("whoami=" + str(whoami))
        wd = node.account.ssh_output(f'ls -al {service.WORK_DIR}', allow_fail=True)
        self.logger.warn("WORK_DIR=" + str(wd))

        data = control_utility.cluster_state()

        self.logger.warn("state")
        self.logger.warn(data)

        client_config = ignite_config._replace(client_mode=True, data_storage=None,
                                               discovery_spi=from_ignite_cluster(service, slice(0, self.NUM_NODES - 1)))

        single_key_tx_streamer = IgniteApplicationService(
            self.test_context,
            client_config,
            java_class_name="org.apache.ignite.internal.ducktest.tests.pme_free_switch_test."
                            "SingleKeyTxStreamerApplication",
            params={"cacheName": "test-cache", "warmup": 1000})

        single_key_tx_streamer.start()

        time.sleep(30)

        single_key_tx_streamer.stop()

        data = control_utility.cluster_state()

        self.logger.warn("state")
        self.logger.warn(data)

        idle = control_utility.idle_verify()

        self.logger.warn("idle_verify")
        self.logger.warn(idle)

        data = control_utility.idle_verify_dump(node)

        dump_1 = get_dump_path(data)

        self.logger.warn("path to dump_1=" + dump_1)

        check_validate_index(control_utility)

        data = control_utility.snapshot_create(self.SNAPSHOT_NAME)

        self.logger.warn("snapshot")
        self.logger.warn(data)

        single_key_tx_streamer.start()

        time.sleep(30)

        single_key_tx_streamer.stop()

        data = control_utility.idle_verify_dump(node)

        dump_2 = get_dump_path(data)

        self.logger.warn("path to dump_2=" + dump_2)

        diff = node.account.ssh_output(f'diff {dump_1} {dump_2}', allow_fail=True)

        assert len(diff) != 0, diff

        # service.stop()

        service.rename_db(new_db_name='old_db')

        service.copy_snap_to_db(self.SNAPSHOT_NAME)

        service.run()

        control_utility.activate()

        data = control_utility.idle_verify_dump(node)

        dump_3 = get_dump_path(data)

        self.logger.warn("path to dump_3=" + dump_2)

        diff = node.account.ssh_output(f'diff {dump_1} {dump_3}', allow_fail=True)

        assert len(diff) == 0, diff

        check_validate_index(control_utility)

    def tearDown(self):
        self.copy_ignite_workdir()


def get_dump_path(data):
    assert 'VisorIdleVerifyDumpTask successfully' in data

    match = re.search(r'/.*.txt', data)

    return match[0]


def check_validate_index(control_utility):
    data = control_utility.validate_indexes()
    assert 'no issues found.' in data

def check_validate_index(control_utility):
    data = control_utility.validate_indexes()
    assert 'no issues found.' in data
