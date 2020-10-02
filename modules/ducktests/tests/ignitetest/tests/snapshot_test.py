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

import re
import time
from datetime import datetime, timedelta
from ducktape.mark.resource import cluster
from ducktape.tests.status import FAIL

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.services.utils.ignite_persistence import IgnitePersistenceAware
from ignitetest.services.utils.jmx_utils import JmxClient
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
        4. Idle verify dump1.
        5. Snapshot.
        6. Load.
        7. Idle verify dump2.
        8. Сhecking that dump1 and dump2 are different.
        9. Stop ignite and replace db from snapshot.
        10. Run ignite cluster.
        11. Idle verify dump3.
        12. Checking the equality of dump1 and dump3.
        """
        data_cfg = DataStorageConfiguration(default=DataRegionConfiguration(persistent=True))

        ignite_config = IgniteConfiguration(
            version=IgniteVersion(ignite_version),
            data_storage=data_cfg,
        )

        service = IgniteService(self.test_context, ignite_config, num_nodes=self.NUM_NODES - 1)
        service.start()

        control_utility = ControlUtility(service, self.test_context)
        control_utility.activate()

        client_config = ignite_config._replace(client_mode=True, data_storage=None)

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

        check_idle_verify(control_utility)
        check_validate_indexes(control_utility)
        dump_1 = get_dump_path(control_utility, node)

        self.logger.warn(f'Path to dump_1 on {node.account.externally_routable_ip}={dump_1}')

        data = control_utility.snapshot_create(self.SNAPSHOT_NAME)

        self.logger.debug(data)

        await_snapshot(service, logger=self.logger)

        print_snapshot_size(service, self.SNAPSHOT_NAME, self.logger)

        load(streamer)

        dump_2 = get_dump_path(control_utility, node)

        self.logger.warn(f'Path to dump_2 on {node.account.externally_routable_ip}={dump_2}')

        diff = node.account.ssh_output(f'diff {dump_1} {dump_2}', allow_fail=True)
        assert len(diff) != 0

        service.stop()

        service.rename_db(new_db_name='old_db')
        service.copy_snap_to_db(self.SNAPSHOT_NAME)

        service.restart()

        control_utility.activate()

        check_idle_verify(control_utility)
        check_validate_indexes(control_utility)
        dump_3 = get_dump_path(control_utility, node)

        self.logger.warn(f'Path to dump_3 on {node.account.externally_routable_ip}={dump_3}')

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


def load(service_load: IgniteApplicationService, duration: int = 60):
    """
    Load.
    """
    service_load.start()
    try:
        service_load.await_stopped(duration)
    except AssertionError:
        service_load.stop()


def await_snapshot(service: IgniteApplicationService, time_out=60, logger=None):
    """
    Waiting for the snapshot to complete.
    """
    delta_time = datetime.now() + timedelta(seconds=time_out)

    while datetime.now() < delta_time:
        for node in service.nodes:
            mbean = JmxClient(node).find_mbean('snapshot')
            star_time = int(list(mbean.__getattr__('LastSnapshotStartTime'))[0])
            end_time = int(list(mbean.__getattr__('LastSnapshotEndTime'))[0])
            err_msg = list(mbean.__getattr__('LastSnapshotErrorMessage'))[0]

            if logger is not None:
                logger.debug(f'Hostname={node.account.hostname}, '
                             f'LastSnapshotStartTime={star_time}, '
                             f'LastSnapshotEndTime={end_time}, '
                             f'LastSnapshotErrorMessage={err_msg}'
                             )

            if (0 < star_time < end_time) & (err_msg == ''):
                return

        time.sleep(1)

    raise TimeoutError(f'LastSnapshotStartTime={star_time}, '
                       f'LastSnapshotEndTime={end_time}, '
                       f'LastSnapshotErrorMessage={err_msg}')


def print_snapshot_size(service: IgniteApplicationService, snapshot_name: str, logger):
    """
    Print size snapshot dir on service nodes.
    """
    res = service.ssh_output_all(f'du -hs {IgnitePersistenceAware.SNAPSHOT}/{snapshot_name} | ' + "awk '{print $1}'")
    for items in res.items():
        data = items[1].decode("utf-8")
        logger.info(f'Snapshot {snapshot_name} on {items[0]}: {data}')
