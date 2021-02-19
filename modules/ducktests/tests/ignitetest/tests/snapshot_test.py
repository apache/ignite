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
import os

from ducktape.mark.resource import cluster

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.utils import ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import IgniteVersion, LATEST_2_9


# pylint: disable=W0223
class SnapshotTest(IgniteTest):
    """
    Test Snapshot.
    """
    SNAPSHOT_NAME = "test_snapshot"

    CACHE_NAME = "TEST_CACHE"

    @cluster(num_nodes=4)
    @ignite_versions(str(LATEST_2_9))
    def snapshot_test(self, ignite_version):
        """
        Basic snapshot test.
        """
        version = IgniteVersion(ignite_version)

        ignite_config = IgniteConfiguration(
            version=version,
            data_storage=DataStorageConfiguration(default=DataRegionConfiguration(persistent=True)),
            metric_exporter='org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi'
        )

        service = IgniteService(self.test_context, ignite_config, num_nodes=len(self.test_context.cluster) - 1)
        service.start()

        control_utility = ControlUtility(service)
        control_utility.activate()

        client_config = IgniteConfiguration(
            client_mode=True,
            version=version,
            discovery_spi=from_ignite_cluster(service)
        )

        loader = IgniteApplicationService(
            self.test_context,
            client_config,
            java_class_name="org.apache.ignite.internal.ducktest.tests.snapshot_test.DataLoaderApplication",
            params={
                "start": 0,
                "cacheName": self.CACHE_NAME,
                "interval": 500_000,
                "valueSizeKb": 1
            }
        )

        loader.run()
        loader.free()

        control_utility.validate_indexes()
        control_utility.idle_verify()
        node = service.nodes[0]

        dump_1 = control_utility.idle_verify_dump(node)

        index_file = os.path.join(service.database_dir, node.account.hostname.replace('-', '_').replace('.', '_'),
                                  f'cache-{self.CACHE_NAME}', 'index.bin')
        assert next(node.account.ssh_capture(f'du -m  {index_file} | cut -f1', callback=int)) > 30

        control_utility.snapshot_create(self.SNAPSHOT_NAME)

        diff_cache_group_on_nodes_with_snapshot(service, self.SNAPSHOT_NAME, self.CACHE_NAME)

        loader = IgniteApplicationService(
            self.test_context,
            client_config,
            java_class_name="org.apache.ignite.internal.ducktest.tests.snapshot_test.DataLoaderApplication",
            params={
                "start": 500_000,
                "cacheName": self.CACHE_NAME,
                "interval": 100_000,
                "valueSizeKb": 1
            }
        )

        loader.start(clean=False)
        loader.wait()

        dump_2 = control_utility.idle_verify_dump(node)

        diff = node.account.ssh_output(f'diff {dump_1} {dump_2}', allow_fail=True)
        assert len(diff) != 0

        service.stop()

        service.restore_from_snapshot(self.SNAPSHOT_NAME)

        service.start(clean=False)

        control_utility.activate()

        control_utility.validate_indexes()
        control_utility.idle_verify()

        dump_3 = control_utility.idle_verify_dump(node)

        diff = node.account.ssh_output(f'diff {dump_1} {dump_3}', allow_fail=True)
        assert len(diff) == 0, diff


def diff_cache_group_on_nodes_with_snapshot(service, snapshot_name: str, cache_group_name: str, ) -> None:
    """
    :param service: Service.
    :param snapshot_name: Name of snapshot.
    :param cache_group_name: Name of cache group.
    """
    cache_group = f'cache-{cache_group_name}'

    for node in service.nodes:
        snapshot_cg_dir = os.path.join(service.snapshots_dir, snapshot_name, 'db', node.account.hostname, cache_group)
        service_cg_dir = os.path.join(service.database_dir, node.account.hostname, cache_group)

        node.account.ssh(f'diff -r {snapshot_cg_dir} {service_cg_dir}')
