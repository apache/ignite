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
import subprocess

from ducktape.mark.resource import cluster

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.services.utils.path import get_home_dir
from ignitetest.utils import ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import IgniteVersion, DEV_BRANCH, LATEST_2_9


# pylint: disable=W0223
class SnapshotTest(IgniteTest):
    """
    Test Snapshot.
    """
    SNAPSHOT_NAME = "test_snapshot"

    CACHE_NAME = "TEST_CACHE"

    @cluster(num_nodes=3)
    @ignite_versions(
        str(DEV_BRANCH),
        str(LATEST_2_9)
    )
    def snapshot_test(self, ignite_version):
        """
        Basic snapshot test.
        """

        version = IgniteVersion(ignite_version)

        # if version == DEV_BRANCH:
        #     # Long execution of index validation in the presence of all modules in classpath on DEV_BRANCH version.
        #     # https://issues.apache.org/jira/browse/IGNITE-14147
        #     excluded_modules = get_other_modules(
        #         self.test_context.globals, 'core', 'spring', 'indexing', 'log4j', 'ducktests')
        #     envs = {'EXCLUDE_MODULES': excluded_modules}
        # else:
        #     envs = {}

        ignite_config = IgniteConfiguration(
            version=version,
            data_storage=DataStorageConfiguration(default=DataRegionConfiguration(persistent=True)),
            metric_exporter='org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi'
        )

        service = IgniteService(self.test_context, ignite_config, num_nodes=len(self.test_context.cluster) - 2,
                                startup_timeout_sec=180, jvm_opts="-Dtest1")
        service.start()

        control_utility = ControlUtility(service)
        control_utility.activate()

        client_config = IgniteConfiguration(
            client_mode=True,
            version=version,
            discovery_spi=from_ignite_cluster(service)
        )

        loader1 = IgniteApplicationService(
            self.test_context,
            client_config,
            java_class_name="org.apache.ignite.internal.ducktest.tests.snapshot_test.DataLoaderApplication",
            startup_timeout_sec=180,
            shutdown_timeout_sec=300,
            jvm_opts="-Dtest2",
            # envs=envs,
            params={
                "cacheName": self.CACHE_NAME,
                "interval": 500_000,
                "valueSizeKb": 1
            }
        )

        loader2 = IgniteApplicationService(
            self.test_context,
            client_config,
            java_class_name="org.apache.ignite.internal.ducktest.tests.snapshot_test.DataLoaderApplication",
            startup_timeout_sec=180,
            shutdown_timeout_sec=300,
            jvm_opts="-Dtest3",
            # envs=envs,
            params={
                "start": 500_000,
                "cacheName": self.CACHE_NAME,
                "interval": 100_000,
                "valueSizeKb": 1
            }
        )

        loader1.run()

        control_utility.validate_indexes()
        control_utility.idle_verify()
        node = service.nodes[0]

        dump_1 = control_utility.idle_verify_dump(node)

        control_utility.snapshot_create(self.SNAPSHOT_NAME)

        loader2.run()

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

#
# def get_other_modules(globals: dict, *module: str) -> str:
#     """
#     :param globals: Globals parameters.
#     :param module: Module name.
#     :return Other modules.
#     """
#
#     install_root = globals.get("install_root", "/opt")
#
#     modules_dir = os.path.join(get_home_dir(install_root, "ignite", DEV_BRANCH), "modules")
#
#     modules = '|'.join(module)
#
#     res = subprocess.run(f'cd {modules_dir} ; ls | grep -Ev \'{modules}\' | tr \'\n\' \',\'',
#                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, text=True, shell=True)
#
#     return res.stdout
