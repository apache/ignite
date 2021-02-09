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
from ducktape.mark.resource import cluster

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
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

    @cluster(num_nodes=4)
    @ignite_versions(
        str(DEV_BRANCH),
        # str(LATEST_2_9)
    )
    def snapshot_test(self, ignite_version):
        """
        Basic snapshot test.
        """
        exclude_modules = [
            "aop", "apache-license-gen", "benchmarks", "camel", "cassandra", "clients", "cloud",
            "codegen", "compatibility", "compress", "dev-utils", "direct-io", "extdata", "flink",
            "flume", "gce", "geospatial", "hadoop", "hibernate-4.2", "hibernate-5.1", "hibernate-5.3",
            "ignored-tests", "jcl", "jms11", "jta", "kafka", "kubernetes", "log4j2", "mesos", "ml",
            "mqtt", "opencensus", "osgi", "osgi-karaf", "osgi-paxlogging", "platforms", "rest-http",
            "rocketmq", "scalar", "scalar-2.10", "schedule", "slf4j", "spark", "spark-2.4", "sqlline",
            "ssh", "storm", "tools", "twitter", "urideploy", "visor-console", "visor-console-2.10",
            "visor-plugins", "web", "yardstick", "yarn", "zeromq"
        ]

        envs = {'EXCLUDE_MODULES': ','.join(exclude_modules)}

        ignite_config = IgniteConfiguration(
            version=IgniteVersion(ignite_version),
            data_storage=DataStorageConfiguration(default=DataRegionConfiguration(persistent=True)),
            metric_exporter='org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi'
        )

        service = IgniteService(self.test_context, ignite_config, num_nodes=len(self.test_context.cluster) - 1,
                                envs=envs)
        service.start()

        control_utility = ControlUtility(service)
        control_utility.activate()

        client_config = IgniteConfiguration(
            client_mode=True,
            version=IgniteVersion(ignite_version),
            discovery_spi=from_ignite_cluster(service)
        )

        loader = IgniteApplicationService(
            self.test_context,
            client_config,
            java_class_name="org.apache.ignite.internal.ducktest.tests.snapshot_test.DataLoaderApplication",
            shutdown_timeout_sec=300,
            envs=envs,
            params={
                "cacheName": self.CACHE_NAME,
                "interval": 500_000,
                "dataSizeKB": 1
            }
        )

        loader.run()

        control_utility.validate_indexes()
        control_utility.idle_verify()
        node = service.nodes[0]

        dump_1 = control_utility.idle_verify_dump(node)

        control_utility.snapshot_create(self.SNAPSHOT_NAME)

        loader.run()

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
