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
This module contains client tests.
"""
import threading
import uuid

from ducktape.mark import defaults, parametrize
from ignitetest.services.zk.zookeeper import ZookeeperSettings, ZookeeperService

from ignitetest.services.utils.control_utility import ControlUtility

from ignitetest.services.utils.ignite_configuration.data_storage import DataStorageConfiguration, \
    DataRegionConfiguration

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, IgniteThinClientConfiguration
from ignitetest.services.utils.ssl.client_connector_configuration import ClientConnectorConfiguration
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, LATEST, IgniteVersion
from ignitetest.utils.data_loader.data_loader import DataLoadParams, DataLoader, data_region_size

SERVER_NODES = 4
PRELOAD_NODES = 16
DATA_REGION_SIZE = 10 * 1024 * 1024 * 1024


class ManyThinClientTest(IgniteTest):

    JAVA_CLIENT_CLASS_NAME = "org.apache.ignite.internal.ducktest.tests.thin_client_test.ThinClientDataGenerationApplication"

    @cluster(num_nodes=SERVER_NODES + PRELOAD_NODES + 1)
    # @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @ignite_versions(str(DEV_BRANCH))
    @defaults(preloaders=[8], preloader_threads=[2], jvm_opts=[['-Xmx1G']])
    @parametrize(clients=20, client_threads=8, entry_count=60_000_000, entry_size=300, batch_size=50_000, job_size=3)
    def test_many_thin_clients(self, ignite_version, clients, client_threads, entry_count, entry_size,
                               preloaders, preloader_threads, jvm_opts, batch_size, job_size):
        """
        Many thin writing clients connections test.
        """

        data_load_params = DataLoadParams(cache_names=["TEST-CACHE"],
                                          entry_count=entry_count, entry_size=entry_size,
                                          preloaders=preloaders, threads=preloader_threads, jvm_opts=jvm_opts)
        loader = DataLoader(self.test_context, data_load_params)

        version = IgniteVersion(ignite_version)

        num_nodes = 4
        region_size = data_region_size(self, int(data_load_params.data_size / num_nodes))
        self.logger.info(f"region size: {region_size}")
        ignite_config = IgniteConfiguration(
            version=version,
            client_connector_configuration=ClientConnectorConfiguration(),
            data_storage=DataStorageConfiguration(
                max_wal_archive_size=2 * region_size,
                default=DataRegionConfiguration(persistent=True,
                                                max_size=region_size)),
            metric_exporters={"org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi"}
        )

        ignite = IgniteService(self.test_context, ignite_config, num_nodes=num_nodes, jvm_opts=['-Xmx3G'])
        ignite.start()

        control_utility = ControlUtility(ignite)
        control_utility.activate()

        loader.load_data(ignite)
        loader.free()

        zk = start_zookeeper(self.test_context, 1, 1000)

        addresses = ignite.nodes[0].account.hostname + ":" + str(ignite_config.client_connector_configuration.port)

        apps = []
        for _ in range(clients):
            _app = IgniteApplicationService(
                self.test_context,
                config=IgniteThinClientConfiguration(addresses=addresses),
                java_class_name=self.JAVA_CLIENT_CLASS_NAME
            )
            _app.log_level = "DEBUG"
            apps.append(_app)

        from_key = 0
        to_key = entry_count

        count = int((to_key - from_key) / clients)
        end = from_key

        token = str(uuid.uuid4())

        workers = []
        for _app in apps:
            start = end
            end += count
            if end > to_key:
                end = to_key

            _app.params = {
                "cacheName": "TEST-CACHE",
                "entrySize": entry_size,
                "threads": client_threads,
                "preloaders": clients,
                "preloadersToken": token,
                "timeoutSecs": 3600,
                "from": start,
                "to": end,
                "batchSize": batch_size,
                "jobSize": job_size,
                "zookeeperConnectionString": zk.connection_string()
            }
            _app.shutdown_timeout_sec = 3600
            # _app.jvm_opts = self.data_load_params.jvm_opts

            worker = threading.Thread(
                name="thin-starter-worker-" + str(start),
                target=_app.start_async
            )
            worker.daemon = True
            worker.start()

            workers.append(worker)
            # _app.start_async()

        for worker in workers:
            worker.join()

        for _app in apps:
            _app.await_stopped()

        return {}


def start_zookeeper(test_context, num_nodes, failure_detection_timeout):
    """
    Start zookeeper cluster.
    """
    zk_settings = ZookeeperSettings(min_session_timeout=failure_detection_timeout)

    zk_quorum = ZookeeperService(test_context, num_nodes, settings=zk_settings)
    zk_quorum.start_async()
    return zk_quorum
