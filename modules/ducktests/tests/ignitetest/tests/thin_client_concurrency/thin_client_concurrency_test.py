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
from datetime import datetime, timedelta

from ducktape.mark import parametrize

from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration.data_storage import DataStorageConfiguration, \
    DataRegionConfiguration
from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.ssl.client_connector_configuration import ClientConnectorConfiguration
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion
from ignitetest.services.utils.jmx_utils import JmxClient
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, IgniteThinClientConfiguration
from ignitetest.services.utils.ignite_configuration.cache import CacheConfiguration
from ignitetest.services.ignite_execution_exception import IgniteExecutionException

SERVER_NODES_COUNT = 4


class ThinClientConcurrencyTest(IgniteTest):

    JAVA_GREEDY_THIN_CLIENT_CLASS_NAME = \
        "org.apache.ignite.internal.ducktest.tests.thin_client_test.ThinClientDataGenerationApplication"

    JAVA_THIN_CLIENT_CLASS_NAME = \
        "org.apache.ignite.internal.ducktest.tests.thin_client_test.ThinClientSelfTestApplication"

    @cluster(num_nodes=SERVER_NODES_COUNT+2)
    @ignite_versions(str(DEV_BRANCH))
    @parametrize(client_threads=10, entry_count=200_000, entry_size=300, batch_size=10_000, job_size=1,
                 backups=1, thread_pool_size=8, atomicity_mode='TRANSACTIONAL', jvm_opts=['-Xmx2G'])
    def test_one_greedy_thin_client(self, ignite_version, client_threads, entry_count, entry_size,
                                    batch_size, job_size, backups, thread_pool_size, atomicity_mode, jvm_opts):
        """
        Test shows that a single thin client using huge putAlls may take all resources of ignite node in a way that
        another thin client would fail to connect and will be kicked by timeout (10 secs by default).

        ****************************************************
        *  Test PASS means that the problem is reproduced. *
        ****************************************************

        On server side WARN like "Unable to perform handshake within timeout [timeout=10000" is logged.

        On client side the "org.apache.ignite.client.ClientConnectionException: Channel is closed" raised in the
        Ignition::startClient().
        """

        addresses, ignite = self.__start_cluster(atomicity_mode, backups, entry_count, entry_size, ignite_version,
                                                 jvm_opts, thread_pool_size, SERVER_NODES_COUNT)

        app1 = IgniteApplicationService(
            self.test_context,
            config=IgniteThinClientConfiguration(addresses=addresses),
            java_class_name=self.JAVA_GREEDY_THIN_CLIENT_CLASS_NAME,
            jvm_opts=jvm_opts
        )
        app1.log_level = "DEBUG"
        app1.shutdown_timeout_sec = 3600

        app1.params = {
            "cacheName": "TEST-CACHE",
            "entrySize": entry_size,
            "threads": client_threads,
            "timeoutSecs": 3600,
            "from": 0,
            "to": entry_count,
            "batchSize": batch_size,
            "batchesPerTask": job_size,
            "clientPerTask": False
        }
        app1.start_async()

        # wait for queue to fill
        self.__await_thin_executor_task_count_reached(ignite.nodes[0], 30)

        app2 = IgniteApplicationService(
            self.test_context,
            config=IgniteThinClientConfiguration(addresses=addresses),
            java_class_name=self.JAVA_THIN_CLIENT_CLASS_NAME,
            jvm_opts=jvm_opts
        )

        # thin client in app2 would fail to connect by timeout, because of high load from app1
        app2.start_async()

        app1.await_stopped()

        try:
            app2.await_stopped()
        except IgniteExecutionException as ex:
            assert "org.apache.ignite.client.ClientConnectionException: Channel is closed" in str(ex)

        ignite.await_event_on_node("Unable to perform handshake within timeout", ignite.nodes[0],
                                   timeout_sec=.1, from_the_beginning=True)

        return {}

    @cluster(num_nodes=SERVER_NODES_COUNT+8)
    @ignite_versions(str(DEV_BRANCH))
    @parametrize(clients=8, client_threads=4, entry_count=15_000_000, entry_size=300, batch_size=100_000, job_size=1,
                 backups=1, thread_pool_size=8, atomicity_mode='ATOMIC', jvm_opts=['-Xmx3G'])
    @parametrize(clients=4, client_threads=4, entry_count=250_000, entry_size=300, batch_size=10_000, job_size=1,
                 backups=1, thread_pool_size=8, atomicity_mode='TRANSACTIONAL', jvm_opts=['-Xmx2G'])
    def test_many_thin_clients(self, ignite_version, clients, client_threads, entry_count, entry_size,
                               batch_size, job_size, backups, thread_pool_size, atomicity_mode, jvm_opts):
        """
        Test demonstrates that several concurrent thin clients using huge putAlls may produce a load which would
        prevent new clients connections.

        ****************************************************
        *  Test PASS means that the problem is reproduced. *
        ****************************************************

        On server side WARN like "Unable to perform handshake within timeout [timeout=10000" is logged.

        On client side the "org.apache.ignite.client.ClientConnectionException: Channel is closed" raised in the
        Ignition::startClient().
        """

        addresses, ignite = self.__start_cluster(atomicity_mode, backups, entry_count, entry_size, ignite_version,
                                                 jvm_opts, thread_pool_size, SERVER_NODES_COUNT)

        apps = []
        for _ in range(clients):
            _app = IgniteApplicationService(
                self.test_context,
                config=IgniteThinClientConfiguration(addresses=addresses),
                java_class_name=self.JAVA_GREEDY_THIN_CLIENT_CLASS_NAME,
                jvm_opts=jvm_opts
            )
            _app.log_level = "DEBUG"
            apps.append(_app)

        from_key = 0
        to_key = entry_count

        count = int((to_key - from_key) / clients)
        end = from_key

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
                "timeoutSecs": 3600,
                "from": start,
                "to": end,
                "batchSize": batch_size,
                "batchesPerTask": job_size,
                "clientPerTask": True
            }
            _app.shutdown_timeout_sec = 3600

            worker = threading.Thread(
                name="thin-starter-worker-" + str(start),
                target=_app.start_async
            )
            worker.daemon = True
            worker.start()

            workers.append(worker)

        for worker in workers:
            worker.join()

        for _app in apps:
            try:
                _app.await_stopped()
            except IgniteExecutionException:
                pass

        ignite.await_event_on_node("Unable to perform handshake within timeout", ignite.nodes[0],
                                   timeout_sec=.1, from_the_beginning=True)

        return {}

    def __start_cluster(self, atomicity_mode, backups, entry_count, entry_size, ignite_version, jvm_opts,
                        thread_pool_size, num_nodes):
        version = IgniteVersion(ignite_version)
        region_size = self.__data_region_size(int(entry_count * entry_size * (backups + 1) * 1.5 / num_nodes))
        ignite_config = IgniteConfiguration(
            version=version,
            client_connector_configuration=ClientConnectorConfiguration(
                thread_pool_size=thread_pool_size
            ),
            metric_exporters={"org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi"},
            data_storage=DataStorageConfiguration(
                max_wal_archive_size=2 * region_size,
                default=DataRegionConfiguration(persistent=True,
                                                max_size=region_size)),
            caches=[CacheConfiguration(
                name='TEST-CACHE',
                backups=backups,
                atomicity_mode=atomicity_mode
            )]
        )
        ignite = IgniteService(self.test_context, ignite_config, num_nodes=num_nodes, jvm_opts=jvm_opts)
        ignite.start()
        control_utility = ControlUtility(ignite)
        control_utility.activate()
        addresses = ignite.nodes[0].account.hostname + ":" + str(ignite_config.client_connector_configuration.port)
        return addresses, ignite

    def __await_thin_executor_task_count_reached(self, node, value, timeout_sec=60):

        delta_time = datetime.now() + timedelta(seconds=timeout_sec)

        reached = False

        mbean = JmxClient(node).find_mbean(".*name=GridThinClientExecutor")
        mbean.name = mbean.name.replace('"Thread Pools"', '"Thread\\ Pools"')
        while datetime.now() < delta_time and not reached:
            count = int(next(mbean.TaskCount))
            self.logger.info(f"TaskCount: {count}")
            reached = count >= value

        if reached:
            return

        raise TimeoutError(f'TaskCount was not reached within the time: {timeout_sec} seconds.')

    DATA_REGION_SIZE_LIMIT_KEY_NAME = "data_region_size_limit"
    DATA_REGION_SIZE_DEFAULT_LIMIT = 2 * 1024 * 1024 * 1024

    def __data_region_size(self, required_size):
        return max(100 * 1024 * 1024,
                   min(self._global_int(self.DATA_REGION_SIZE_LIMIT_KEY_NAME, self.DATA_REGION_SIZE_DEFAULT_LIMIT),
                       required_size))
