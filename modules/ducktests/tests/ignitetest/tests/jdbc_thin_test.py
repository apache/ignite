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
This module contains Thin JDBC driver tests.
"""
from ducktape.mark import parametrize, defaults

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, IgniteThinJdbcConfiguration
from ignitetest.services.utils.ignite_configuration.cache import CacheConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration, \
    DataStorageConfiguration
from ignitetest.services.utils.jmx_utils import JmxClient
from ignitetest.services.utils.ssl.client_connector_configuration import ClientConnectorConfiguration
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, LATEST, IgniteVersion


class JdbcThinTest(IgniteTest):
    """
    Thin JDBC driver test.
    """
    @cluster(num_nodes=2)
    @ignite_versions(str(DEV_BRANCH), str(LATEST), version_prefix="server_version")
    @ignite_versions(str(DEV_BRANCH), str(LATEST), version_prefix="thin_jdbc_version")
    def test_simple_insert_select(self, server_version, thin_jdbc_version):
        """
        Smoke test ensuring the Thin JDBC driver just works doing simple SQL queries
        and that the compationility between Ignite versions is preserved.
        """
        server_config = IgniteConfiguration(
            version=IgniteVersion(server_version),
            client_connector_configuration=ClientConnectorConfiguration())

        ignite = IgniteService(self.test_context, server_config, 1)

        ignite.start()

        ControlUtility(ignite).activate()

        address = ignite.nodes[0].account.hostname + ":" + str(server_config.client_connector_configuration.port)

        app = IgniteApplicationService(
            self.test_context,
            IgniteThinJdbcConfiguration(
                version=IgniteVersion(thin_jdbc_version),
                addresses=[address]
            ),
            java_class_name="org.apache.ignite.internal.ducktest.tests.jdbc.JdbcThinSelfTestApplication",
            num_nodes=1)

        app.start()
        app.await_stopped()

        ignite.stop()

    @cluster(num_nodes=4)
    @ignite_versions(str(DEV_BRANCH))
    @defaults(mode=["blob", "stream"])
    @parametrize(blob_size=512*1024*1024, server_heap=6, insert_heap=3, select_heap=1)
    def test_blob(self, ignite_version, blob_size, mode, server_heap, insert_heap, select_heap):
        """
        Thin JDBC test for Blobs.
        Measures heap memeory used to insert and select a single blob both on server and client sides.
        """
        cache_config = CacheConfiguration(
            name="WITH_STATISTICS_ENABLED*",
            statistics_enabled=True,
            backups=1,
            atomicity_mode="ATOMIC")

        server_config = IgniteConfiguration(
            version=IgniteVersion(ignite_version),
            client_connector_configuration=ClientConnectorConfiguration(),
            data_storage=DataStorageConfiguration(
                metrics_enabled=True,
                wal_segment_size=blob_size + 1024,
                max_wal_archive_size=5 * (blob_size + 1024),
                default=DataRegionConfiguration(
                    persistence_enabled=True,
                    metrics_enabled=True,
                    initial_size=int(1.5 * blob_size),
                    max_size=int(1.5 * blob_size)
                )),
            caches=[cache_config])

        ignite = IgniteService(self.test_context, server_config, 2,
                               jvm_opts=[f"-Xmx{server_heap}g", f"-Xms{server_heap}g"])

        ignite.start()

        control_sh = ControlUtility(ignite)
        control_sh.activate()

        java_client_class_name = "org.apache.ignite.internal.ducktest.tests.jdbc.JdbcThinBlobTestApplication"

        address = ignite.nodes[0].account.hostname + ":" + str(server_config.client_connector_configuration.port)

        client_insert = IgniteApplicationService(
            self.test_context,
            IgniteThinJdbcConfiguration(
                version=IgniteVersion(ignite_version),
                url=f"jdbc:ignite:thin://{address}"
            ),
            java_class_name=java_client_class_name,
            num_nodes=1,
            jvm_opts=[f"-Xmx{insert_heap}g", f"-Xms{insert_heap}g"],
            params={
                "blob_size": blob_size,
                "action": "insert",
                "mode": mode
            })

        client_insert.start()
        client_insert.await_event("IGNITE_LOB_APPLICATION_DONE", 300, from_the_beginning=True)

        client_select = IgniteApplicationService(
            self.test_context,
            IgniteThinJdbcConfiguration(
                version=IgniteVersion(ignite_version),
                url=f"jdbc:ignite:thin://{address}"
            ),
            java_class_name=java_client_class_name,
            num_nodes=1,
            jvm_opts=[f"-Xmx{select_heap}g", f"-Xms{select_heap}g"],
            params={
                "action": "select"
            })

        client_select.start()
        client_select.await_event("IGNITE_LOB_APPLICATION_DONE", 300, from_the_beginning=True)

        selected_blob_size = int(client_select.extract_result("BLOB_SIZE"))
        assert selected_blob_size == blob_size

        data = {
            "blob_size_gb": blob_size / 1024 / 1024 / 1024,
            "server_peak_heap_usage_gb": get_peak_heap_memory_usage(ignite.nodes),
            "client_insert_peak_heap_usage_gb": get_peak_heap_memory_usage(client_insert.nodes),
            "client_select_peak_heap_usage_gb": get_peak_heap_memory_usage(client_select.nodes)
        }

        client_insert.stop()
        client_select.stop()

        online_ignite_nodes = [n for n in control_sh.cluster_state().baseline if n.state == "ONLINE"]
        assert len(online_ignite_nodes) == 2

        ignite.stop()

        return data


def get_peak_heap_memory_usage(nodes):
    """
    Reads the peak heap memory usage of the given nodes via the Jmx.
    """
    def node_peak_heap_memory_usage(node):
        client = JmxClient(node)

        eden_mbean = client.find_mbean('.*G1 Eden Space,type=MemoryPool', domain="java.lang")
        old_mbean = client.find_mbean('.*G1 Old Gen,type=MemoryPool', domain="java.lang")
        survivor_mbean = client.find_mbean('.*G1 Survivor Space,type=MemoryPool', domain="java.lang")

        return float("{:.2f}".format((int(next(client.mbean_attribute(eden_mbean.name, 'PeakUsage.used'))) +
                                      int(next(client.mbean_attribute(old_mbean.name, 'PeakUsage.used'))) +
                                      int(next(client.mbean_attribute(survivor_mbean.name, 'PeakUsage.used')))) /
                                     1024 / 1024 / 1024))

    return {node.name: node_peak_heap_memory_usage(node) for node in nodes}
