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
This module contains client queries tests.
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
from ignitetest.services.utils.nmon_utility import NmonUtility
from ignitetest.services.utils.ssl.client_connector_configuration import ClientConnectorConfiguration
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion


class JdbcThinBlobTest(IgniteTest):
    @cluster(num_nodes=4)
    @ignite_versions(str(DEV_BRANCH))
    @defaults(max_inmem=[None, 0, 2*1024*1024*1024], mode=["blob", "stream"], bias=[True, False])
    @parametrize(blob_size=1*1024*1024*1024, server_heap=9, insert_heap=6, select_heap=6)
    def test_jdbc_thin_blob(self, ignite_version, blob_size,
                            mode,
                            server_heap, insert_heap, select_heap,
                            max_inmem,
                            bias):
        """
        Thin JDBC test for Blobs.
        """
        cache_config = CacheConfiguration(name="WITH_STATISTICS_ENABLED*",
                                          statistics_enabled=True,
                                          backups=1,
                                          atomicity_mode="ATOMIC")

        server_config = IgniteConfiguration(version=IgniteVersion(ignite_version),
                                            client_connector_configuration=ClientConnectorConfiguration(),
                                            data_storage=DataStorageConfiguration(
                                                checkpoint_frequency=5000,
                                                metrics_enabled=True,
                                                wal_segment_size=blob_size + 1024,
                                                max_wal_archive_size=20 * (blob_size + 1024),
                                                default=DataRegionConfiguration(
                                                    persistence_enabled=True,
                                                    metrics_enabled=True,
                                                    initial_size=int(1.5 * blob_size),
                                                    max_size=int(1.5 * blob_size)
                                                )
                                            ),
                                            caches=[cache_config])

        jvm_opts = [f"-Xmx{server_heap}g",
                    f"-Xms{server_heap}g",
                    "-XX:+SafepointTimeout",
                    "-XX:SafepointTimeoutDelay=100",
                    "-XX:StartFlightRecording=dumponexit=true,"
                    "settings=/opt/ignite-dev/modules/ducktests/tests/jfr/profile-mem.jfc,"
                    "filename=/mnt/service/jfr/recording.jfr"]

        if not bias:
            jvm_opts.append("-XX:-UseBiasedLocking")

        ignite = IgniteService(self.test_context, server_config, 2,
                               jvm_opts=jvm_opts)

        ignite.start()

        nmon = NmonUtility(ignite)
        nmon.start(freq_sec=1)

        control_sh = ControlUtility(ignite)
        control_sh.activate()

        address = ignite.nodes[0].account.hostname + ":" + str(server_config.client_connector_configuration.port)

        cls = "org.apache.ignite.internal.ducktest.tests.jdbc.JdbcThinBlobTestApplication"

        client_insert = IgniteApplicationService(
            self.test_context,
            IgniteThinJdbcConfiguration(
                version=IgniteVersion(ignite_version),
                url=f"jdbc:ignite:thin://{address}" + (f"?maxInMemoryLobSize={max_inmem}" if max_inmem else "")
            ),
            java_class_name=cls,
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
            java_class_name=cls,
            num_nodes=1,
            jvm_opts=[f"-Xmx{select_heap}g", f"-Xms{select_heap}g"],
            params={
                "action": "select"
            })

        client_select.start()
        client_select.await_event("IGNITE_LOB_APPLICATION_DONE", 300, from_the_beginning=True)

        data = {
            "blob_size_gb": float("{:.2f}".format(int(client_select.extract_result("BLOB_SIZE")) / 1024 / 1024 / 1024)),
            "server_peak_heap_usage_gb": get_peak_memory_usage(ignite.nodes),
            "client_insert_peak_heap_usage_gb": get_peak_memory_usage(client_insert.nodes),
            "client_select_peak_heap_usage_gb": get_peak_memory_usage(client_select.nodes)
        }

        client_insert.stop()
        client_select.stop()

        state = control_sh.cluster_state()
        self.logger.info(state)

        online = [n for n in state.baseline if n.state == "ONLINE"]
        assert len(online) == 2

        nmon.stop()
        ignite.stop()

        return data


def get_peak_memory_usage(nodes):
    def node_peak_memory_usage(node):
        client = JmxClient(node)

        eden_mbean = client.find_mbean('.*G1 Eden Space,type=MemoryPool', domain="java.lang")
        old_mbean = client.find_mbean('.*G1 Old Gen,type=MemoryPool', domain="java.lang")
        survivor_mbean = client.find_mbean('.*G1 Survivor Space,type=MemoryPool', domain="java.lang")

        return float("{:.2f}".format((int(next(client.mbean_attribute(eden_mbean.name, 'PeakUsage.used'))) +
                                      int(next(client.mbean_attribute(old_mbean.name, 'PeakUsage.used'))) +
                                      int(next(client.mbean_attribute(survivor_mbean.name, 'PeakUsage.used')))) /
                                     1024 / 1024 / 1024))

    return {node.name: node_peak_memory_usage(node) for node in nodes}
