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
Stops a baseline server of a persistent cluster under transactional load and returns it.
"""
import re
import signal
import time

from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.mdc.mdc_cluster import LOAD_APP
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.cache import CacheConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.services.utils.load_progress import load_progress, load_window
from ignitetest.tests.rebalance.util import await_rebalance_start, check_type_of_rebalancing
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion

CACHE = "node-return"

LOCK_CACHE = "node-return-locks"

# Load before the stop, while the node is away (longer than the failure detection timeout), after the join.
BEFORE_SEC = 10
AWAY_SEC = 30
AFTER_SEC = 20

# Bounds, calibrated on master (the ticket's notes): the longest load stall while the node is away, per stop
# signal, and the duration of the join's exchange.
AWAY_STALL_MS = {"SIGTERM": 60_000, "SIGKILL": 60_000, "SIGSEGV": 60_000}
JOIN_EXCHANGE_MS = 60_000


class NodeReturnTest(IgniteTest):
    """
    The load keeps going while a server is away; the server catches up from its WAL and the load has no errors after
    it joins.
    """
    @cluster(num_nodes=4)
    @ignite_versions(str(DEV_BRANCH))
    @matrix(stop=["SIGTERM", "SIGKILL", "SIGSEGV"], explicit_locks=[False])
    @matrix(stop=["SIGTERM"], explicit_locks=[True])
    def test_node_return(self, ignite_version, stop, explicit_locks):
        """
        Stops the server by the signal and starts it again. With explicit locks every load transaction runs under an
        explicit lock, so the exchanges meet a held lock.
        """
        caches = [CacheConfiguration(name=name, atomicity_mode="TRANSACTIONAL", backups=1)
                  for name in (CACHE, LOCK_CACHE)]

        ignite = IgniteService(
            self.test_context,
            IgniteConfiguration(
                version=IgniteVersion(ignite_version),
                data_storage=DataStorageConfiguration(default=DataRegionConfiguration(persistence_enabled=True)),
                caches=caches,
                metric_exporters={"org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi"}),
            num_nodes=3,
            jvm_opts=["-DIGNITE_PDS_WAL_REBALANCE_THRESHOLD=0", "-DIGNITE_PREFER_WAL_REBALANCE=true"])

        ignite.start()

        control = ControlUtility(ignite)
        control.activate()

        load = IgniteApplicationService(
            self.test_context,
            ignite.config._replace(client_mode=True, discovery_spi=from_ignite_cluster(ignite)),
            java_class_name=LOAD_APP,
            params={"mode": "TX_PUT", "cacheName": CACHE, "continueOnError": True, "progressIntervalMs": 1000,
                    "lockCache": LOCK_CACHE if explicit_locks else ""})

        load.start()

        time.sleep(BEFORE_SEC)

        node = ignite.nodes[-1]
        survivor = ignite.nodes[0]

        stopped_ms = now_ms()

        ignite.signal_node(node, getattr(signal, stop))
        ignite.await_stopped_node(node)

        time.sleep(AWAY_SEC)

        exchanges = len(exchange_times_ms(survivor))
        returned_ms = now_ms()

        ignite.start_node(node)

        wait_until(lambda: len(exchange_times_ms(survivor)) > exchanges, timeout_sec=ignite.startup_timeout_sec,
                   err_msg="The join's exchange did not complete")

        joined_ms = now_ms()
        join_exchange_ms = exchange_times_ms(survivor)[exchanges]

        await_rebalance_start(ignite)
        ignite.await_rebalance()

        rebalance = check_type_of_rebalancing([node], is_full=False)

        time.sleep(AFTER_SEC)

        load.stop()

        control.idle_verify()

        progress = load_progress(load)
        away = load_window(progress, stopped_ms, returned_ms)
        after = load_window(progress, joined_ms, now_ms())

        self.logger.info(f"Load while the node is away: {away}, after the join: {after}, "
                         f"join exchange: {join_exchange_ms} ms, rebalance: {rebalance}")

        assert away.max_stall_ms <= AWAY_STALL_MS[stop], \
            f"The load stood {away.max_stall_ms} ms while the node was away, bound {AWAY_STALL_MS[stop]} ms"

        assert join_exchange_ms <= JOIN_EXCHANGE_MS, \
            f"The join's exchange took {join_exchange_ms} ms, bound {JOIN_EXCHANGE_MS} ms"

        assert after.ok > 0 and after.err == 0, f"The load after the join: {after}"

        return {"away": away._asdict(), "after": after._asdict(), "join_exchange_ms": join_exchange_ms}


def now_ms():
    """
    :return: Current time in milliseconds, comparable with the load's progress records.
    """
    return int(time.time() * 1000)


def exchange_times_ms(node):
    """
    :return: Total time of each exchange the node completed, in order.
    """
    out = node.account.ssh_output(f"grep 'Exchange timings' {node.log_file}", allow_fail=True,
                                  combine_stderr=False).decode()

    return [int(ms) for ms in re.findall(r'stage="Total time" \((\d+) ms\)', out)]
