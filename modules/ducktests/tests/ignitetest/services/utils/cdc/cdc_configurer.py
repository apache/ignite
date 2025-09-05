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
# limitations under the License

"""
This module contains helper classes for CDC configuration with different CDC consumers
implemented in ignite extensions.
"""

import time
from copy import deepcopy
from typing import NamedTuple

from ducktape.cluster.remoteaccount import RemoteCommandError

from ignitetest.services.utils.cdc.cdc_spec import get_cdc_spec
from ignitetest.services.utils.cdc.ignite_cdc import IgniteCdcUtility
from ignitetest.services.utils.cdc.ignite_cdc_params import IgniteCdcParams
from ignitetest.services.utils.ignite_configuration import TcpDiscoverySpi
from ignitetest.services.utils.ignite_configuration.discovery import TcpDiscoveryVmIpFinder
from ignitetest.services.utils.jmx_utils import JmxClient
from ignitetest.utils.bean import Bean


class CdcParams(NamedTuple):
    """
    CDC test parameters.
    """
    cdc_caches: list
    cdc_max_batch_size: int = None
    cdc_only_primary: bool = None
    cdc_kafka_to_ignite_threads: int = 8
    cdc_kafka_partitions: int = 16
    cdc_kafka_to_ignite_nodes: int = 2
    cdc_kafka_retention_ms: int = None
    cdc_kafka_nodes: int = 2
    cdc_keep_binary: bool = None


class CdcConfigurer:
    """
    Base CDC configurer class for different CDC consumer extensions.
    """
    def __init__(self):
        self.ignite_cdc = None
        self.source_cluster = None

    def configure_source_cluster(self, source_cluster, target_cluster, cdc_params: CdcParams):
        """
        Configures CDC on the source cluster. Updates the source_cluster in place.

        :param source_cluster Ignite service representing the source cluster.
        :param target_cluster Ignite service representing the target cluster.
        :param cdc_params CDC test params.
        """
        source_cluster.config = source_cluster.config._replace(
            discovery_spi=TcpDiscoverySpi(ip_finder=TcpDiscoveryVmIpFinder()),
            plugins=[*source_cluster.config.plugins,
                     ('bean.j2',
                      Bean("org.apache.ignite.cdc.conflictresolve.CacheVersionConflictResolverPluginProvider",
                           cluster_id="1",
                           caches=cdc_params.cdc_caches))],
            ext_beans=self.get_cdc_beans(source_cluster, target_cluster, cdc_params),
            data_storage=source_cluster.config.data_storage._replace(
                default=source_cluster.config.data_storage.default._replace(cdc_enabled=True)
            )
        )

    def configure_target_cluster(self, target_cluster, cdc_params: CdcParams):
        """
        Configures CDC on the target cluster. Updates the target_cluster in place.

        :param target_cluster Ignite service representing the target cluster.
        :param cdc_params CDC test params.
        """
        target_cluster.config = target_cluster.config._replace(
            plugins=[*target_cluster.config.plugins,
                     ('bean.j2',
                      Bean("org.apache.ignite.cdc.conflictresolve.CacheVersionConflictResolverPluginProvider",
                           cluster_id="2",
                           caches=cdc_params.cdc_caches))]
        )

        target_cluster.spec = get_cdc_spec(target_cluster.spec.__class__, target_cluster)

    def get_cdc_beans(self, source_cluster, target_cluster, cdc_params: CdcParams):
        """
        Returns list of CDC beans required to be created in the source Ignite cluster.
        Each bean is represented as a pair of j2 template and params instance.

        :param source_cluster Ignite service representing the source cluster.
        :param target_cluster Ignite service representing the target cluster.
        :param cdc_params CDC test params.
        :return: list of beans
        """

        metric_exporters = deepcopy(source_cluster.config.metric_exporters)
        metric_exporters.add("org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi")

        return [("ignite_cdc.j2", IgniteCdcParams(
            metric_exporter_spi=metric_exporters,
            keep_binary=cdc_params.cdc_keep_binary
        ))]

    def start_ignite_cdc(self, source_cluster):
        """
        Starts process executing the CDC consumer (ignite_cdc.sh).

        :param source_cluster: Source Ignite cluster.
        :return: Service running the CDC consumer.
        """
        self.ignite_cdc = IgniteCdcUtility(source_cluster)

        self.ignite_cdc.start()

        self.source_cluster = source_cluster

    def stop_ignite_cdc(self, source_cluster, timeout_sec):
        """
        Stops process executing the CDC consumer (ignite_cdc.sh).

        :param source_cluster: Source Ignite cluster.
        :param timeout_sec: Timeout.
        :return: arbitrary CDC consumer specific metrics (if any).
        """
        self.ignite_cdc.stop()

        try:
            source_cluster.await_event("WalRecordsConsumer stopped",
                                       timeout_sec=timeout_sec, from_the_beginning=True,
                                       log_file="ignite-cdc.log")
        except TimeoutError:
            self.ignite_cdc.stop(force_stop=True)

        return self.close_cdc_beans(timeout_sec)

    def close_cdc_beans(self, timeout_sec):
        """
        Close and stop any services run for CDC streamer
        :param timeout_sec: Timeout.
        :return: arbitrary CDC consumer specific metrics (if any).
        """
        return {}

    def wait_cdc(self, no_new_events_period_secs, timeout_sec):
        wait_ignite_cdc_service(self.ignite_cdc, no_new_events_period_secs, timeout_sec)


def wait_ignite_cdc_service(ignite_cdc, no_new_events_period_secs, timeout_sec):
    """
    Waits all events are processed by the CDC streamer.

    It's considered that all events are processed if no new events are processed
    for last 'no_new_events_period_secs' seconds.
    """
    start = time.time()
    end = start + timeout_sec

    while True:
        now = time.time()
        if now > end:
            raise TimeoutError(f"Timed out waiting {timeout_sec} seconds for ignite_cdc.sh to stream all data.")

        last = last_ignite_cdc_event_time(ignite_cdc)

        if last + no_new_events_period_secs < now:
            return
        else:
            time.sleep(1)


def last_ignite_cdc_event_time(ignite_cdc):
    """
    Requests timestamp (unix time in seconds) of the last CDC event processed
    by the CDC streamer.
    """
    def last_event_time_on(node):
        jmx_client = JmxClient(node)

        if isinstance(ignite_cdc, IgniteCdcUtility):
            main_java_class = ignite_cdc.APP_SERVICE_CLASS
        else:
            main_java_class = ignite_cdc.main_java_class

        pids = ignite_cdc.pids(node, main_java_class)

        if len(pids) == 0:
            raise AssertionError("ignite_cdc java process is not found on node: " + node.account.hostname)

        jmx_client.pid = pids[0]

        try:
            mbean = jmx_client.find_mbean('.*name=cdc.*')

            return int(next(mbean.LastEventTime).strip())
        except RemoteCommandError:
            ignite_cdc.cluster.test_context.logger.warn(
                "LastEventTime metric wasn't exposed in ignite_cdc, node: " + node.account.hostname)

            return -1

    return max([last_event_time_on(node) for node in ignite_cdc.nodes]) / 1_000
