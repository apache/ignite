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
from typing import NamedTuple

from ducktape.cluster.remoteaccount import RemoteCommandError

from ignitetest.services.utils.cdc.ignite_cdc import IgniteCdcUtility
from ignitetest.services.utils.jmx_utils import JmxClient


class CdcConfiguration(NamedTuple):
    check_frequency: int = None
    keep_binary: bool = None
    lock_timeout: int = None
    metric_exporter_spi: set = None
    name: str = "IgniteCdcParams"


class CdcParams:
    def __init__(self, caches=None, max_batch_size=None, only_primary=None,
                 conflict_resolve_field=None, cdc_configuration=None):
        self.caches = caches
        self.max_batch_size = max_batch_size
        self.only_primary = only_primary
        self.conflict_resolve_field = conflict_resolve_field

        self.cdc_configuration = CdcConfiguration() if cdc_configuration is None else cdc_configuration


class CdcContext:
    def __init__(self):
        self.ignite_cdc = None
        self.source_cluster = None


class CdcConfigurer:
    """
    Base CDC configurer class for different CDC consumer extensions.
    """
    def configure_source_cluster(self, src_cluster, dst_cluster, cdc_params: CdcParams):
        """
        Configures CDC on the source cluster. Updates the source_cluster in place.

        :param src_cluster Ignite service representing the source cluster.
        :param dst_cluster Ignite service representing the target cluster.
        :param cdc_params CDC test params.
        """
        ctx = CdcContext()

        ctx.cdc_params = cdc_params
        ctx.source_cluster = src_cluster

        beans = self.get_cdc_beans(src_cluster, dst_cluster, cdc_params, ctx)

        src_cluster.config = src_cluster.config._replace(
            ext_beans=[
                *src_cluster.config.ext_beans,
                *beans
            ]
        )

        return ctx

    def get_cdc_beans(self, src_cluster, dst_cluster, cdc_params: CdcParams, ctx):
        """
        Returns list of CDC beans required to be created in the source Ignite cluster.
        Each bean is represented as a pair of j2 template and params instance.

        :param src_cluster Ignite service representing the source cluster.
        :param dst_cluster Ignite service representing the destination cluster.
        :param cdc_params CDC test params.
        :param ctx CDC context.
        :return: list of beans
        """
        if cdc_params.cdc_configuration.metric_exporter_spi is None:
            if src_cluster.config.metric_exporters is None:
                src_cluster.config.metric_exporters = set()

        src_cluster.config.metric_exporters.add("org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi")

        cdc_params.cdc_configuration = cdc_params.cdc_configuration._replace(
            metric_exporter_spi=src_cluster.config.metric_exporters
        )

        ctx.ignite_cdc = IgniteCdcUtility(src_cluster)

        return [("ignite_cdc.j2", cdc_params.cdc_configuration)]

    def start_ignite_cdc(self, ctx):
        """
        Starts process executing the CDC consumer (ignite_cdc.sh).

        :param cdc_params: CDC test params.
        :param source_cluster: Source Ignite cluster.
        :return: Service running the CDC consumer.
        """
        ctx.ignite_cdc.start()

    def stop_ignite_cdc(self, ctx, timeout_sec):
        """
        Stops process executing the CDC consumer (ignite_cdc.sh).

        :param ignite_cdc: Service running the CDC consumer.
        :param source_cluster: Source Ignite cluster.
        :param timeout_sec: Timeout.
        :return: arbitrary CDC consumer specific metrics (if any).
        """
        ctx.ignite_cdc.stop()

        try:
            ctx.source_cluster.await_event("WalRecordsConsumer stopped",
                                           timeout_sec=timeout_sec, from_the_beginning=True,
                                           log_file="ignite-cdc.log")
        except TimeoutError:
            ctx.ignite_cdc.stop(force_stop=True)

        return {}

    def wait_cdc(self, ctx, no_new_events_period_secs, timeout_sec):
        wait_ignite_cdc_service(ctx.ignite_cdc, no_new_events_period_secs, timeout_sec)


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
