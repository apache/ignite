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
This module contains base helper class for CDC configuration with different
CDC consumers implemented in ignite extensions.
"""

import time
from typing import NamedTuple

from ducktape.cluster.remoteaccount import RemoteCommandError

from ignitetest.services.utils.cdc.ignite_cdc import IgniteCdcUtility
from ignitetest.services.utils.jmx_utils import JmxClient


class CdcParams:
    """
    CDC parameters.
    """
    def __init__(self, caches=None, max_batch_size=None, only_primary=None,
                 conflict_resolve_field=None, cdc_configuration=None):
        self.caches = caches
        self.max_batch_size = max_batch_size
        self.only_primary = only_primary
        self.conflict_resolve_field = conflict_resolve_field

        self.cdc_configuration = CdcConfiguration() if cdc_configuration is None else cdc_configuration


class CdcConfiguration(NamedTuple):
    """
    CDC configuration template parameters.
    """
    check_frequency: int = None
    keep_binary: bool = None
    lock_timeout: int = None
    metric_exporter_spi: set = None


class CdcContext:
    """
    CDC context.

    Keeps information about CDC configuration needed to control the CDC (start, stop, wait etc.).
    In particular stores references to ignite-cdc and source cluster services.
    Different CDC extension helpers may add more fields.
    """
    def __init__(self):
        self.cdc_params = None
        self.ignite_cdc = None
        self.source_cluster = None


class CdcHelper:
    """
    Base CDC helper class for different CDC consumer extensions.
    """
    def configure(self, src_cluster, dst_cluster, cdc_params):
        """
        Configures the CDC. Updates the src_cluster service in place.

        May be overridden by subclasses.

        :param src_cluster: Ignite service for the source cluster.
        :param dst_cluster: Ignite service for the destination cluster.
        :param cdc_params: CDC test parameters.

        :return: CDC context
        """
        ctx = CdcContext()

        ctx.cdc_params = cdc_params
        ctx.source_cluster = src_cluster

        beans = self.get_src_cluster_cdc_ext_beans(src_cluster, dst_cluster, cdc_params, ctx)

        src_cluster.config = src_cluster.config._replace(
            ext_beans=[
                *src_cluster.config.ext_beans,
                *beans
            ]
        )

        ctx.ignite_cdc = IgniteCdcUtility(src_cluster)

        return ctx

    def get_src_cluster_cdc_ext_beans(self, src_cluster, dst_cluster, cdc_params, ctx):
        """
        Returns list of CDC beans required to be created in the source Ignite cluster.
        May update the CDC contex in place.

        Supposed to be overridden and extended by subclasses.

        :param src_cluster: Ignite service for the source cluster.
        :param dst_cluster: Ignite service for the destination cluster.
        :param cdc_params: CDC test parameters.
        :param ctx: CDC context.
        :return: List of beans. Each bean is represented as a pair of j2 template and params instance.
        """
        if cdc_params.cdc_configuration.metric_exporter_spi is None:
            if src_cluster.config.metric_exporters is None:
                src_cluster.config.metric_exporters = set()

        src_cluster.config.metric_exporters.add("org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi")

        cdc_params.cdc_configuration = cdc_params.cdc_configuration._replace(
            metric_exporter_spi=src_cluster.config.metric_exporters
        )

        return [("ignite_cdc.j2", cdc_params.cdc_configuration)]

    def start_ignite_cdc(self, ctx):
        """
        Starts CDC.

        Supposed to be overridden and extended by subclasses.
        Default implementation starts process executing the CDC consumer (ignite_cdc.sh).

        :param ctx: CDC context.
        """
        ctx.ignite_cdc.start()

    def stop_ignite_cdc(self, ctx, timeout_sec):
        """
        Stops CDC.

        Supposed to be overridden and extended by subclasses.
        Default implementation stops process executing the CDC consumer (ignite_cdc.sh).

        :param ctx: CDC context.
        :param timeout_sec: Timeout.
        :return: Arbitrary CDC consumer specific metrics (if any).
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
        """
        Waits all events are processed by the CDC streamer.

        It's considered that all events are processed if no new events are processed
        for last 'no_new_events_period_secs' seconds.

        :param ctx: CDC context.
        :param no_new_events_period_secs: Time period to wait for new events.
        :param timeout_sec: Timeout.
        """
        wait_ignite_cdc_service(ctx.ignite_cdc, no_new_events_period_secs, timeout_sec)


def wait_ignite_cdc_service(ignite_cdc, no_new_events_period_secs, timeout_sec):
    """
    Waits all events are processed by the CDC streamer in service passed.

    It's considered that all events are processed if no new events are processed
    for last 'no_new_events_period_secs' seconds.

    :param ignite_cdc: Service running the CDC.
    :param no_new_events_period_secs: Time period to wait for new events.
    :param timeout_sec: Timeout.
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
    by the CDC streamer in service passed.

    :param ignite_cdc: Service running the CDC.
    :return: Timestamp of the last CDC event (unix time in seconds).
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
        except (StopIteration, RemoteCommandError):
            ignite_cdc.logger.warn("Filed to read LastEventTime metric from ignite_cdc, node: " + node.account.hostname)

            return -1

    return max([last_event_time_on(node) for node in ignite_cdc.nodes]) / 1_000
