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

from typing import NamedTuple

from ignitetest.utils.bean import Bean
from ignitetest.utils.version import V_2_7_6

METRICS_KEY = "metrics"

ENABLED = "enabled"

OPENCENSUS_TEMPLATE_FILE = "opencensus_metrics_beans_macro.j2"
OPENCENSUS_KEY_NAME = "opencensus"
OPENCENSUS_NAME = "OpencensusMetrics"

JMX_KEY_NAME = "jmx"


class OpencensusMetricsParams(NamedTuple):
    """
    Params for Opencensus metrics exporter.

    Attributes:
        period  period of metrics export in millisecs
        port    port of http server to export metrics
        name    params name
    """
    period: int
    port: int
    name: str


def is_opencensus_metrics_enabled(service):
    """
    Returns True if OpenCensus metrics exporter is enabled via globals

    :param service: Ignite service
    :return: bool
    """
    return service.config.version > V_2_7_6 and \
        METRICS_KEY in service.context.globals and \
        OPENCENSUS_KEY_NAME in service.context.globals[METRICS_KEY] and \
        service.context.globals[METRICS_KEY][OPENCENSUS_KEY_NAME].get(ENABLED, False)


def configure_opencensus_metrics(config, _globals, spec):
    """
    Adds OpenCensus metrics exporter beans into the Ignite node configuration

    :param config: config object to be modified
    :param _globals: Globals parameters
    :return: the updated configuration object
    """
    if config.metrics_update_frequency is None:
        config = config._replace(metrics_update_frequency=1000)

    metrics_params = __get_opencensus_metrics_params(_globals)
    config.metric_exporters.add(Bean("org.apache.ignite.spi.metric.opencensus.OpenCensusMetricExporterSpi",
                                     period=metrics_params.period,
                                     sendInstanceName=True))

    if not any("opencensus.metrics.port" in jvm_opt for jvm_opt in spec.jvm_opts):
        spec.jvm_opts.append("-Dopencensus.metrics.port=%d" % metrics_params.port)

    if not any(bean[0] == OPENCENSUS_TEMPLATE_FILE for bean in config.ext_beans):
        config.ext_beans.append((OPENCENSUS_TEMPLATE_FILE, metrics_params))

    return config


def __get_opencensus_metrics_params(_globals: dict):
    """
    Get OpenCensus metrics exporter parameters from Globals.

    :param _globals: Globals parameters
    :return: instance of the OpencensusMetricsParams
    """
    return OpencensusMetricsParams(period=_globals[METRICS_KEY][OPENCENSUS_KEY_NAME].get("period", 1000),
                                   port=_globals[METRICS_KEY][OPENCENSUS_KEY_NAME].get("port", 8082),
                                   name=OPENCENSUS_NAME)


def is_jmx_metrics_enabled(service):
    """
    Returns True if JMX metrics exporter is enabled via globals

    :param service: Ignite service
    :return: bool
    """
    return service.config.version > V_2_7_6 and \
        METRICS_KEY in service.context.globals and \
        JMX_KEY_NAME in service.context.globals[METRICS_KEY] and \
        service.context.globals[METRICS_KEY][JMX_KEY_NAME].get(ENABLED, False)


def configure_jmx_metrics(config):
    """
    Adds JMX metrics exporter bean into the Ignite node configuration

    :param config: configuration object to be modified
    :return: the updated configuration object
    """
    if config.metrics_update_frequency is None:
        config = config._replace(metrics_update_frequency=1000)

    config.metric_exporters.add("org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi")

    return config
