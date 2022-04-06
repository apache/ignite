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
    period: int
    port: int
    name: str

    @staticmethod
    def enabled(service):
        return service.config.version > V_2_7_6 and \
               METRICS_KEY in service.context.globals and \
               OPENCENSUS_KEY_NAME in service.context.globals[METRICS_KEY] and \
               service.context.globals[METRICS_KEY][OPENCENSUS_KEY_NAME].get(ENABLED, False)

    @staticmethod
    def __from_globals(_globals):
        return OpencensusMetricsParams(period=_globals[METRICS_KEY][OPENCENSUS_KEY_NAME].get("period", 1000),
                                       port=_globals[METRICS_KEY][OPENCENSUS_KEY_NAME].get("port", 8082),
                                       name=OPENCENSUS_NAME)

    @staticmethod
    def add_to_config(config, _globals):
        if config.metrics_update_frequency is None:
            config = config._replace(metrics_update_frequency=1000)

        metrics_params = OpencensusMetricsParams.__from_globals(_globals)
        config.metric_exporters.add(Bean("org.apache.ignite.spi.metric.opencensus.OpenCensusMetricExporterSpi",
                                         period=metrics_params.period,
                                         sendInstanceName=True))

        if not any((bean[1].name and bean[1].name == OPENCENSUS_NAME) for bean in config.ext_beans):
            config.ext_beans.append((OPENCENSUS_TEMPLATE_FILE, metrics_params))

        return config


class JmxMetricsParams:
    @staticmethod
    def enabled(service):
        return service.config.version > V_2_7_6 and \
               METRICS_KEY in service.context.globals and \
               JMX_KEY_NAME in service.context.globals[METRICS_KEY] and \
               service.context.globals[METRICS_KEY][JMX_KEY_NAME][ENABLED]

    @staticmethod
    def add_to_config(config):
        if config.metrics_update_frequency is None:
            config = config._replace(metrics_update_frequency=1000)

        config.metric_exporters.add("org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi")

        return config
