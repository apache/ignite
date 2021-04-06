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
This module contains class to start ignite cluster node.
"""

from ignitetest.services.utils.ignite_aware import IgniteAwareService


class IgniteService(IgniteAwareService):
    """
    Ignite node service.
    """
    APP_SERVICE_CLASS = "org.apache.ignite.startup.cmdline.CommandLineStartup"

    # pylint: disable=R0913
    def __init__(self, context, config, num_nodes, jvm_opts=None, full_jvm_opts=None, startup_timeout_sec=60,
                 shutdown_timeout_sec=10, modules=None):
        super().__init__(context, config, num_nodes, startup_timeout_sec, shutdown_timeout_sec, self.APP_SERVICE_CLASS,
                         modules=modules, jvm_opts=jvm_opts, full_jvm_opts=full_jvm_opts)


def node_failed_event_pattern(failed_node_id=None):
    """Failed node pattern in log."""
    return "Node FAILED: .\\{1,\\}Node \\[id=" + (failed_node_id if failed_node_id else "") + \
           ".\\{1,\\}\\(isClient\\|client\\)=false"


def get_event_time(service, log_node, log_pattern, from_the_beginning=True, timeout=15):
    """
    Extracts event time from ignite log by pattern .
    :param service: ducktape service (ignite service) responsible to search log.
    :param log_node: ducktape node to search ignite log on.
    :param log_pattern: pattern to search ignite log for.
    :param from_the_beginning: switches searching log from its beginning.
    :param timeout: timeout to wait for the patters in the log.
    """
    service.await_event_on_node(log_pattern, log_node, timeout, from_the_beginning=from_the_beginning,
                                backoff_sec=0.3)

    return IgniteAwareService.event_time(log_pattern, log_node)
