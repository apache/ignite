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

import os
import re
import signal
from datetime import datetime

from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.utils.util import wait_until

from ignitetest.services.utils.ignite_aware import IgniteAwareService


class IgniteService(IgniteAwareService):
    """
    Ignite node service.
    """
    APP_SERVICE_CLASS = "org.apache.ignite.startup.cmdline.CommandLineStartup"
    HEAP_DUMP_FILE = os.path.join(IgniteAwareService.PERSISTENT_ROOT, "ignite-heap.bin")

    # pylint: disable=R0913
    def __init__(self, context, config, num_nodes, jvm_opts=None, modules=None):
        super().__init__(context, config, num_nodes, modules=modules, jvm_opts=jvm_opts)

    # pylint: disable=W0221
    def start(self, timeout_sec=180):
        super().start()

        self.logger.info("Waiting for Ignite(s) to start...")

        for node in self.nodes:
            self.await_node_started(node, timeout_sec)

    def await_node_started(self, node, timeout_sec):
        """
        Await topology ready event on node start.
        :param node: Node to wait
        :param timeout_sec: Number of seconds to wait event.
        """
        self.await_event_on_node("Topology snapshot", node, timeout_sec, from_the_beginning=True)

        if len(self.pids(node)) == 0:
            raise Exception("No process ids recorded on node %s" % node.account.hostname)

    # pylint: disable=W0221
    def stop_node(self, node, clean_shutdown=True, timeout_sec=60):
        pids = self.pids(node)
        sig = signal.SIGTERM if clean_shutdown else signal.SIGKILL

        for pid in pids:
            node.account.signal(pid, sig, allow_fail=False)

        try:
            wait_until(lambda: len(self.pids(node)) == 0, timeout_sec=timeout_sec,
                       err_msg="Ignite node failed to stop in %d seconds" % timeout_sec)
        except Exception:
            self.thread_dump(node)
            raise

    def clean_node(self, node):
        node.account.kill_java_processes(self.APP_SERVICE_CLASS, clean_shutdown=False, allow_fail=True)
        node.account.ssh("sudo rm -rf -- %s" % self.PERSISTENT_ROOT, allow_fail=False)

    def thread_dump(self, node):
        """
        Generate thread dump on node.
        :param node: Ignite service node.
        """
        for pid in self.pids(node):
            try:
                node.account.signal(pid, signal.SIGQUIT, allow_fail=True)
            except RemoteCommandError:
                self.logger.warn("Could not dump threads on node")

    def pids(self, node):
        try:
            cmd = "jcmd | grep -e %s | awk '{print $1}'" % self.APP_SERVICE_CLASS
            pid_arr = list(node.account.ssh_capture(cmd, allow_fail=True, callback=int))
            return pid_arr
        except (RemoteCommandError, ValueError):
            return []


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

    _, stdout, _ = log_node.account.ssh_client.exec_command(
        "grep '%s' %s" % (log_pattern, IgniteAwareService.STDOUT_STDERR_CAPTURE))

    return datetime.strptime(re.match("^\\[[^\\[]+\\]", stdout.read().decode("utf-8")).group(),
                             "[%Y-%m-%d %H:%M:%S,%f]")
