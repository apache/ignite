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

import functools
import operator
import os
import signal
import time
from threading import Thread

from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.utils.util import wait_until

from ignitetest.services.utils.concurrent import CountDownLatch
from ignitetest.services.utils.ignite_aware import IgniteAwareService
from ignitetest.tests.utils.version import DEV_BRANCH


class IgniteService(IgniteAwareService):
    """
    Ignite node service.
    """
    APP_SERVICE_CLASS = "org.apache.ignite.startup.cmdline.CommandLineStartup"
    HEAP_DUMP_FILE = os.path.join(IgniteAwareService.PERSISTENT_ROOT, "ignite-heap.bin")

    logs = {
        "console_log": {
            "path": IgniteAwareService.STDOUT_STDERR_CAPTURE,
            "collect_default": True},

        "heap_dump": {
            "path": HEAP_DUMP_FILE,
            "collect_default": False}
    }

    # pylint: disable=R0913
    def __init__(self, context, num_nodes, modules=None, client_mode=False, version=DEV_BRANCH, properties=""):
        super(IgniteService, self).__init__(context, num_nodes, modules, client_mode, version, properties)

    # pylint: disable=W0221
    def start(self, timeout_sec=180):
        super(IgniteService, self).start()

        self.logger.info("Waiting for Ignite(s) to start...")

        for node in self.nodes:
            self.await_node_started(node, timeout_sec)

    def start_cmd(self, node):
        jvm_opts = self.jvm_options + " "
        jvm_opts += "-J-DIGNITE_SUCCESS_FILE=" + IgniteService.PERSISTENT_ROOT + "/success_file "
        jvm_opts += "-J-Dlog4j.configDebug=true "

        cmd = "export EXCLUDE_TEST_CLASSES=true; "
        cmd += "export IGNITE_LOG_DIR=" + IgniteService.PERSISTENT_ROOT + "; "
        cmd += "export USER_LIBS=%s; " % ":".join(self.user_libs)
        cmd += "%s %s %s 1>> %s 2>> %s &" % \
               (self.path.script("ignite.sh"),
                jvm_opts,
                IgniteService.CONFIG_FILE,
                IgniteService.STDOUT_STDERR_CAPTURE,
                IgniteService.STDOUT_STDERR_CAPTURE)
        return cmd

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
            self.__stop_node(node, pid, sig)

        try:
            wait_until(lambda: len(self.pids(node)) == 0, timeout_sec=timeout_sec,
                       err_msg="Ignite node failed to stop in %d seconds" % timeout_sec)
        except Exception:
            self.thread_dump(node)
            raise

    def stop_nodes_async(self, nodes, delay_ms=100, clean_shutdown=True, timeout_sec=20):
        """
        Stops the nodes asynchronously.
        """
        sig = signal.SIGTERM if clean_shutdown else signal.SIGKILL

        sem = CountDownLatch(len(nodes))

        delay = 0

        for node in nodes:
            Thread(target=self.__stop_node, args=(node, next(iter(self.pids(node))), sig, sem, delay)).start()

            delay += delay_ms

        try:
            wait_until(lambda: len(functools.reduce(operator.iconcat, (self.pids(n) for n in nodes), [])) == 0,
                       timeout_sec=timeout_sec, err_msg="Ignite node failed to stop in %d seconds" % timeout_sec)
        except Exception:
            for node in nodes:
                self.thread_dump(node)
            raise

    @staticmethod
    def __stop_node(node, pid, sig, start_waiter=None, delay_ms=0):
        if start_waiter:
            start_waiter.count_down()
            start_waiter.wait()

        if delay_ms > 0:
            time.sleep(delay_ms/1000.0)

        node.account.signal(pid, sig, False)

    def clean_node(self, node):
        node.account.kill_java_processes(self.APP_SERVICE_CLASS, clean_shutdown=False, allow_fail=True)
        node.account.ssh("sudo rm -rf -- %s" % IgniteService.PERSISTENT_ROOT, allow_fail=False)

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
