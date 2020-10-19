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
from datetime import datetime
from threading import Thread

from time import monotonic
from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.utils.util import wait_until

from ignitetest.services.utils.concurrent import CountDownLatch, AtomicValue
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

    def restart(self, timeout_sec=180):
        """
        Restart ignite cluster without cleaning.
        """
        self.stop()

        for node in self.nodes:
            self.start_node(node)

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
            self.__stop_node(node, pid, sig)

        try:
            wait_until(lambda: len(self.pids(node)) == 0, timeout_sec=timeout_sec,
                       err_msg="Ignite node failed to stop in %d seconds" % timeout_sec)
        except Exception:
            self.thread_dump(node)
            raise

    def stop_nodes_async(self, nodes, delay_ms=0, clean_shutdown=True, timeout_sec=20, wait_for_stop=False):
        """
        Stops the nodes asynchronously.
        """
        sig = signal.SIGTERM if clean_shutdown else signal.SIGKILL

        sem = CountDownLatch(len(nodes))
        time_holder = AtomicValue()

        delay = 0
        threads = []

        for node in nodes:
            thread = Thread(target=self.__stop_node,
                            args=(node, next(iter(self.pids(node))), sig, sem, delay, time_holder))

            threads.append(thread)

            thread.start()

            delay += delay_ms

        for thread in threads:
            thread.join(timeout_sec)

        if wait_for_stop:
            try:
                wait_until(lambda: len(functools.reduce(operator.iconcat, (self.pids(n) for n in nodes), [])) == 0,
                           timeout_sec=timeout_sec, err_msg="Ignite node failed to stop in %d seconds" % timeout_sec)
            except Exception:
                for node in nodes:
                    self.thread_dump(node)
                raise

        return time_holder.get()

    @staticmethod
    def __stop_node(node, pid, sig, start_waiter=None, delay_ms=0, time_holder=None):
        if start_waiter:
            start_waiter.count_down()
            start_waiter.wait()

        if delay_ms > 0:
            time.sleep(delay_ms / 1000.0)

        if time_holder:
            mono = monotonic()
            timestamp = datetime.now()

            time_holder.compare_and_set(None, (mono, timestamp))

        node.account.signal(pid, sig, False)

    def clean_node(self, node):
        node.account.kill_java_processes(self.APP_SERVICE_CLASS, clean_shutdown=False, allow_fail=True)
        node.account.ssh("sudo rm -rf -- %s" % self.PERSISTENT_ROOT, allow_fail=False)

    def remove(self, node, path: str):  # pylint: disable=R0201
        """
        Remove on node.
        """
        node.account.ssh("sudo rm -rf -- %s" % path, allow_fail=False)

    def rename_db(self, new_db_name: str):
        """
        Rename db.
        """
        for node in self.nodes:
            self._rename_db(node, new_db_name)

    def _rename_db(self, node, new_db_name: str):
        """
        Rename db.
        """
        assert len(self.pids(node)) == 0

        node.account.ssh(f"mv {self.WORK_DIR}/db {self.WORK_DIR}/{new_db_name}")

    def restore_from_snapshot(self, snapshot_name: str):
        """
        Copy from snapshot to db.
        """
        for node in self.nodes:
            self._copy_snap_to_db(node, snapshot_name)

    def _copy_snap_to_db(self, node, snapshot_name: str):
        """
        Copy from snapshot to db.
        """
        assert len(self.pids(node)) == 0

        node.account.ssh(f"cp -r {self.SNAPSHOT}/{snapshot_name}/db {self.WORK_DIR}", allow_fail=False)

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
