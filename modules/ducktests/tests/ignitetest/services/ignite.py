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
import threading

from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.utils.util import wait_until

from ignitetest.services.utils.ignite_aware import IgniteAwareService
from ignitetest.tests.utils.version import DEV_BRANCH


# class Latch:
#     def __init__(self, count):
#         self.cond_var = threading.Condition()
#         self.count = count
#
#     def wait(self):
#         with self.cond_var:
#             while self.count > 0:
#                 self.cond_var.wait()
#
#     def count_down(self):
#         with self.cond_var:
#             if self.count > 0:
#                 self.count -= 1
#
#             if self.count == 0:
#                 self.cond_var.notify_all()

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
            node.account.signal(pid, sig, allow_fail=False)

        try:
            wait_until(lambda: len(self.pids(node)) == 0, timeout_sec=timeout_sec,
                       err_msg="Ignite node failed to stop in %d seconds" % timeout_sec)
        except Exception:
            self.thread_dump(node)
            raise

    def stop_nodes(self, nodes, delay_ms=100, clean_shutdown=True, timeout_sec=20):
        sig = signal.SIGTERM if clean_shutdown else signal.SIGKILL
        #
        # latch = Latch(len(nodes))
        #
        # threads = []
        # for node in nodes:
        #     thread = threading.Thread(target=self.__stop_node__, args=(node, next(iter(self.pids(node))), sig, latch))
        #     self.logger.warn("Starting thread")
        #     threads.append(thread)
        #     thread.start()
        #     self.logger.warn("Started thread")
        #
        # for thread in threads:
        #     thread.join()


    # for thread in threads:
        #     thread.join()

        # return {"activate_res": " ".join(activate_res), "baseline_res": baseline_res}


        sem = threading.Semaphore(len(nodes))
        # for node in nodes:
        #     sem.acquire()

        # sem = RLock()
        # sem.acquire()
        #
        # sem = Condition(RLock())
        # sem.acquire()
        #
        # sem = Event()
        # sem.set()

        try:
            for node in nodes:
                thread = threading.Thread(target=self.__stop_node__,args=(node, next(iter(self.pids(node))), sig, sem))

                self.logger.warn("Starting stop thread for pid " + str(next(iter(self.pids(node)))))

                thread.start()

                self.logger.warn("Started stop thread for pid " + str(next(iter(self.pids(node)))))
        finally:
            pass
            #for node in nodes:
                #sem.release()
            # sem.release()

        try:
            wait_until(lambda: len(functools.reduce(operator.iconcat, (self.pids(n) for n in nodes), [])) == 0,
                       timeout_sec=timeout_sec, err_msg="Ignite node failed to stop in %d seconds" % timeout_sec)
        except Exception:
            for node in nodes:
                self.thread_dump(node)
            raise

    # def __stop_node__(self, node, pid, sig, latch):
    #     self.logger.warn("__stop_node__ launched, countdown the latch")
    #     latch.count_down()
    #     self.logger.warn("__stop_node__ launched, waiting for the latch")
    #     latch.wait()
    #
    #     self.logger.info("Killing on %s" % node.account.externally_routable_ip)
    #
    #     node.account.signal(pid, sig)
    #
    #     self.logger.info("Killed on %s" % node.account.externally_routable_ip)

    def __stop_node__(self, node, pid, sig,  sem):
        if sem:
            self.logger.warn("Started stop thread for pid " + str(pid) + " on node " + node.discovery_info().node_id + ". Waiting for the sem.")

            #sem.acquire()
            sem.release()

            self.logger.warn("Released stop thread for pid " + str(pid) + " on node " + node.discovery_info().node_id + ". Waited for the sem.")

        self.logger.warn("Stopping " + str(pid) + " on node " + node.discovery_info().node_id)

        node.account.signal(pid, sig, True)

        self.logger.warn("Stopped " + str(pid))

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
