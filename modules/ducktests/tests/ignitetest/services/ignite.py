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

import os.path
import signal

from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.services.service import Service
from ducktape.utils.util import wait_until

from ignitetest.services.utils.ignite_aware import IgniteAwareService
from ignitetest.version import DEV_BRANCH


class IgniteService(IgniteAwareService):
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

    def __init__(self, context, num_nodes, version=DEV_BRANCH, properties=""):
        IgniteAwareService.__init__(self, context, num_nodes, version, properties)

    def start(self, timeout_sec=180):
        Service.start(self)

        self.logger.info("Waiting for Ignite(s) to start...")

        for node in self.nodes:
            self.await_node_stated(node, timeout_sec)

    def start_cmd(self, node):
        jvm_opts = "-J-DIGNITE_SUCCESS_FILE=" + IgniteService.PERSISTENT_ROOT + "/success_file "
        jvm_opts += "-J-Dlog4j.configDebug=true"

        cmd = "export EXCLUDE_TEST_CLASSES=true; "
        cmd += "export IGNITE_LOG_DIR=" + IgniteService.PERSISTENT_ROOT + "; "
        cmd += "export USER_LIBS=%s/libs/optional/ignite-log4j/*; " % self.path.home(self.version)
        cmd += "%s %s %s 1>> %s 2>> %s &" % \
               (self.path.script("ignite.sh", node),
                jvm_opts,
                IgniteService.CONFIG_FILE,
                IgniteService.STDOUT_STDERR_CAPTURE,
                IgniteService.STDOUT_STDERR_CAPTURE)
        return cmd

    def await_node_stated(self, node, timeout_sec):
        self.await_event_on_node("Topology snapshot", node, timeout_sec, from_the_beginning=True)

        if len(self.pids(node)) == 0:
            raise Exception("No process ids recorded on node %s" % node.account.hostname)

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
        node.account.ssh("sudo rm -rf -- %s" % IgniteService.PERSISTENT_ROOT, allow_fail=False)

    def thread_dump(self, node):
        for pid in self.pids(node):
            try:
                node.account.signal(pid, signal.SIGQUIT, allow_fail=True)
            except:
                self.logger.warn("Could not dump threads on node")

    def pids(self, node):
        """Return process ids associated with running processes on the given node."""
        try:
            cmd = "jcmd | grep -e %s | awk '{print $1}'" % self.APP_SERVICE_CLASS
            pid_arr = [pid for pid in node.account.ssh_capture(cmd, allow_fail=True, callback=int)]
            return pid_arr
        except (RemoteCommandError, ValueError) as e:
            return []
