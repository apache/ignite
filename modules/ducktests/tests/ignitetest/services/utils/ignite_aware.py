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

import os
from abc import abstractmethod

from ducktape.services.background_thread import BackgroundThreadService

from ignitetest.services.utils.ignite_config import IgniteConfig
from ignitetest.services.utils.ignite_path import IgnitePath

"""
The base class to build services aware of Ignite.
"""


class IgniteAwareService(BackgroundThreadService):
    # Root directory for persistent output
    PERSISTENT_ROOT = "/mnt/service"
    STDOUT_STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "console.log")
    WORK_DIR = os.path.join(PERSISTENT_ROOT, "work")
    CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "ignite-config.xml")
    LOG4J_CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "ignite-log4j.xml")

    logs = {
        "console_log": {
            "path": STDOUT_STDERR_CAPTURE,
            "collect_default": True}
    }

    def __init__(self, context, num_nodes, version, properties):
        BackgroundThreadService.__init__(self, context, num_nodes)

        self.log_level = "DEBUG"
        self.config = IgniteConfig()
        self.path = IgnitePath()
        self.properties = properties
        self.version = version

        for node in self.nodes:
            node.version = version

    def start_node(self, node):
        self.init_persistent(node)

        BackgroundThreadService.start_node(self, node)

    def init_persistent(self, node):
        node.account.mkdirs(self.PERSISTENT_ROOT)
        node.account.create_file(self.CONFIG_FILE, self.config.render(
            self.PERSISTENT_ROOT, self.WORK_DIR, properties=self.properties))
        node.account.create_file(self.LOG4J_CONFIG_FILE, self.config.render_log4j(self.WORK_DIR))

    @abstractmethod
    def start_cmd(self, node):
        raise NotImplementedError

    @abstractmethod
    def pids(self, node):
        raise NotImplementedError

    def _worker(self, idx, node):
        cmd = self.start_cmd(node)

        self.logger.debug("Attempting to start Application Service on %s with command: %s" % (str(node.account), cmd))

        node.account.ssh(cmd)

    def alive(self, node):
        return len(self.pids(node)) > 0

    def await_event_on_node(self, evt_message, node, timeout_sec, from_the_beginning=False, backoff_sec=5):
        with node.account.monitor_log(self.STDOUT_STDERR_CAPTURE) as monitor:
            if from_the_beginning:
                monitor.offset = 0

            monitor.wait_until(evt_message, timeout_sec=timeout_sec, backoff_sec=backoff_sec,
                               err_msg="Event [%s] was not triggered in %d seconds" % (evt_message, timeout_sec))

    def await_event(self, evt_message, timeout_sec, from_the_beginning=False, backoff_sec=5):
        assert len(self.nodes) == 1

        self.await_event_on_node(evt_message, self.nodes[0], timeout_sec, from_the_beginning=from_the_beginning,
                                 backoff_sec=backoff_sec)

    def execute(self, command):
        for node in self.nodes:
            cmd = "%s 1>> %s 2>> %s" % \
                  (self.path.script(command, node),
                   self.STDOUT_STDERR_CAPTURE,
                   self.STDOUT_STDERR_CAPTURE)

            node.account.ssh(cmd)
