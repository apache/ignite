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
This module contains the base class to build services aware of Ignite.
"""

import os
from abc import abstractmethod

from ducktape.services.background_thread import BackgroundThreadService
from ducktape.utils.util import wait_until

from ignitetest.services.utils.ignite_config import IgniteLoggerConfig, IgniteServerConfig, IgniteClientConfig
from ignitetest.services.utils.ignite_path import IgnitePath
from ignitetest.services.utils.jmx_utils import ignite_jmx_mixin

from ignitetest.utils.version import IgniteVersion


class IgniteAwareService(BackgroundThreadService):
    """
    The base class to build services aware of Ignite.
    """
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

    # pylint: disable=R0913
    def __init__(self, context, num_nodes, modules, client_mode, version, properties):
        super(IgniteAwareService, self).__init__(context, num_nodes)

        self.jvm_options = context.globals.get("jvm_opts", "")

        self.log_level = "DEBUG"
        self.properties = properties

        if isinstance(version, IgniteVersion):
            self.version = version
        else:
            self.version = IgniteVersion(version)

        self.path = IgnitePath(self.version, context)
        self.client_mode = client_mode

        libs = modules or []
        libs.extend(["ignite-log4j"])
        libs = map(lambda m: self.path.module(m) + "/*", libs)
        self.user_libs = ":".join(libs)

    def start_node(self, node):
        self.init_persistent(node)

        super(IgniteAwareService, self).start_node(node)

        wait_until(lambda: len(self.pids(node)) > 0, timeout_sec=10)

        ignite_jmx_mixin(node, self.pids(node))

    def init_persistent(self, node):
        """
        Init persistent directory.
        :param node: Ignite service node.
        """
        logger_config = IgniteLoggerConfig().render(work_dir=self.WORK_DIR)

        node.account.mkdirs(self.PERSISTENT_ROOT)
        node.account.create_file(self.CONFIG_FILE, self.config().render(
            config_dir=self.PERSISTENT_ROOT, work_dir=self.WORK_DIR, properties=self.properties))
        node.account.create_file(self.LOG4J_CONFIG_FILE, logger_config)

    @abstractmethod
    def start_cmd(self, node):
        """
        Start up command for service.
        :param node: Ignite service node.
        """
        raise NotImplementedError

    @abstractmethod
    def pids(self, node):
        """
        :param node: Ignite service node.
        :return: List of service's pids.
        """
        raise NotImplementedError

    def config(self):
        """
        :return: Ignite node configuration.
        """
        if self.client_mode:
            return IgniteClientConfig(self.context)

        return IgniteServerConfig(self.context)

    # pylint: disable=W0613
    def _worker(self, idx, node):
        cmd = self.start_cmd(node)

        self.logger.debug("Attempting to start Application Service on %s with command: %s" % (str(node.account), cmd))

        node.account.ssh(cmd)

    def alive(self, node):
        """
        :param node: Ignite service node.
        :return: True if node is alive.
        """
        return len(self.pids(node)) > 0

    # pylint: disable=R0913
    def await_event_on_node(self, evt_message, node, timeout_sec, from_the_beginning=False, backoff_sec=5):
        """
        Await for specific event message in a node's log file.
        :param evt_message: Event message.
        :param node: Ignite service node.
        :param timeout_sec: Number of seconds to check the condition for before failing.
        :param from_the_beginning: If True, search for message from the beggining of log file.
        :param backoff_sec: Number of seconds to back off between each failure to meet the condition
                before checking again.
        """
        with node.account.monitor_log(self.STDOUT_STDERR_CAPTURE) as monitor:
            if from_the_beginning:
                monitor.offset = 0

            monitor.wait_until(evt_message, timeout_sec=timeout_sec, backoff_sec=backoff_sec,
                               err_msg="Event [%s] was not triggered in %d seconds" % (evt_message, timeout_sec))

    def await_event(self, evt_message, timeout_sec, from_the_beginning=False, backoff_sec=5):
        """
        Await for specific event messages on all nodes.
        :param evt_message: Event message.
        :param timeout_sec: Number of seconds to check the condition for before failing.
        :param from_the_beginning: If True, search for message from the beggining of log file.
        :param backoff_sec: Number of seconds to back off between each failure to meet the condition
                before checking again.
        """
        assert len(self.nodes) == 1

        self.await_event_on_node(evt_message, self.nodes[0], timeout_sec, from_the_beginning=from_the_beginning,
                                 backoff_sec=backoff_sec)

    def execute(self, command):
        """
        Execute command on all nodes.
        :param command: Command to execute.
        """
        for node in self.nodes:
            cmd = "%s 1>> %s 2>> %s" % \
                  (self.path.script(command),
                   self.STDOUT_STDERR_CAPTURE,
                   self.STDOUT_STDERR_CAPTURE)

            node.account.ssh(cmd)
