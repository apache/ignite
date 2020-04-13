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

from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.services.background_thread import BackgroundThreadService

from ignitetest.directory_layout.ignite_path import IgnitePathResolverMixin

from ignitetest.version import DEV_BRANCH

"""
The console consumer is a tool that reads data from Kafka and outputs it to standard output.
"""


class IgniteClientApp(IgnitePathResolverMixin, BackgroundThreadService):
    # Root directory for persistent output
    PERSISTENT_ROOT = "/mnt/client_app"
    STDOUT_CAPTURE = os.path.join(PERSISTENT_ROOT, "client_app.stdout")
    STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "client_app.stderr")
    LOG_DIR = os.path.join(PERSISTENT_ROOT, "logs")
    LOG_FILE = os.path.join(LOG_DIR, "client_app.log")

    logs = {
        "consumer_stdout": {
            "path": STDOUT_CAPTURE,
            "collect_default": False},
        "consumer_stderr": {
            "path": STDERR_CAPTURE,
            "collect_default": False},
        "consumer_log": {
            "path": LOG_FILE,
            "collect_default": True}
    }

    def __init__(self, context, version=DEV_BRANCH, num_nodes=1):
        """
        Args:
            num_nodes:                  number of nodes to use (this should be 1)
        """
        BackgroundThreadService.__init__(self, context, num_nodes)

        for node in self.nodes:
            node.version = version

    def start_cmd(self, node):
        """Return the start command appropriate for the given node."""
        args = self.args.copy()
        args['stdout'] = IgniteClientApp.STDOUT_CAPTURE
        args['stderr'] = IgniteClientApp.STDERR_CAPTURE
        args['log_dir'] = IgniteClientApp.LOG_DIR
        args['config_file'] = IgniteClientApp.CONFIG_FILE
        args['stdout'] = IgniteClientApp.STDOUT_CAPTURE
        args['console_consumer'] = self.path.script("kafka-console-consumer.sh", node)

        cmd = "%(console_consumer)s " \
              "--topic %(topic)s " \
              "--consumer.config %(config_file)s " % args

        cmd += " 2>> %(stderr)s | tee -a %(stdout)s &" % args
        return cmd

    def pids(self, node):
        return node.account.java_pids(self.java_class_name())

    def alive(self, node):
        return len(self.pids(node)) > 0

    def _worker(self, idx, node):
        node.account.ssh("mkdir -p %s" % IgniteClientApp.PERSISTENT_ROOT, allow_fail=False)

        self.security_config = self.kafka.security_config.client_config(node=node,
                                                                        jaas_override_variables=self.jaas_override_variables)
        self.security_config.setup_node(node)

        if self.client_prop_file_override:
            prop_file = self.client_prop_file_override
        else:
            prop_file = self.prop_file(node)

        # TODO: create ignite config: node.account.create_file(IgniteClientApp.CONFIG_FILE, prop_file)

        # Run and capture output
        cmd = self.start_cmd(node)
        self.logger.debug("Console consumer %d command: %s", idx, cmd)

        consumer_output = node.account.ssh_capture(cmd, allow_fail=False)

        # TODO: create ignite version of waiting for a app finish
        for line in consumer_output:
            msg = line.strip()
            if msg == "shutdown_complete":
                # Note that we can only rely on shutdown_complete message if running 0.10.0 or greater
                if node in self.clean_shutdown_nodes:
                    raise Exception(
                        "Unexpected shutdown event from consumer, already shutdown. Consumer index: %d" % idx)
                self.clean_shutdown_nodes.add(node)
            else:
                if self.message_validator is not None:
                    msg = self.message_validator(msg)
                if msg is not None:
                    self.messages_consumed[idx].append(msg)

    def start_node(self, node):
        BackgroundThreadService.start_node(self, node)

    def stop_node(self, node):
        self.logger.info("%s Stopping node %s" % (self.__class__.__name__, str(node.account)))
        node.account.kill_java_processes(self.java_class_name(),
                                         clean_shutdown=True, allow_fail=True)

        stopped = self.wait_node(node, timeout_sec=self.stop_timeout_sec)
        assert stopped, "Node %s: did not stop within the specified timeout of %s seconds" % \
                        (str(node.account), str(self.stop_timeout_sec))

    def clean_node(self, node):
        if self.alive(node):
            self.logger.warn("%s %s was still alive at cleanup time. Killing forcefully..." %
                             (self.__class__.__name__, node.account))
        node.account.kill_java_processes(self.java_class_name(), clean_shutdown=False, allow_fail=True)
        node.account.ssh("rm -rf %s" % IgniteClientApp.PERSISTENT_ROOT, allow_fail=False)
        self.security_config.clean_node(node)

    def java_class_name(self):
        return "org.apache.ignite.test.IgniteApplication"

    def has_log_message(self, node, message):
        try:
            node.account.ssh("grep '%s' %s" % (message, IgniteClientApp.LOG_FILE))
        except RemoteCommandError:
            return False
        return True
