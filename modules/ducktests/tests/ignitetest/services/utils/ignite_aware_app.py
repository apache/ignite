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
This module contains the base class to build Ignite aware application written on java.
"""

import base64
import json
import re

from ignitetest.services.utils.ignite_aware import IgniteAwareService


class IgniteAwareApplicationService(IgniteAwareService):
    """
    The base class to build Ignite aware application written on java.
    """

    # pylint: disable=R0913
    def __init__(self, context, java_class_name, modules, client_mode, version, properties, params, timeout_sec,
                 service_java_class_name="org.apache.ignite.internal.ducktest.utils.IgniteAwareApplicationService"):
        super(IgniteAwareApplicationService, self).__init__(context, 1, modules, client_mode, version, properties)

        self.servicejava_class_name = service_java_class_name
        self.java_class_name = java_class_name
        self.timeout_sec = timeout_sec
        self.stop_timeout_sec = 10
        self.params = params

    def start(self):
        super(IgniteAwareApplicationService, self).start()

        self.logger.info("Waiting for Ignite aware Application (%s) to start..." % self.java_class_name)

        self.await_event("Topology snapshot", self.timeout_sec, from_the_beginning=True)
        self.await_event("IGNITE_APPLICATION_INITIALIZED", self.timeout_sec, from_the_beginning=True)

    def start_cmd(self, node):
        cmd = self.env()
        cmd += "%s %s %s 1>> %s 2>> %s &" % \
               (self.path.script("ignite.sh"),
                self.jvm_opts(),
                self.app_args(),
                self.STDOUT_STDERR_CAPTURE,
                self.STDOUT_STDERR_CAPTURE)
        return cmd

    # pylint: disable=W0221
    def stop_node(self, node, clean_shutdown=True, timeout_sec=20):
        self.logger.info("%s Stopping node %s" % (self.__class__.__name__, str(node.account)))
        node.account.kill_java_processes(self.servicejava_class_name, clean_shutdown=clean_shutdown, allow_fail=True)

        stopped = self.wait_node(node, timeout_sec=self.stop_timeout_sec)
        assert stopped, "Node %s: did not stop within the specified timeout of %s seconds" % \
                        (str(node.account), str(self.stop_timeout_sec))

        self.await_event("IGNITE_APPLICATION_FINISHED", from_the_beginning=True, timeout_sec=timeout_sec)

    def clean_node(self, node):
        if self.alive(node):
            self.logger.warn("%s %s was still alive at cleanup time. Killing forcefully..." %
                             (self.__class__.__name__, node.account))

        node.account.kill_java_processes(self.servicejava_class_name, clean_shutdown=False, allow_fail=True)

        node.account.ssh("rm -rf %s" % self.PERSISTENT_ROOT, allow_fail=False)

    def app_args(self):
        """
        :return: Application arguments.
        """
        args = self.java_class_name + "," + IgniteAwareApplicationService.CONFIG_FILE

        if self.params != "":
            args += "," + str(base64.b64encode(json.dumps(self.params).encode("UTF-8")))

        return args

    def pids(self, node):
        return node.account.java_pids(self.servicejava_class_name)

    def jvm_opts(self):
        """
        :return: Application JVM options.
        """
        return "-J-DIGNITE_SUCCESS_FILE=" + self.PERSISTENT_ROOT + "/success_file " + \
               "-J-Dlog4j.configDebug=true " \
               "-J-Xmx1G " \
               "-J-ea " \
               "-J-DIGNITE_ALLOW_ATOMIC_OPS_IN_TX=false " + self.jvm_options

    def env(self):
        """
        :return: Export string of additional environment variables.
        """
        if not self.version.is_dev:
            # Jackson requred to parse application params at java side. Release's version should be used.
            for line in self.nodes[0].account.ssh_capture(
                    "ls -d %s/libs/optional/ignite-aws/* | grep jackson | tr '\n' ':' | sed 's/.$//'" % self.path.home):
                self.user_libs.extend([line])

        return "export MAIN_CLASS={main_class}; ".format(main_class=self.servicejava_class_name) + \
               "export EXCLUDE_TEST_CLASSES=true; " + \
               "export IGNITE_LOG_DIR={log_dir}; ".format(log_dir=self.PERSISTENT_ROOT) + \
               "export USER_LIBS=%s:/opt/ignite-dev/modules/ducktests/target/*; " % (":".join(self.user_libs))

    def extract_result(self, name):
        """
        :param name: Result parameter's name.
        :return: Extracted result of application run.
        """
        res = ""

        output = self.nodes[0].account.ssh_capture(
            "grep '%s' %s" % (name + "->", self.STDOUT_STDERR_CAPTURE), allow_fail=False)

        for line in output:
            res = re.search("%s(.*)%s" % (name + "->", "<-"), line).group(1)

        return res
