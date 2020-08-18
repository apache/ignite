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

import re

from ignitetest.services.utils.ignite_aware import IgniteAwareService
from ignitetest.utils.version import DEV_BRANCH


class IgniteApplicationService(IgniteAwareService):
    """
    The base class to build Ignite aware application written on java.
    """

    SERVICE_JAVA_CLASS_NAME = "org.apache.ignite.internal.ducktest.utils.IgniteAwareApplicationService"

    # pylint: disable=R0913
    def __init__(self, context, java_class_name, params="", properties="", timeout_sec=60, modules=None,
                 client_mode=True, version=DEV_BRANCH, servicejava_class_name=SERVICE_JAVA_CLASS_NAME,
                 jvm_opts=None, start_ignite=True):
        super(IgniteApplicationService, self).__init__(context, 1, properties,
                                                       client_mode=client_mode,
                                                       version=version,
                                                       modules=modules,
                                                       servicejava_class_name=servicejava_class_name,
                                                       java_class_name=java_class_name,
                                                       params=params,
                                                       jvm_opts=jvm_opts,
                                                       start_ignite=start_ignite)

        self.servicejava_class_name = servicejava_class_name
        self.java_class_name = java_class_name
        self.timeout_sec = timeout_sec
        self.stop_timeout_sec = 10

    def start(self):
        super(IgniteApplicationService, self).start()

        self.logger.info("Waiting for Ignite aware Application (%s) to start..." % self.java_class_name)

        self.await_event("Topology snapshot", self.timeout_sec, from_the_beginning=True)
        self.await_event("IGNITE_APPLICATION_INITIALIZED\\|IGNITE_APPLICATION_BROKEN", self.timeout_sec,
                         from_the_beginning=True)

        try:
            self.await_event("IGNITE_APPLICATION_INITIALIZED", 1, from_the_beginning=True)
        except Exception:
            raise Exception("Java application execution failed. %s" % self.extract_result("ERROR"))

    # pylint: disable=W0221
    def stop_node(self, node, clean_shutdown=True, timeout_sec=20):
        self.logger.info("%s Stopping node %s" % (self.__class__.__name__, str(node.account)))
        node.account.kill_java_processes(self.servicejava_class_name, clean_shutdown=clean_shutdown, allow_fail=True)

        stopped = self.wait_node(node, timeout_sec=self.stop_timeout_sec)
        assert stopped, "Node %s: did not stop within the specified timeout of %s seconds" % \
                        (str(node.account), str(self.stop_timeout_sec))

        self.await_event("IGNITE_APPLICATION_FINISHED\\|IGNITE_APPLICATION_BROKEN", from_the_beginning=True,
                         timeout_sec=timeout_sec)

    def clean_node(self, node):
        if self.alive(node):
            self.logger.warn("%s %s was still alive at cleanup time. Killing forcefully..." %
                             (self.__class__.__name__, node.account))

        node.account.kill_java_processes(self.servicejava_class_name, clean_shutdown=False, allow_fail=True)

        node.account.ssh("rm -rf %s" % self.PERSISTENT_ROOT, allow_fail=False)

    def pids(self, node):
        return node.account.java_pids(self.servicejava_class_name)

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
