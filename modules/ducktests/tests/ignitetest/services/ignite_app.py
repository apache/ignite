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
import os
import re

from ducktape.errors import TimeoutError
from ducktape.utils.util import wait_until

from ignitetest.services.ignite_execution_exception import IgniteExecutionException
from ignitetest.services.utils.ignite_aware import IgniteAwareService
from ignitetest.services.utils.ignite_configuration import CustomApplicationConfiguration
from ignitetest.services.utils.jmx_utils import ignite_jmx_mixin


class IgniteApplicationService(IgniteAwareService):
    """
    The base class to build Ignite aware application written on java.
    """

    SERVICE_JAVA_CLASS_NAME = "org.apache.ignite.internal.ducktest.utils.IgniteAwareApplicationService"
    APP_INIT_EVT_MSG = "IGNITE_APPLICATION_INITIALIZED"
    APP_FINISH_EVT_MSG = "IGNITE_APPLICATION_FINISHED"
    APP_BROKEN_EVT_MSG = "IGNITE_APPLICATION_BROKEN"

    def __init__(self, context, config, java_class_name, num_nodes=1, params="", startup_timeout_sec=60,
                 shutdown_timeout_sec=60, modules=None, main_java_class=SERVICE_JAVA_CLASS_NAME, jvm_opts=None,
                 merge_with_default=True):
        super().__init__(context, config, num_nodes, startup_timeout_sec, shutdown_timeout_sec, main_java_class,
                         modules, jvm_opts=jvm_opts, merge_with_default=merge_with_default)

        self.java_class_name = java_class_name
        self.params = params

    def await_started(self):
        super().await_started()

        self.__check_status(self.APP_INIT_EVT_MSG, timeout=self.startup_timeout_sec)

    def await_stopped(self):
        super().await_stopped()

        self.__check_status(self.APP_FINISH_EVT_MSG)

    def __check_status(self, desired, timeout=1):
        self.await_event("%s\\|%s" % (desired, self.APP_BROKEN_EVT_MSG), timeout, from_the_beginning=True)

        try:
            self.await_event(self.APP_BROKEN_EVT_MSG, 1, from_the_beginning=True)
            raise IgniteExecutionException("Java application execution failed. %s" % self.extract_result("ERROR"))
        except TimeoutError:
            pass

        try:
            self.await_event(desired, 1, from_the_beginning=True)
        except Exception:
            raise Exception("Java application execution failed.") from None

    def get_init_time(self, selector=min):
        """
        Gets the time of application init event.
        :param selector: Selector function, default is min.
        :return: Application initialization time.
        """
        return self.get_event_time(self.APP_INIT_EVT_MSG, selector=selector)

    def get_finish_time(self, selector=max):
        """
        Gets the time of application finish event.
        :param selector: Selector function, default is max.
        :return: Application finish time.
        """
        return self.get_event_time(self.APP_FINISH_EVT_MSG, selector=selector)

    def extract_result(self, name):
        """
        :param name: Result parameter's name.
        :return: Extracted result of application run.
        """
        results = self.extract_results(name)

        assert len(results) == len(self.nodes), f"Expected exactly {len(self.nodes)} occurence," \
                                                f" but found {len(results)}."

        return results[0] if results else ""

    def extract_results(self, name):
        """
        :param name: Results parameter's name.
        :return: Extracted results of application run.
        """
        res = []

        for node in self.nodes:
            output = node.account.ssh_capture(
                "grep '%s' %s" % (name + "->", node.log_file), allow_fail=False)
            for line in output:
                res.append(re.search("%s(.*)%s" % (name + "->", "<-"), line).group(1))

        return res


class IgniteCustomApplicationService(IgniteApplicationService):
    """
    The base class to build Ignite aware application written on java.
    """

    APP_INIT_EVT_MSG = "Topology snapshot"
    APP_FINISH_EVT_MSG = "IGNITE_APPLICATION_FINISHED"
    APP_BROKEN_EVT_MSG = "Exception in thread \"main\""

    def __init__(self, context, main_java_class, config=CustomApplicationConfiguration(), java_class_name='',
                 num_nodes=1, params="", startup_timeout_sec=60, shutdown_timeout_sec=60, modules=None,
                 jvm_opts=None, merge_with_default=True):
        super().__init__(context=context, config=config, modules=modules, main_java_class=main_java_class,
                         startup_timeout_sec=startup_timeout_sec, shutdown_timeout_sec=shutdown_timeout_sec,
                         jvm_opts=jvm_opts, merge_with_default=merge_with_default, num_nodes=num_nodes,
                         java_class_name=java_class_name)

        self.java_class_name = java_class_name
        self.params = params

    def __update_node_log_file(self, node):
        """
        Update the node log file.
        """
        if not hasattr(node, 'log_file'):
            node.log_file = os.path.join(self.log_dir, "console.log")

    def start_node(self, node, **kwargs):
        self.init_shared(node)
        self.init_persistent(node)

        self.__update_node_log_file(node)

        super().start_node(node, **kwargs)

        wait_until(lambda: self.alive(node), timeout_sec=10)

        ignite_jmx_mixin(node, self)
