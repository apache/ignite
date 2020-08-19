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

from abc import abstractmethod, ABCMeta

from ducktape.services.background_thread import BackgroundThreadService
from ducktape.utils.util import wait_until
from six import add_metaclass

from ignitetest.services.utils.ignite_spec import resolve_spec
from ignitetest.services.utils.jmx_utils import ignite_jmx_mixin
from ignitetest.services.utils.ignite_persistence import IgnitePersistenceAware


@add_metaclass(ABCMeta)
class IgniteAwareService(BackgroundThreadService, IgnitePersistenceAware):
    """
    The base class to build services aware of Ignite.
    """

    # pylint: disable=R0913
    def __init__(self, context, num_nodes, properties, **kwargs):
        """
        **kwargs are params that passed to IgniteSpec
        """
        super(IgniteAwareService, self).__init__(context, num_nodes)

        # Ducktape checks a Service implementation attribute 'logs' to get config for logging.
        # IgniteAwareService uses IgnitePersistenceAware mixin to override default Service 'log' definition.
        self.log_level = "DEBUG"
        self.logs = IgnitePersistenceAware.logs

        self.properties = properties

        self.spec = resolve_spec(self, context, **kwargs)

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
        super(IgniteAwareService, self).init_persistent(node)

        node_config = self.spec.config().render(config_dir=self.PERSISTENT_ROOT,
                                                work_dir=self.WORK_DIR,
                                                properties=self.properties,
                                                consistent_id=node.account.externally_routable_ip)

        setattr(node, "consistent_id", node.account.externally_routable_ip)
        node.account.create_file(self.CONFIG_FILE, node_config)

    @abstractmethod
    def pids(self, node):
        """
        :param node: Ignite service node.
        :return: List of service's pids.
        """
        raise NotImplementedError

    # pylint: disable=W0613
    def _worker(self, idx, node):
        cmd = self.spec.command()

        self.logger.debug("Attempting to start Application Service on %s with command: %s" % (str(node.account), cmd))

        node.account.ssh(cmd)

    def alive(self, node):
        """
        :param node: Ignite service node.
        :return: True if node is alive.
        """
        return len(self.pids(node)) > 0

    def await_event_on_node(self, evt_message, node, timeout_sec, from_the_beginning=False, backoff_sec=5):
        """
        Await for specific event message in a node's log file.
        :param evt_message: Event message.
        :param node: Ignite service node.
        :param timeout_sec: Number of seconds to check the condition for before failing.
        :param from_the_beginning: If True, search for message from the beginning of log file.
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
        for node in self.nodes:
            self.await_event_on_node(evt_message, node, timeout_sec, from_the_beginning=from_the_beginning,
                                     backoff_sec=backoff_sec)
