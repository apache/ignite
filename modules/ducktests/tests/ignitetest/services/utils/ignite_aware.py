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
import signal
import socket
import sys
import time
from abc import abstractmethod, ABCMeta
from datetime import datetime
from threading import Thread

from ducktape.services.background_thread import BackgroundThreadService
from ducktape.utils.util import wait_until

from ignitetest.services.utils.concurrent import CountDownLatch, AtomicValue
from ignitetest.services.utils.ignite_persistence import IgnitePersistenceAware
from ignitetest.services.utils.ignite_spec import resolve_spec
from ignitetest.services.utils.jmx_utils import ignite_jmx_mixin
from ignitetest.services.utils.log_utils import monitor_log


class IgniteAwareService(BackgroundThreadService, IgnitePersistenceAware, metaclass=ABCMeta):
    """
    The base class to build services aware of Ignite.
    """

    NETFILTER_STORE_PATH = os.path.join(IgnitePersistenceAware.TEMP_DIR, "iptables.bak")

    # pylint: disable=R0913
    def __init__(self, context, config, num_nodes, startup_timeout_sec, shutdown_timeout_sec, **kwargs):
        """
        **kwargs are params that passed to IgniteSpec
        """
        super().__init__(context, num_nodes)

        # Ducktape checks a Service implementation attribute 'logs' to get config for logging.
        # IgniteAwareService uses IgnitePersistenceAware mixin to override default Service 'log' definition.
        self.log_level = "DEBUG"

        self.config = config
        self.startup_timeout_sec = startup_timeout_sec
        self.shutdown_timeout_sec = shutdown_timeout_sec

        self.spec = resolve_spec(self, context, config, **kwargs)

        self.disconnected_nodes = []
        self.killed = False

    def start_async(self):
        """
        Starts in async way.
        """
        super().start()

    def start(self):
        self.start_async()
        self.await_started()

    def await_started(self):
        """
        Awaits start finished.
        """
        self.logger.info("Waiting for IgniteAware(s) to start ...")

        self.await_event("Topology snapshot", self.startup_timeout_sec, from_the_beginning=True)

    def start_node(self, node):
        self.init_persistent(node)

        super().start_node(node)

        wait_until(lambda: self.alive(node), timeout_sec=10)

        ignite_jmx_mixin(node, self.pids(node))

    def stop_async(self):
        """
        Stop in async way.
        """
        super().stop()

    def stop(self):
        if not self.killed:
            self.stop_async()
            self.await_stopped()
        else:
            self.logger.debug("Skipping node stop since it already killed.")

    def await_stopped(self):
        """
        Awaits stop finished.
        """
        self.logger.info("Waiting for IgniteAware(s) to stop ...")

        for node in self.nodes:
            stopped = self.wait_node(node, timeout_sec=self.shutdown_timeout_sec)
            assert stopped, "Node %s's worker thread did not stop in %d seconds" % \
                            (str(node.account), self.shutdown_timeout_sec)

        for node in self.nodes:
            wait_until(lambda: not self.alive(node), timeout_sec=self.shutdown_timeout_sec,
                       err_msg="Node %s's remote processes failed to stop in %d seconds" %
                               (str(node.account), self.shutdown_timeout_sec))

    def stop_node(self, node):
        pids = self.pids(node)

        for pid in pids:
            node.account.signal(pid, signal.SIGTERM, allow_fail=False)

    def kill(self):
        """
        Kills nodes.
        """
        self.logger.info("Killing IgniteAware(s) ...")

        for node in self.nodes:
            pids = self.pids(node)

            for pid in pids:
                node.account.signal(pid, signal.SIGKILL, allow_fail=False)

        for node in self.nodes:
            wait_until(lambda: not self.alive(node), timeout_sec=self.shutdown_timeout_sec,
                       err_msg="Node %s's remote processes failed to be killed in %d seconds" %
                               (str(node.account), self.shutdown_timeout_sec))

        self.killed = True

    def clean(self):
        self.__restore_iptables()

        super().clean()

    def init_persistent(self, node):
        """
        Init persistent directory.
        :param node: Ignite service node.
        """
        super().init_persistent(node)

        node_config = self._prepare_config(node)

        node.account.create_file(self.CONFIG_FILE, node_config)

    def _prepare_config(self, node):
        if not self.config.consistent_id:
            config = self.config._replace(consistent_id=node.account.externally_routable_ip)
        else:
            config = self.config

        config.discovery_spi.prepare_on_start(cluster=self)

        config.discovery_spi.local_address = socket.gethostbyname(node.account.hostname)

        node_config = self.spec.config_template.render(config_dir=self.PERSISTENT_ROOT, work_dir=self.WORK_DIR,
                                                       config=config)

        setattr(node, "consistent_id", node.account.externally_routable_ip)

        self.logger.debug("Config for node %s: %s" % (node.account.hostname, node_config))

        return node_config

    @abstractmethod
    def pids(self, node):
        """
        :param node: Ignite service node.
        :return: List of service's pids.
        """
        raise NotImplementedError

    # pylint: disable=W0613
    def _worker(self, idx, node):
        cmd = self.spec.command

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
        with monitor_log(node, self.STDOUT_STDERR_CAPTURE, from_the_beginning) as monitor:
            monitor.wait_until(evt_message, timeout_sec=timeout_sec, backoff_sec=backoff_sec,
                               err_msg="Event [%s] was not triggered on '%s' in %d seconds" % (evt_message, node.name,
                                                                                               timeout_sec))

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

    def exec_on_nodes_async(self, nodes, task, simultaneously=True, delay_ms=0, timeout_sec=20):
        """
        Executes given task on the nodes.
        :param task: a 'lambda: node'.
        :param simultaneously: Enables or disables simultaneous start of the task on each node.
        :param delay_ms: delay before task run. Begins with 0, grows by delay_ms for each next node in nodes.
        :param timeout_sec: timeout to wait the task.
        """
        sem = CountDownLatch(len(nodes)) if simultaneously else None
        time_holder = AtomicValue()

        delay = 0
        threads = []

        for node in nodes:
            thread = Thread(target=self.__exec_on_node, args=(node, task, sem, delay, time_holder))

            threads.append(thread)

            thread.start()

            delay += delay_ms

        for thread in threads:
            thread.join(timeout_sec)

        return time_holder.get()

    @staticmethod
    def __exec_on_node(node, task, start_waiter=None, delay_ms=0, time_holder=None):
        if start_waiter:
            start_waiter.count_down()
            start_waiter.wait()

        if delay_ms > 0:
            time.sleep(delay_ms / 1000.0)

        if time_holder:
            mono = time.monotonic()
            timestamp = datetime.now()

            time_holder.compare_and_set(None, (mono, timestamp))

        task(node)

    def drop_network(self, nodes=None):
        """
        Disconnects node from cluster.
        """
        if nodes is None:
            assert self.num_nodes == 1
            nodes = self.nodes

        for node in nodes:
            self.logger.info("Disconnecting " + node.account.hostname + ".")

        self.__backup_iptables(nodes)

        cm_spi = self.config.communication_spi
        dsc_spi = self.config.discovery_spi

        cm_ports = str(cm_spi.port) if cm_spi.port_range < 1 else str(cm_spi.port) + ':' + str(
            cm_spi.port + cm_spi.port_range)

        dsc_ports = str(dsc_spi.port) if not hasattr(dsc_spi, 'port_range') or dsc_spi.port_range < 1 else str(
            dsc_spi.port) + ':' + str(dsc_spi.port + dsc_spi.port_range)

        cmd = f"sudo iptables -I %s 1 -p tcp -m multiport --dport {dsc_ports},{cm_ports} -j DROP"

        for node in nodes:
            self.logger.debug("Activating netfilter on '%s': %s" % (node.name, self.__dump_netfilter_settings(node)))

        return self.exec_on_nodes_async(nodes,
                                        lambda n: (n.account.ssh_client.exec_command(cmd % "INPUT"),
                                                   n.account.ssh_client.exec_command(cmd % "OUTPUT")))

    def __backup_iptables(self, nodes):
        # Store current network filter settings.
        for node in nodes:
            cmd = "sudo iptables-save | tee " + IgniteAwareService.NETFILTER_STORE_PATH

            exec_error = str(node.account.ssh_client.exec_command(cmd)[2].read(), sys.getdefaultencoding())

            if "Warning: iptables-legacy tables present" in exec_error:
                cmd = "sudo iptables-legacy-save | tee " + IgniteAwareService.NETFILTER_STORE_PATH

                exec_error = str(node.account.ssh_client.exec_command(cmd)[2].read(), sys.getdefaultencoding())

            assert len(exec_error) == 0, "Failed to store iptables rules on '%s': %s" % (node.name, exec_error)

            self.logger.debug("Netfilter before launch on '%s': %s" % (node.name, self.__dump_netfilter_settings(node)))

            assert self.disconnected_nodes.count(node) == 0

            self.disconnected_nodes.append(node)

    def __restore_iptables(self):
        # Restore previous network filter settings.
        cmd = "sudo iptables-restore < " + IgniteAwareService.NETFILTER_STORE_PATH

        errors = []

        for node in self.disconnected_nodes:
            exec_error = str(node.account.ssh_client.exec_command(cmd)[2].read(), sys.getdefaultencoding())

            if len(exec_error) > 0:
                errors.append("Failed to restore iptables rules on '%s': %s" % (node.name, exec_error))
            else:
                self.logger.debug(
                    "Netfilter after launch on '%s': %s" % (node.name, self.__dump_netfilter_settings(node)))

        if len(errors) > 0:
            self.logger.error("Failed restoring actions:" + os.linesep + os.linesep.join(errors))

            raise RuntimeError("Unable to restore node states. See the log above.")

    @staticmethod
    def __dump_netfilter_settings(node):
        """
        Reads current netfilter settings on the node for debugging purposes.
        """
        return str(node.account.ssh_client.exec_command("sudo iptables -L -n")[1].read(), sys.getdefaultencoding())
