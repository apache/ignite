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
import re
import signal
import socket
import sys
import time
from abc import abstractmethod, ABCMeta
from datetime import datetime
from enum import IntEnum
from threading import Thread

from ducktape.utils.util import wait_until

from ignitetest.services.utils.background_thread import BackgroundThreadService
from ignitetest.services.utils.concurrent import CountDownLatch, AtomicValue
from ignitetest.services.utils.ducktests_service import DucktestsService
from ignitetest.services.utils.ignite_spec import resolve_spec
from ignitetest.services.utils.jmx_utils import ignite_jmx_mixin
from ignitetest.services.utils.log_utils import monitor_log
from ignitetest.services.utils.path import IgnitePathAware
# pylint: disable=too-many-public-methods
from ignitetest.services.utils.ssl.connector_configuration import ConnectorConfiguration
from ignitetest.services.utils.ssl.ssl_params import get_ssl_params, is_ssl_enabled, IGNITE_SERVER_ALIAS, \
    IGNITE_CLIENT_ALIAS
from ignitetest.utils.enum import constructible


class IgniteAwareService(BackgroundThreadService, IgnitePathAware, metaclass=ABCMeta):
    """
    The base class to build services aware of Ignite.
    """
    @constructible
    class NetPart(IntEnum):
        """
        Network part to emulate failure.
        """
        INPUT = 0
        OUTPUT = 1
        ALL = 2

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
        self.init_logs_attribute()

        self.disconnected_nodes = []

    @property
    def version(self):
        return self.config.version

    @property
    def project(self):
        return self.spec.project

    @property
    def globals(self):
        return self.context.globals

    def start_async(self, **kwargs):
        """
        Starts in async way.
        """
        self.update_ssl_config_with_globals()
        super().start(**kwargs)

    def start(self, **kwargs):
        self.start_async(**kwargs)
        self.await_started()

    def update_ssl_config_with_globals(self):
        """
        Update ssl configuration from globals.
        """
        ssl_params = None
        if self.config.ssl_params is None and is_ssl_enabled(self.globals):
            ssl_params = get_ssl_params(
                self.globals,
                IGNITE_CLIENT_ALIAS if self.config.client_mode else IGNITE_SERVER_ALIAS
            )
        if ssl_params:
            self.config = self.config._replace(ssl_params=ssl_params,
                                               connector_configuration=ConnectorConfiguration(
                                                   ssl_enabled=True, ssl_params=ssl_params))

    def await_started(self):
        """
        Awaits start finished.
        """
        self.logger.info("Waiting for IgniteAware(s) to start ...")

        self.await_event("Topology snapshot", self.startup_timeout_sec, from_the_beginning=True)

    def start_node(self, node, **kwargs):
        self.init_persistent(node)

        self.__update_node_log_file(node)

        super().start_node(node, **kwargs)

        wait_until(lambda: self.alive(node), timeout_sec=10)

        ignite_jmx_mixin(node, self.spec, self.pids(node))

    def stop_async(self, **kwargs):
        """
        Stop in async way.
        """
        super().stop(**kwargs)

    def stop(self, **kwargs):
        self.stop_async(**kwargs)

        # Making this async on FORCE_STOP to eliminate waiting on killing services on tear down.
        # Waiting will happen on plain stop() call made by ducktape during same step.
        if not kwargs.get(DucktestsService.FORCE_STOP, False):
            self.await_stopped()

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

    def stop_node(self, node, **kwargs):
        pids = self.pids(node)

        for pid in pids:
            node.account.signal(pid,
                                signal.SIGKILL if kwargs.get(DucktestsService.FORCE_STOP, False) else signal.SIGTERM,
                                allow_fail=False)

    def clean(self, **kwargs):
        self.__restore_iptables()

        super().clean(**kwargs)

    def clean_node(self, node, **kwargs):
        super().clean_node(node, **kwargs)

        node.account.ssh("rm -rf -- %s" % self.persistent_root, allow_fail=False)

    def init_persistent(self, node):
        """
        Init persistent directory.
        :param node: Ignite service node.
        """
        super().init_persistent(node)

        node_config = self._prepare_config(node)

        node.account.create_file(self.config_file, node_config)

    def _prepare_config(self, node):
        if not self.config.consistent_id:
            config = self.config._replace(consistent_id=node.account.externally_routable_ip)
        else:
            config = self.config

        config = config._replace(local_host=socket.gethostbyname(node.account.hostname))

        config.discovery_spi.prepare_on_start(cluster=self)

        node_config = self.spec.config_template.render(config_dir=self.persistent_root, work_dir=self.work_dir,
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
    def worker(self, idx, node, **kwargs):
        cmd = self.spec.command(node)

        self.logger.debug("Attempting to start Application Service on %s with command: %s" % (str(node.account), cmd))

        node.account.ssh(cmd)

    def alive(self, node):
        """
        :param node: Ignite service node.
        :return: True if node is alive.
        """
        return len(self.pids(node)) > 0

    @staticmethod
    def await_event_on_node(evt_message, node, timeout_sec, from_the_beginning=False, backoff_sec=5):
        """
        Await for specific event message in a node's log file.
        :param evt_message: Event message.
        :param node: Ignite service node.
        :param timeout_sec: Number of seconds to check the condition for before failing.
        :param from_the_beginning: If True, search for message from the beginning of log file.
        :param backoff_sec: Number of seconds to back off between each failure to meet the condition
                before checking again.
        """
        with monitor_log(node, node.log_file, from_the_beginning) as monitor:
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

    @staticmethod
    def event_time(evt_message, node):
        """
        Gets the time of specific event message in a node's log file.
        :param evt_message: Pattern to search log for.
        :param node: Ducktape node to searching log.
        :return: Time of found log message matched to pattern or None if not found.
        """
        _, stdout, _ = node.account.ssh_client.exec_command(
            "grep '%s' %s" % (evt_message, node.log_file))

        match = re.match("^\\[[^\\[]+\\]", stdout.read().decode("utf-8"))

        return datetime.strptime(match.group(), "[%Y-%m-%d %H:%M:%S,%f]") if match else None

    def get_event_time(self, evt_message, selector=max):
        """
        Gets the time of the specific event from all nodes, using selector.
        :param evt_message: Event message.
        :param selector: Selector function, default is max.
        :return: Minimal event time.
        """
        return selector(filter(lambda t: t is not None,
                               map(lambda node: self.event_time(evt_message, node), self.nodes)), default=None)

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

    @property
    def netfilter_store_path(self):
        """
        :return: path to store backup of iptables filter
        """
        return os.path.join(self.temp_dir, "iptables.bak")

    def drop_network(self, nodes=None, net_part: NetPart = NetPart.ALL):
        """
        Disconnects node from cluster.
        :param nodes: Nodes to emulate network failure on.
        :param net_part: Part of network to emulate failure of.
        """
        if nodes is None:
            assert self.num_nodes == 1
            nodes = self.nodes

        for node in nodes:
            self.logger.info("Dropping " + str(net_part) + " Ignite connections on '" + node.account.hostname + "' ...")

        self.__backup_iptables(nodes)

        return self.exec_on_nodes_async(nodes, lambda n: self.__enable_netfilter(n, net_part))

    def __enable_netfilter(self, node, net_part: NetPart):
        cm_spi = self.config.communication_spi
        dsc_spi = self.config.discovery_spi

        cm_ports = str(cm_spi.port) if cm_spi.port_range < 1 else str(cm_spi.port) + ':' + str(
            cm_spi.port + cm_spi.port_range)

        dsc_ports = str(dsc_spi.port) if not hasattr(dsc_spi, 'port_range') or dsc_spi.port_range < 1 else str(
            dsc_spi.port) + ':' + str(dsc_spi.port + dsc_spi.port_range)

        if net_part in (IgniteAwareService.NetPart.ALL, IgniteAwareService.NetPart.INPUT):
            node.account.ssh_client.exec_command(
                f"sudo iptables -I INPUT 1 -p tcp -m multiport --dport {dsc_ports},{cm_ports} -j DROP")

        if net_part in (IgniteAwareService.NetPart.ALL, IgniteAwareService.NetPart.OUTPUT):
            node.account.ssh_client.exec_command(
                f"sudo iptables -I OUTPUT 1 -p tcp -m multiport --dport {dsc_ports},{cm_ports} -j DROP")

        self.logger.debug("Activated netfilter on '%s': %s" % (node.name, self.__dump_netfilter_settings(node)))

    def __backup_iptables(self, nodes):
        # Store current network filter settings.
        for node in nodes:
            cmd = f"sudo iptables-save | tee {self.netfilter_store_path}"

            exec_error = str(node.account.ssh_client.exec_command(cmd)[2].read(), sys.getdefaultencoding())

            if "Warning: iptables-legacy tables present" in exec_error:
                cmd = f"sudo iptables-legacy-save | tee {self.netfilter_store_path}"

                exec_error = str(node.account.ssh_client.exec_command(cmd)[2].read(), sys.getdefaultencoding())

            assert len(exec_error) == 0, "Failed to store iptables rules on '%s': %s" % (node.name, exec_error)

            self.logger.debug("Netfilter before launch on '%s': %s" % (node.name, self.__dump_netfilter_settings(node)))

            assert self.disconnected_nodes.count(node) == 0

            self.disconnected_nodes.append(node)

    def __restore_iptables(self):
        # Restore previous network filter settings.
        cmd = f"sudo iptables-restore < {self.netfilter_store_path}"

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

    def __update_node_log_file(self, node):
        """
        Update the node log file.
        """
        if not hasattr(node, 'log_file'):
            node.log_file = os.path.join(self.log_dir, "console.log")

        cnt = list(node.account.ssh_capture(f'ls {self.log_dir} | '
                                            f'grep -E "^console.log(.[0-9]+)?$" | '
                                            f'wc -l', callback=int))[0]
        if cnt > 0:
            rotated_log = os.path.join(self.log_dir, f"console.log.{cnt}")
            self.logger.debug(f"rotating {node.log_file} to {rotated_log} on {node.name}")
            node.account.ssh(f"mv {node.log_file} {rotated_log}")

    @staticmethod
    def exec_command(node, cmd):
        """Executes the command passed on the given node and returns result as string."""
        return str(node.account.ssh_client.exec_command(cmd)[1].read(), sys.getdefaultencoding())

    @staticmethod
    def node_id(node):
        """
        Returns node id from its log if started.
        This is a remote call. Reuse its results if possible.
        """
        regexp = "^>>> Local node \\[ID=([^,]+),.+$"
        cmd = "grep -E '%s' %s | sed -r 's/%s/\\1/'" % (regexp, node.log_file, regexp)

        return IgniteAwareService.exec_command(node, cmd).strip().lower()

    def restore_from_snapshot(self, snapshot_name: str):
        """
        Restore from snapshot.
        :param snapshot_name: Name of Snapshot.
        """
        snapshot_db = os.path.join(self.snapshots_dir, snapshot_name, "db")

        for node in self.nodes:
            assert len(self.pids(node)) == 0

            node.account.ssh(f'rm -rf {self.database_dir}', allow_fail=False)
            node.account.ssh(f'cp -r {snapshot_db} {self.work_dir}', allow_fail=False)
