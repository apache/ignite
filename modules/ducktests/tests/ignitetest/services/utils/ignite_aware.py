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
import random
import re
import signal
import sys
import time
import tempfile
from abc import ABCMeta
from datetime import datetime, timedelta
from enum import IntEnum
from pathlib import Path
from threading import Thread
from filelock import FileLock

from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.utils.util import wait_until

from ignitetest.services.utils import IgniteServiceType
from ignitetest.services.utils.background_thread import BackgroundThreadService
from ignitetest.services.utils.concurrent import CountDownLatch, AtomicValue
from ignitetest.services.utils.ignite_spec import resolve_spec, SHARED_PREPARED_FILE
from ignitetest.services.utils.jmx_utils import ignite_jmx_mixin, JmxClient
from ignitetest.services.utils.jvm_utils import JvmProcessMixin
from ignitetest.services.utils.log_utils import monitor_log
from ignitetest.services.utils.path import IgnitePathAware
from ignitetest.utils.enum import constructible


class IgniteAwareService(BackgroundThreadService, IgnitePathAware, JvmProcessMixin, metaclass=ABCMeta):
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

    def __init__(self, context, config, num_nodes, startup_timeout_sec, shutdown_timeout_sec, main_java_class, modules,
                 **kwargs):
        """
        **kwargs are params that passed to IgniteSpec
        """
        super().__init__(context, num_nodes)

        # Ducktape checks a Service implementation attribute 'logs' to get config for logging.
        # IgniteAwareService uses IgnitePersistenceAware mixin to override default Service 'log' definition.
        self.log_level = "DEBUG"

        self.config = config
        self.main_java_class = main_java_class
        self.startup_timeout_sec = startup_timeout_sec
        self.shutdown_timeout_sec = shutdown_timeout_sec
        self.modules = modules

        self.spec = resolve_spec(self, **kwargs)
        self.init_logs_attribute()

        self.disconnected_nodes = []

    @property
    def product(self):
        return str(self.config.version)

    @property
    def globals(self):
        return self.context.globals

    def start_async(self, **kwargs):
        """
        Starts in async way.
        """
        super().start(**kwargs)

    def start(self, **kwargs):
        self.start_async(**kwargs)
        self.await_started()

    def await_started(self):
        """
        Awaits start finished.
        """
        if self.config.service_type in (IgniteServiceType.NONE, IgniteServiceType.THIN_CLIENT):
            return

        self.logger.info("Waiting for IgniteAware(s) to start ...")

        self.await_event("Topology snapshot", self.startup_timeout_sec, from_the_beginning=True)

    def start_node(self, node, **kwargs):
        self.init_shared(node)
        self.init_persistent(node)

        self.__update_node_log_file(node)

        super().start_node(node, **kwargs)

        wait_until(lambda: self.alive(node), timeout_sec=10)

        ignite_jmx_mixin(node, self)

    def stop_async(self, force_stop=False, **kwargs):
        """
        Stop in async way.
        """
        super().stop(force_stop, **kwargs)

    def stop(self, force_stop=False, **kwargs):
        self.stop_async(force_stop, **kwargs)

        # Making this async on FORCE_STOP to eliminate waiting on killing services on tear down.
        # Waiting will happen on plain stop() call made by ducktape during same step.
        if not force_stop:
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

    def stop_node(self, node, force_stop=False, **kwargs):
        pids = self.pids(node, self.main_java_class)

        for pid in pids:
            node.account.signal(pid, signal.SIGKILL if force_stop else signal.SIGTERM, allow_fail=False)

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

        self._prepare_configs(node)

    def init_shared(self, node):
        """
        Init shared directory. Content of shared directory must be equal on all test nodes.
        :param node: Ignite service node.
        """
        local_shared_dir = self._init_local_shared()

        if not os.path.isdir(local_shared_dir):
            self.logger.debug("Local shared dir not exists. Nothing to copy. " + str(local_shared_dir))
            return

        node.account.mkdirs(f"{self.persistent_root} {self.shared_root}")

        for file in os.listdir(local_shared_dir):
            self.logger.debug("Copying shared file to node. " + str(file))
            node.account.copy_to(os.path.join(local_shared_dir, file), self.shared_root)

    def _init_local_shared(self):
        """
        :return: path to local share folder. Files should be copied on all nodes in `shared_root` folder.
        """
        local_dir = os.path.join(tempfile.gettempdir(), str(self.context.session_context.session_id))

        if not self.spec.is_prepare_shared_files(local_dir):
            return local_dir

        with FileLock("init_shared.lock", timeout=120):
            if self.spec.is_prepare_shared_files(local_dir):
                self.spec.prepare_shared_files(local_dir)
                Path(os.path.join(local_dir, SHARED_PREPARED_FILE)).touch()

        return local_dir

    def _prepare_configs(self, node):
        config = self.spec \
            .extend_config(self.config) \
            .prepare_for_env(self, node)

        for name, template in self.spec.config_templates():
            config_txt = template.render(service=self, config=config)

            node.account.create_file(os.path.join(self.config_dir, name), config_txt)

            self.logger.debug("Config %s for node %s: %s" % (name, node.account.hostname, config_txt))

        setattr(node, "consistent_id", node.account.externally_routable_ip)

    def worker(self, idx, node, **kwargs):
        cmd = self.spec.command(node)

        self.logger.debug("Attempting to start Application Service on %s with command: %s" % (str(node.account), cmd))

        node.account.ssh(cmd)

    def alive(self, node):
        """
        :param node: Ignite service node.
        :return: True if node is alive.
        """
        return len(self.pids(node, self.main_java_class)) > 0

    def await_event_on_node(self, evt_message, node, timeout_sec, from_the_beginning=False, backoff_sec=.1,
                            log_file=None):
        """
        Await for specific event message in a node's log file.
        :param evt_message: Event message.
        :param node: Ignite service node.
        :param timeout_sec: Number of seconds to check the condition for before failing.
        :param from_the_beginning: If True, search for message from the beginning of log file.
        :param backoff_sec: Number of seconds to back off between each failure to meet the condition
                before checking again.
        :param log_file: Explicit log file.
        """
        with monitor_log(node, os.path.join(self.log_dir, log_file) if log_file else node.log_file,
                         from_the_beginning) as monitor:
            monitor.wait_until(evt_message, timeout_sec=timeout_sec, backoff_sec=backoff_sec,
                               err_msg="Event [%s] was not triggered on '%s' in %d seconds" % (evt_message, node.name,
                                                                                               timeout_sec))

    def await_event(self, evt_message, timeout_sec, from_the_beginning=False, backoff_sec=.1, log_file=None):
        """
        Await for specific event messages on all nodes.
        :param evt_message: Event message.
        :param timeout_sec: Number of seconds to check the condition for before failing.
        :param from_the_beginning: If True, search for message from the beggining of log file.
        :param backoff_sec: Number of seconds to back off between each failure to meet the condition
                before checking again.
        :param log_file: Explicit log file.
        """
        for node in self.nodes:
            self.await_event_on_node(evt_message, node, timeout_sec, from_the_beginning=from_the_beginning,
                                     backoff_sec=backoff_sec, log_file=log_file)

    @staticmethod
    def event_time(evt_message, node):
        """
        Gets the time of specific event message in a node's log file.
        :param evt_message: Pattern to search log for.
        :param node: Ducktape node to searching log.
        :return: Time of found log message matched to pattern or None if not found.
        """
        stdout = IgniteAwareService.exec_command(node, "grep '%s' %s" % (evt_message, node.log_file))

        match = re.match("^\\[[^\\[]+\\]", stdout)

        return datetime.strptime(match.group(), "[%Y-%m-%dT%H:%M:%S,%f]") if match else None

    def get_event_time_on_node(self, node, log_pattern, from_the_beginning=True, timeout=15):
        """
        Extracts event time from ignite log by pattern .
        :param node: ducktape node to search ignite log on.
        :param log_pattern: pattern to search ignite log for.
        :param from_the_beginning: switches searching log from its beginning.
        :param timeout: timeout to wait for the patters in the log.
        """
        self.await_event_on_node(log_pattern, node, timeout, from_the_beginning=from_the_beginning, backoff_sec=0)

        return self.event_time(log_pattern, node)

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

    def __exec_on_node(self, node, task, start_waiter=None, delay_ms=0, time_holder=None):
        try:
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
        except BaseException:
            self.logger.error("async task threw exception:", exc_info=1)
            raise

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

        cm_ports = str(cm_spi.local_port) if cm_spi.local_port_range < 1 else str(cm_spi.local_port) + ':' + str(
            cm_spi.local_port + cm_spi.local_port_range)

        dsc_ports = str(dsc_spi.port) if not hasattr(dsc_spi, 'port_range') or dsc_spi.port_range < 1 else str(
            dsc_spi.port) + ':' + str(dsc_spi.port + dsc_spi.port_range)

        settings = ""

        if net_part in (IgniteAwareService.NetPart.ALL, IgniteAwareService.NetPart.INPUT):
            settings = self.__apply_iptables_settings(
                node,
                f"sudo iptables -I INPUT 1 -p tcp -m multiport --dport {dsc_ports},{cm_ports} -j DROP")

        if net_part in (IgniteAwareService.NetPart.ALL, IgniteAwareService.NetPart.OUTPUT):
            settings = self.__apply_iptables_settings(
                node,
                f"sudo iptables -I OUTPUT 1 -p tcp -m multiport --dport {dsc_ports},{cm_ports} -j DROP")

        self.logger.debug("Activated netfilter on '%s':\n%s" % (node.name, settings))

    def __apply_iptables_settings(self, node, cmd):
        # Sets given iptables settings and ensures they were applied.
        settings_before = self.__dump_netfilter_settings(node)

        out, err = IgniteAwareService.exec_command_ex(node, cmd)

        assert len(out) == 0, \
            "Unexpected iptables output on '" + node.name + "': '" + out + "'\n   Command: '" + cmd + "'."
        assert len(err) == 0, \
            "Unexpected iptables output on '" + node.name + "': '" + err + "'.\n   Command: '" + cmd + "'."

        settings = self.__dump_netfilter_settings(node)

        assert settings_before != settings, \
            "iptables settings not set on '" + node.name + "'\n   Command: '" + cmd + "'\n   ---iptables before---\n" \
            + settings_before + "\n   ---iptables after---\n" + settings

        return settings

    def __backup_iptables(self, nodes):
        # Store current network filter settings.
        for node in nodes:
            cmd = f"sudo iptables-save | tee {self.netfilter_store_path}"

            _, err = IgniteAwareService.exec_command_ex(node, cmd)

            if "Warning: iptables-legacy tables present" in err:
                cmd = f"sudo iptables-legacy-save | tee {self.netfilter_store_path}"

                _, err = IgniteAwareService.exec_command_ex(node, cmd)

            assert len(err) == 0, "Failed to store iptables rules on '%s': %s" % (node.name, err)

            self.logger.debug("Netfilter before launch on '%s': %s" % (node.name, self.__dump_netfilter_settings(node)))

            assert self.disconnected_nodes.count(node) == 0

            self.disconnected_nodes.append(node)

    def __restore_iptables(self):
        # Restore previous network filter settings.
        cmd = f"sudo iptables-restore < {self.netfilter_store_path}"

        errors = []

        for node in self.disconnected_nodes:
            settings_before = self.__dump_netfilter_settings(node)

            _, err = IgniteAwareService.exec_command_ex(node, cmd)

            settings_after = self.__dump_netfilter_settings(node)

            if len(err) > 0:
                errors.append("Failed to restore iptables rules on '%s': %s" % (node.name, err))
            elif settings_before == settings_after:
                errors.append("iptables settings not restored on '" + node.name + "':\n" + settings_after)
            else:
                self.logger.debug(
                    "Netfilter after launch on '%s':\n%s" % (node.name, self.__dump_netfilter_settings(node)))

        if len(errors) > 0:
            self.logger.error("Failed restoring actions:" + os.linesep + os.linesep.join(errors))

            raise RuntimeError("Unable to restore node states. See the log above.")

    @staticmethod
    def __dump_netfilter_settings(node):
        """
        Reads current netfilter settings on the node for debugging purposes.
        """
        out, err = IgniteAwareService.exec_command_ex(node, "sudo iptables -L -n")

        if "Warning: iptables-legacy tables present" in err:
            out, err = IgniteAwareService.exec_command_ex(node, "sudo iptables-legacy -L -n")

        assert len(err) == 0, "Failed to dump iptables rules on '%s': %s" % (node.name, err)

        return out

    def __update_node_log_file(self, node):
        """
        Update the node log file.
        """
        if not hasattr(node, 'log_file'):
            # '*' here is to support LoggerNodeIdAndApplicationAware loggers generates logs like 'ignite-367efed9.log'
            # default Ignite configuration uses o.a.i.l.l.Log4jRollingFileAppender generates such files.
            node.log_file = os.path.join(self.log_dir, "ignite*.log")

        cnt = list(node.account.ssh_capture(f'ls {self.log_dir} | '
                                            f'grep -E "^ignite.log(.[0-9]+)?$" | '
                                            f'wc -l', callback=int))[0]
        if cnt > 0:
            rotated_log = os.path.join(self.log_dir, f"ignite.log.{cnt}")
            self.logger.debug(f"rotating {node.log_file} to {rotated_log} on {node.name}")
            node.account.ssh(f"mv {node.log_file} {rotated_log}")

    @staticmethod
    def exec_command_ex(node, cmd):
        """Executes the command passed on the given node and returns out and error results as string."""
        _, out, err = node.account.ssh_client.exec_command(cmd)

        return str(out.read(), sys.getdefaultencoding()), str(err.read(), sys.getdefaultencoding())

    @staticmethod
    def exec_command(node, cmd, check_error=True):
        """Executes the command passed on the given node and returns out result as string."""
        out, err = IgniteAwareService.exec_command_ex(node, cmd)

        if check_error:
            assert len(err) == 0, f"Command failed: '{cmd}'.\nError: '{err}'"

        return out

    def thread_dump(self, node):
        """
        Generate thread dump on node.
        :param node: Ignite service node.
        """
        for pid in self.pids(node, self.main_java_class):
            try:
                node.account.signal(pid, signal.SIGQUIT, allow_fail=True)
            except RemoteCommandError:
                self.logger.warn("Could not dump threads on node")

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
            assert len(self.pids(node, self.main_java_class)) == 0

            node.account.ssh(f'rm -rf {self.database_dir}', allow_fail=False)
            node.account.ssh(f'cp -r {snapshot_db} {self.work_dir}', allow_fail=False)

    def await_rebalance(self, timeout_sec=600):
        """
        Waiting for the rebalance to complete.
        For the method, you need to set the
        metric_exporters={'org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi'}
        to the config.

        :param timeout_sec: Timeout to wait the rebalance to complete.
        """

        delta_time = datetime.now() + timedelta(seconds=timeout_sec)

        node = random.choice(self.alive_nodes)

        rebalanced = False
        mbean = JmxClient(node).find_mbean('.*name=cluster')

        while datetime.now() < delta_time and not rebalanced:
            rebalanced = next(mbean.Rebalanced) == 'true'

        if rebalanced:
            return

        raise TimeoutError(f'Rebalancing was not completed within the time: {timeout_sec} seconds.')

    @property
    def alive_nodes(self) -> list:
        """
        Alive nodes.

        :return List of alives nodes.
        """
        return [node for node in self.nodes if self.alive(node)]


def node_failed_event_pattern(failed_node_id=None):
    """Failed node pattern in log."""
    return "Node FAILED: .\\{1,\\}Node \\[id=" + (failed_node_id if failed_node_id else "") + \
           ".\\{1,\\}\\(isClient\\|client\\)=false"
