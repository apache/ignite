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
Fills one volume of a persistent server node and checks what the node does.
"""
import os
import time

from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, IgniteThinClientConfiguration, \
    DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.services.utils.ignite_spec import envs_to_exports
from ignitetest.services.utils.path import IgnitePathAware
from ignitetest.services.utils.ssl.client_connector_configuration import ClientConnectorConfiguration
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion

MB = 1024 * 1024

FILLER = "filler"

STDOUT = "ignite.out"

STDERR = "ignite.err"


class StockLogConfigTemplate:
    """
    The distribution's own config/ignite-log4j.xml, with its log directory, ${sys:IGNITE_HOME}/work/log, moved to the
    service's log directory. The file is read on the test driver, which, like every ducker container, has the
    distributions under install_root.
    """
    @staticmethod
    def render(service, **kwargs):
        """
        :param service: Ignite service the config is for.
        :return: Text of the log4j2 config.
        """
        with open(os.path.join(service.home_dir, "config", "ignite-log4j.xml"), encoding="utf-8") as stock:
            return stock.read().replace("${sys:IGNITE_HOME}/work/log", service.log_dir)


def production_launch_spec(base_spec):
    """
    :param base_spec: Spec class to inherit from: the service's own, so that a NodeSpec set in globals still applies.
    :return: Spec class that starts a server node the way a production setup does: with the distribution's own
             config/ignite-log4j.xml, and with stdout and stderr redirected to files in the log directory instead of
             piped through tee into console.log. A JVM that fails to start leaves its output in those files.
    """
    class ProductionLaunchSpec(base_spec):
        """
        Starts a server node the way a production setup does.
        """
        def config_templates(self):
            return [(name, StockLogConfigTemplate() if name == IgnitePathAware.IGNITE_LOG_CONFIG_NAME else template)
                    for name, template in super().config_templates()]

        def command(self, node):
            return "%s %s %s %s > %s 2> %s &" % \
                   (envs_to_exports(self.envs()),
                    self.service.script("ignite.sh"),
                    self._jvm_opts(),
                    self.config_file_path(),
                    os.path.join(self.service.log_dir, STDOUT),
                    os.path.join(self.service.log_dir, STDERR))

    return ProductionLaunchSpec


class DiskFullTest(IgniteTest):
    """
    Each test mounts a size-limited tmpfs over a directory of a persistent server node, runs a thin-client load,
    fills the volume and checks what the node does. The tests record today's behavior: which full volume a node
    survives and which one stops it.
    """
    LOAD_APP = "org.apache.ignite.internal.ducktest.tests.disk_full.DiskFullLoadApplication"

    @cluster(num_nodes=2)
    @ignite_versions(str(DEV_BRANCH))
    @matrix(layout=["separate", "separate_jfr", "shared_with_db"])
    def test_log_volume_full(self, ignite_version, layout):
        """
        Fills the volume holding the node's logs: its log4j2 files, stdout, stderr and gc.log. The node starts the way
        a production setup does (production_launch_spec).
        - separate: the logs alone on a 32 MB volume. The node goes on serving puts and control.sh answers. The
          node's log stops growing while the volume is full and grows again once space is freed.
        - separate_jfr: the same volume, plus a JFR recording with its repository in the log directory. The JVM
          exits.
        - shared_with_db: one 256 MB volume holds the logs and the work directory with the partitions, the WAL and
          its archive. The node stops, as with a full data volume. Whether the reason reaches the log depends on
          the space left at that moment, so the test checks only that the process ends.
        """
        ignite = IgniteService(self.test_context, self.__server_config(ignite_version), num_nodes=1)

        jfr_opts = []

        if layout == "separate_jfr":
            jfr_opts = [f"-XX:StartFlightRecording=filename={ignite.log_dir}/recording.jfr",
                        f"-XX:FlightRecorderOptions=repository={ignite.log_dir}/jfr"]

        ignite.spec = production_launch_spec(ignite.spec.__class__)(
            service=ignite, jvm_opts=ignite.spec.user_jvm_opts + jfr_opts,
            merge_with_default=ignite.spec.merge_with_default)

        if layout == "shared_with_db":
            ignite.mount_tmpfs(ignite.persistent_root, 256)
        else:
            ignite.mount_tmpfs(ignite.log_dir, 32)

        node = ignite.nodes[0]

        load = self.__start(ignite)

        try:
            fill(ignite, ignite.log_dir)

            if layout == "separate":
                log_size = ignite.get_file_size(node, node.log_file)

                self.__check_serves(ignite, load)

                assert ControlUtility(ignite).cluster_state().state == "ACTIVE"

                assert ignite.get_file_size(node, node.log_file) == log_size, \
                    "The node's log grew while its volume was full"

                free(ignite, ignite.log_dir)

                wait_until(lambda: ignite.get_file_size(node, node.log_file) > log_size, timeout_sec=30,
                           err_msg="The node's log did not grow after space was freed")
            else:
                self.__check_stops(ignite)
        finally:
            free(ignite, ignite.log_dir)

        load.stop()

    @cluster(num_nodes=2)
    @ignite_versions(str(DEV_BRANCH))
    @matrix(dir=["wal", "archive", "cdc", "db", "work"])
    def test_volume_full(self, ignite_version, dir):
        """
        Fills the volume of one node directory. The WAL, its archive, the partitions and the work directory each have
        a directory of their own, and the one under test gets a size-limited volume. The node starts with the default
        launch.
        - wal: the WAL work directory, archiver on. Its segments are preallocated, so the node goes on serving puts.
        - archive: the WAL archive. The archiver fails, and the node stops.
        - cdc: CDC on, with no consumer and no cdcWalDirectoryMaxSize, and its directory on the archive's volume,
          since hard links need one file system. The links keep archived segments, the volume fills up by itself,
          and the node stops when the archiver fails.
        - db: the partition files. The checkpoint fails, and the node stops.
        - work: the work directory with the binary metadata. Writing the next binary type fails, and the node stops.
        A node that stops must log the failure handler's FailureContext of the expected type, caused by "No space
        left on device".
        """
        ignite = IgniteService(self.test_context, self.__server_config(ignite_version), num_nodes=1)

        root = ignite.persistent_root

        volumes = {
            "wal": (os.path.join(root, "wal"), 64),
            "archive": (os.path.join(root, "archive"), 64),
            "cdc": (os.path.join(root, "archive"), 96),
            "db": (os.path.join(root, "storage"), 128),
            "work": (ignite.work_dir, 32)
        }

        failures = {
            "archive": ("SYSTEM_WORKER_TERMINATION", "Failed to archive WAL segment"),
            "cdc": ("SYSTEM_WORKER_TERMINATION", "Failed to archive WAL segment"),
            "db": ("CRITICAL_ERROR", "StorageException"),
            "work": ("CRITICAL_ERROR", "BinaryMetadataFileStore")
        }

        path, size_mb = volumes[dir]

        data_storage = ignite.config.data_storage._replace(
            storage_path=volumes["db"][0], wal_path=volumes["wal"][0], wal_archive_path=volumes["archive"][0],
            max_wal_archive_size=32 * MB)

        if dir == "cdc":
            data_storage = data_storage._replace(cdc_wal_path=os.path.join(volumes["cdc"][0], "cdc"),
                                                 default=data_storage.default._replace(cdc_enabled=True))

        ignite.config = ignite.config._replace(data_storage=data_storage)

        ignite.mount_tmpfs(path, size_mb)

        node = ignite.nodes[0]

        load = self.__start(ignite)

        try:
            if dir != "cdc":
                fill(ignite, path)

            if dir == "wal":
                segments = ignite.exec_command(node, f"ls {path}/*/*.wal | wc -l").strip()

                assert int(segments) > 0, f"No WAL segments on the volume {path}"

                self.__check_serves(ignite, load)
            else:
                self.__check_stops(ignite, timeout_sec=120 if dir == "cdc" else 60)

                check_failure(ignite, *failures[dir])
        finally:
            free(ignite, path)

        load.stop()

    @staticmethod
    def __server_config(ignite_version):
        """
        :return: Config of a persistent server node with small WAL segments, frequent checkpoints and a thin-client
                 connector.
        """
        return IgniteConfiguration(
            version=IgniteVersion(ignite_version),
            client_connector_configuration=ClientConnectorConfiguration(),
            metrics_log_frequency=5000,
            data_storage=DataStorageConfiguration(
                checkpoint_frequency=1000,
                wal_segment_size=4 * MB,
                max_wal_archive_size=64 * MB,
                default=DataRegionConfiguration(persistence_enabled=True, max_size=256 * MB)))

    def __start(self, ignite):
        """
        Starts the node, activates the cluster and starts the load.
        :return: Load application service, already putting data and recording its progress.
        """
        ignite.start()

        ControlUtility(ignite).activate()

        address = f"{ignite.nodes[0].account.hostname}:{ignite.config.client_connector_configuration.port}"

        load = IgniteApplicationService(
            self.test_context,
            IgniteThinClientConfiguration(addresses=[address], version=ignite.config.version),
            java_class_name=self.LOAD_APP,
            params={"cacheName": "disk-full", "valueSize": 1024, "binaryTypeEvery": 1000})

        load.start()

        return load

    @staticmethod
    def __check_serves(ignite, load, duration_sec=15):
        """
        Checks that the node stays up and the load's puts go on without errors.
        """
        puts, errors = progress(load)

        time.sleep(duration_sec)

        assert ignite.alive(ignite.nodes[0]), "The node stopped"

        puts_after, errors_after = progress(load)

        assert puts_after > puts, f"No puts in {duration_sec} s"
        assert errors_after == errors, f"{errors_after - errors} puts failed in {duration_sec} s"

    @staticmethod
    def __check_stops(ignite, timeout_sec=60):
        """
        Checks that the node's process ends.
        """
        wait_until(lambda: not ignite.alive(ignite.nodes[0]), timeout_sec=timeout_sec,
                   err_msg=f"The node is still up {timeout_sec} s after its volume filled up")


def fill(ignite, path):
    """
    Writes a file into the directory until its volume is full, and checks that less than 1 MB stays free.
    """
    node = ignite.nodes[0]

    node.account.ssh(f"dd if=/dev/zero of={os.path.join(path, FILLER)} bs=1M", allow_fail=True)

    free_bytes = int(ignite.exec_command(node, f"df --output=avail -B1 {path} | tail -1").strip())

    assert free_bytes < MB, f"{free_bytes} bytes are free on the volume {path} after the fill"


def free(ignite, path):
    """
    Removes the file fill() wrote.
    """
    ignite.nodes[0].account.ssh(f"rm -f {os.path.join(path, FILLER)}", allow_fail=True)


def check_failure(ignite, failure_type, cause):
    """
    Checks that the node's log records the failure handler's FailureContext of the given type, with the cause and
    "No space left on device" in the lines that follow it.
    """
    node = ignite.nodes[0]

    failure = ignite.exec_command(node, f"grep -h -m1 -F -A60 'FailureContext [type={failure_type}' "
                                        f"{node.log_file} || true")

    assert failure, f"No {failure_type} failure in the node's log"
    assert cause in failure, f"The {failure_type} failure is not caused by '{cause}': {failure[:2000]}"
    assert "No space left on device" in failure, f"The {failure_type} failure is not ENOSPC: {failure[:2000]}"


def progress(load):
    """
    :return: Puts and failed puts the load application recorded last.
    """
    puts, errors = load.extract_results("progress")[-1].split()

    return int(puts), int(errors)
