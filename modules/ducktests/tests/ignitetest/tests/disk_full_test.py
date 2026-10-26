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
Fills one volume of a persistent server node under load and checks whether the node survives.

The tests reproduce what Ignite does today and guard no behavior, so they run only with
--globals '{"disk_full_tests": true}'.
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
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.services.utils.ignite_spec import IgniteNodeSpecFileOutput
from ignitetest.services.utils.jvm_utils import merge_jvm_settings
from ignitetest.services.utils.ssl.client_connector_configuration import ClientConnectorConfiguration
from ignitetest.utils import cluster, ignite_versions, reproducer
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion

MB = 1024 * 1024

# A full tmpfs still takes writes into the rest of a file's last page.
PAGE = 4096

FILLER = "filler"

# Failure type and failed writer the node logs when the volume fills.
FAILURES = {
    "wal_archive": ("SYSTEM_WORKER_TERMINATION", "Failed to archive WAL segment"),
    "partitions": ("CRITICAL_ERROR", "StorageException"),
    "work": ("CRITICAL_ERROR", "BinaryMetadataFileStore")
}


class DiskFullTest(IgniteTest):
    """
    Each test puts a size-limited tmpfs over a directory of the node, fills it and records what the node does today.
    """
    LOAD_APP = "org.apache.ignite.internal.ducktest.tests.disk_full.DiskFullLoadApplication"

    FLOOD_APP = "org.apache.ignite.internal.ducktest.tests.disk_full.StdoutFloodApplication"

    @cluster(num_nodes=2)
    @reproducer("disk_full_tests")
    @ignite_versions(str(DEV_BRANCH))
    def test_node_survives_and_restarts_on_full_log_volume(self, ignite_version):
        """
        The node serves load and restarts while the volume of its log4j2 log, console.log and gc.log is full.
        """
        ignite = self.__ignite(ignite_version)
        ignite.mount_tmpfs(ignite.log_dir, 32)

        ignite.start()
        ControlUtility(ignite).activate()

        load = self.__start_load_client(ignite)

        fill_volume(ignite, ignite.log_dir)

        check_serves_load_for(ignite, load, seconds=15)
        assert ControlUtility(ignite).cluster_state().state == "ACTIVE"

        for log in log_files(ignite):
            check_write_failed(ignite, log)

        ignite.stop()

        # Waits for a failed put, so the count of puts read next is final until the node is back.
        _, errors = progress(load)
        wait_until(lambda: progress(load)[1] > errors, timeout_sec=30, err_msg="The load still reaches the node")

        puts, _ = progress(load)

        # start() awaits a line in the node's new log, which the full volume refuses.
        ignite.start_async(clean=False)

        wait_until(lambda: progress(load)[0] > puts, timeout_sec=60, err_msg="No puts after the restart")

        check_serves_load_for(ignite, load, seconds=15)
        assert ControlUtility(ignite).cluster_state().state == "ACTIVE"
        assert file_size(ignite, ignite.nodes[0].log_file) == 0, "The restarted node wrote its log"

        free_volume(ignite, ignite.log_dir)

        for log in log_files(ignite):
            check_grows(ignite, log)

        load.stop()

    @cluster(num_nodes=3)
    @reproducer("disk_full_tests")
    @ignite_versions(str(DEV_BRANCH))
    def test_node_survives_log_volume_filled_by_stdout(self, ignite_version):
        """
        A service on the node writes to System.out until its output fills the log volume.
        """
        ignite = self.__ignite(ignite_version)
        ignite.mount_tmpfs(ignite.log_dir, 32)

        ignite.start()
        ControlUtility(ignite).activate()

        load = self.__start_load_client(ignite)

        flood = self.__start_flood_service(ignite)

        wait_until(lambda: free_space(ignite, ignite.log_dir) == 0, timeout_sec=60,
                   err_msg="System.out did not fill the log volume")

        lines = printed_lines(flood)

        check_serves_load_for(ignite, load, seconds=15)
        assert ControlUtility(ignite).cluster_state().state == "ACTIVE"
        assert printed_lines(flood) > lines, "The service stopped writing to System.out"

        for log in log_files(ignite):
            check_write_failed(ignite, log)

        flood.stop()

        enlarge_volume(ignite, ignite.log_dir, 128)

        for log in log_files(ignite):
            check_grows(ignite, log)

        load.stop()

    @cluster(num_nodes=2)
    @reproducer("disk_full_tests")
    @ignite_versions(str(DEV_BRANCH))
    def test_jvm_exits_on_full_jfr_volume(self, ignite_version):
        """
        JFR ends the JVM once it can't write its recording.
        """
        ignite = self.__ignite(ignite_version)
        repository = os.path.join(ignite.persistent_root, "jfr_repository")
        jfr_opts = ["-XX:StartFlightRecording", f"-XX:FlightRecorderOptions=repository={repository}"]
        ignite.spec.jvm_opts = merge_jvm_settings(ignite.spec.jvm_opts, jfr_opts)
        ignite.mount_tmpfs(repository, 32)

        ignite.start()
        ControlUtility(ignite).activate()

        load = self.__start_load_client(ignite)

        fill_volume(ignite, repository)

        ignite.await_event("An irrecoverable error in Jfr", 60, from_the_beginning=True, log_file="console.log")
        check_stops(ignite)

        load.stop()

    @cluster(num_nodes=2)
    @reproducer("disk_full_tests")
    @ignite_versions(str(DEV_BRANCH))
    def test_node_stops_on_full_volume_shared_by_logs_and_data(self, ignite_version):
        """
        One volume holds the logs and all the data, as in Ignite's default layout.
        """
        ignite = self.__ignite(ignite_version)
        ignite.mount_tmpfs(ignite.persistent_root, 256)

        ignite.start()
        ControlUtility(ignite).activate()

        load = self.__start_load_client(ignite)

        fill_volume(ignite, ignite.persistent_root)

        check_stops(ignite)

        load.stop()

    @cluster(num_nodes=2)
    @reproducer("disk_full_tests")
    @ignite_versions(str(DEV_BRANCH))
    def test_node_survives_full_wal_volume(self, ignite_version):
        """
        WAL segments are preallocated, so puts go on into a full WAL volume.
        """
        ignite = self.__ignite(ignite_version)
        wal = os.path.join(ignite.persistent_root, "wal")
        ignite.mount_tmpfs(wal, 64)

        ignite.start()
        ControlUtility(ignite).activate()

        load = self.__start_load_client(ignite)

        segments = ignite.exec_command(ignite.nodes[0], f"ls {wal}/*/*.wal 2>/dev/null | wc -l")
        assert int(segments) > 0, "No WAL segments on the volume"

        fill_volume(ignite, wal)

        check_serves_load_for(ignite, load, seconds=15)

        load.stop()

    @cluster(num_nodes=1)
    @reproducer("disk_full_tests")
    @ignite_versions(str(DEV_BRANCH))
    def test_node_stops_on_wal_volume_smaller_than_its_segments(self, ignite_version):
        """
        The node formats its WAL segments at start, and they don't fit the volume.
        """
        ignite = self.__ignite(ignite_version)

        # Ten 4 MB segments need 40 MB.
        ignite.mount_tmpfs(os.path.join(ignite.persistent_root, "wal"), 16)

        ignite.start_async()

        check_stops(ignite)
        check_failure(ignite, "CRITICAL_ERROR", "Failed to format WAL segment file")

    @cluster(num_nodes=2)
    @reproducer("disk_full_tests")
    @ignite_versions(str(DEV_BRANCH))
    @matrix(volume=["wal_archive", "partitions", "work"])
    def test_node_stops_on_full_volume(self, ignite_version, volume):
        """
        The node stops once it can't write the WAL archive, the partitions or the binary metadata (in work).
        """
        ignite = self.__ignite(ignite_version)
        path = os.path.join(ignite.persistent_root, volume)
        ignite.mount_tmpfs(path, 64)

        ignite.start()
        ControlUtility(ignite).activate()

        load = self.__start_load_client(ignite)

        fill_volume(ignite, path)

        check_stops(ignite)
        check_failure(ignite, *FAILURES[volume])

        load.stop()

    @cluster(num_nodes=2)
    @reproducer("disk_full_tests")
    @ignite_versions(str(DEV_BRANCH))
    def test_node_stops_when_cdc_fills_wal_archive_volume(self, ignite_version):
        """
        CDC with no consumer and no cdcWalDirectoryMaxSize keeps every archived segment.
        """
        ignite = self.__ignite(ignite_version)
        archive = os.path.join(ignite.persistent_root, "wal_archive")
        data_storage = ignite.config.data_storage

        # CDC hard-links archived segments, so its directory must be on the archive's volume.
        ignite.config = ignite.config._replace(data_storage=data_storage._replace(
            cdc_wal_path=os.path.join(archive, "cdc"), default=data_storage.default._replace(cdc_enabled=True)))
        ignite.mount_tmpfs(archive, 256)

        ignite.start()
        ControlUtility(ignite).activate()

        load = self.__start_load_client(ignite)

        check_stops(ignite, timeout_sec=120)
        check_failure(ignite, *FAILURES["wal_archive"])

        load.stop()

    def __ignite(self, ignite_version):
        """
        :return: Persistent server node that writes its stdout and stderr into console.log itself, with the
                 partitions, the WAL and its archive in directories of their own.
        """
        ignite = IgniteService(self.test_context, IgniteConfiguration(
            version=IgniteVersion(ignite_version),
            client_connector_configuration=ClientConnectorConfiguration(),
            metrics_log_frequency=1000,
            data_storage=DataStorageConfiguration(
                checkpoint_frequency=1000,
                wal_segment_size=4 * MB,
                max_wal_archive_size=32 * MB,
                default=DataRegionConfiguration(persistence_enabled=True, max_size=256 * MB))), num_nodes=1)

        ignite.spec = ignite.spec.rebuild_as(IgniteNodeSpecFileOutput)

        root = ignite.persistent_root

        ignite.config = ignite.config._replace(data_storage=ignite.config.data_storage._replace(
            storage_path=os.path.join(root, "partitions"),
            wal_path=os.path.join(root, "wal"),
            wal_archive_path=os.path.join(root, "wal_archive")))

        return ignite

    def __start_load_client(self, ignite):
        """
        Starts a load that records its puts and errors once a second.
        :return: Load application service.
        """
        address = f"{ignite.nodes[0].account.hostname}:{ignite.config.client_connector_configuration.port}"

        load = IgniteApplicationService(
            self.test_context,
            IgniteThinClientConfiguration(addresses=[address], version=ignite.config.version),
            java_class_name=self.LOAD_APP,
            params={"cacheName": "disk-full"})

        load.start()

        return load

    def __start_flood_service(self, ignite):
        """
        Starts the application that deploys the System.out flood service on the server.
        :return: Flood application service.
        """
        flood = IgniteApplicationService(
            self.test_context,
            IgniteConfiguration(version=ignite.config.version, client_mode=True,
                                discovery_spi=from_ignite_cluster(ignite)),
            java_class_name=self.FLOOD_APP)

        flood.start()

        return flood


def log_files(ignite):
    """
    :return: The node's log4j2 log, console.log and gc.log, all on the log volume.
    """
    return [ignite.nodes[0].log_file] + [os.path.join(ignite.log_dir, log) for log in ("console.log", "gc.log")]


def fill_volume(ignite, path):
    """
    Writes a file into the path until its volume refuses the write.
    """
    out = ignite.exec_command(ignite.nodes[0], f"dd if=/dev/zero of={os.path.join(path, FILLER)} bs=1M 2>&1")

    assert "No space left on device" in out, f"{path} did not fill up: {out}"


def free_volume(ignite, path):
    """
    Removes the file fill_volume wrote.
    """
    ignite.nodes[0].account.ssh(f"rm -f {os.path.join(path, FILLER)}", allow_fail=False)


def enlarge_volume(ignite, path, size_mb):
    """
    Grows the tmpfs mounted over the path, keeping its files and what they hold, so writes there go on.
    """
    ignite.nodes[0].account.ssh(f"sudo mount -o remount,size={size_mb}m {path}", allow_fail=False)


def free_space(ignite, path):
    """
    :return: Bytes free on the volume of the path.
    """
    return int(ignite.exec_command(ignite.nodes[0], f"df --output=avail -B1 {path} | tail -1"))


def printed_lines(flood):
    """
    :return: Lines the service wrote to System.out, as the flood application recorded last.
    """
    return int(flood.extract_results("lines")[-1])


def file_size(ignite, path):
    """
    :return: Bytes written into the file on the node.
    """
    return int(ignite.exec_command(ignite.nodes[0], f"stat -c %s {path}"))


def check_write_failed(ignite, path):
    """
    Checks that the file ends on a page boundary: a write the full tmpfs refused filled its last page.
    """
    assert file_size(ignite, path) % PAGE == 0, f"No write into {path} hit the full volume"


def check_grows(ignite, path):
    """
    Checks that the file grows within 15 s.
    """
    size = file_size(ignite, path)

    wait_until(lambda: file_size(ignite, path) > size, timeout_sec=15, err_msg=f"{path} did not grow")


def check_serves_load_for(ignite, load, seconds):
    """
    Waits the given seconds and checks that the node is alive and the puts went on without errors.
    """
    puts, errors = progress(load)

    time.sleep(seconds)

    assert ignite.alive(ignite.nodes[0]), "The node stopped"

    puts_after, errors_after = progress(load)

    assert puts_after > puts, f"No puts in {seconds} s"
    assert errors_after == errors, f"{errors_after - errors} puts failed in {seconds} s"


def check_stops(ignite, timeout_sec=60):
    """
    Checks that the node's process ends.
    """
    wait_until(lambda: not ignite.alive(ignite.nodes[0]), timeout_sec=timeout_sec,
               err_msg=f"The node is still up {timeout_sec} s after its volume filled up")


def check_failure(ignite, failure_type, cause):
    """
    Checks that the node logged a FailureContext of the type, caused by the writer and "No space left on device".
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
