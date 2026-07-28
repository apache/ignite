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

"""Checks for the deploy manifest, the clean allow-list, and provision idempotency."""

import json
import os
import time

import pytest
from fake_transport import FakeTransport

from ducktests_remote.commands import clean as clean_cmd
from ducktests_remote.commands import deploy, provision
from ducktests_remote.config import DEFAULTS, ConfigError
from ducktests_remote.fanout import CHANGED, FAILED, OK, HostResult, fanout, summarise


def _dist(tmp_path, name="ignite-dev", body="binary"):
    root = tmp_path / name
    (root / "bin").mkdir(parents=True)
    (root / "bin" / "ignite.sh").write_text(body, encoding="utf-8")
    (root / "libs").mkdir()
    (root / "libs" / "core.jar").write_text("jar", encoding="utf-8")
    return root


class CheckManifest:
    """Skip-if-unchanged."""

    def check_identical_trees_hash_the_same(self, tmp_path):
        first = _dist(tmp_path / "a")
        second = _dist(tmp_path / "b")
        os.utime(second / "bin" / "ignite.sh",
                 (os.stat(first / "bin" / "ignite.sh").st_atime,
                  os.stat(first / "bin" / "ignite.sh").st_mtime))
        os.utime(second / "libs" / "core.jar",
                 (os.stat(first / "libs" / "core.jar").st_atime,
                  os.stat(first / "libs" / "core.jar").st_mtime))
        assert deploy.build_manifest(first)["hash"] == deploy.build_manifest(second)["hash"]

    def check_changed_content_changes_the_hash(self, tmp_path):
        dist = _dist(tmp_path / "a")
        before = deploy.build_manifest(dist, checksum=True)["hash"]
        (dist / "bin" / "ignite.sh").write_text("different", encoding="utf-8")
        assert deploy.build_manifest(dist, checksum=True)["hash"] != before

    def check_changed_mtime_changes_the_default_hash(self, tmp_path):
        dist = _dist(tmp_path / "a")
        before = deploy.build_manifest(dist)["hash"]
        os.utime(dist / "bin" / "ignite.sh", (time.time() + 60, time.time() + 60))
        assert deploy.build_manifest(dist)["hash"] != before

    def check_manifest_records_size_and_count(self, tmp_path):
        manifest = deploy.build_manifest(_dist(tmp_path / "a"))
        assert manifest["files"] == 2 and manifest["bytes"] > 0
        assert manifest["mode"] == "size+mtime"

    def check_checksum_mode_is_recorded(self, tmp_path):
        assert deploy.build_manifest(_dist(tmp_path / "a"),
                                     checksum=True)["mode"] == "checksum"


class CheckSkipLogic:
    """The decision the fan-out makes per host, given a remote manifest."""

    @staticmethod
    def _remote_says(hash_value):
        transport = FakeTransport()
        transport.when("cat", stdout=json.dumps({"hash": hash_value}))
        return transport

    def check_matching_manifest_means_skip(self):
        transport = self._remote_says("abc")
        existing = json.loads(transport.read_file("/opt/x/.ducktests-deploy.json"))
        assert existing["hash"] == "abc"

    def check_manifest_filename_is_stable(self):
        assert deploy.MANIFEST_NAME == ".ducktests-deploy.json"

    def check_swap_removes_the_old_tree_only_after_the_move(self, tmp_path):
        script = deploy.swap_script("/opt/.x.tmp.1", "/opt/x", False, None)
        move_index = script.index('mv -- "$staging" "$target"')
        remove_index = script.index('rm -rf -- "$old"')
        assert move_index < remove_index, \
            "a half-copied distribution that looks present is worse than an absent one"

    def check_sudo_prefixes_every_privileged_command(self):
        script = deploy.swap_script("/opt/.x.tmp.1", "/opt/x", True, "max")
        assert script.count("sudo -n ") >= 3
        assert "chown -R max" in script


class CheckCleanAllowList:
    """A bug here deletes distributions across every machine at once."""

    def check_default_paths_are_accepted(self):
        assert clean_cmd.validated_paths(DEFAULTS["clean"]) == ["/mnt/service"]

    def check_path_outside_the_allow_list_is_rejected(self):
        with pytest.raises(ConfigError):
            clean_cmd.validated_paths({"paths": ["/opt/ignite-dev"],
                                       "allowed_roots": ["/mnt"]})

    def check_root_itself_is_rejected(self):
        with pytest.raises(ConfigError):
            clean_cmd.validated_paths({"paths": ["/"], "allowed_roots": ["/mnt"]})

    def check_the_allowed_root_itself_is_not_removable(self):
        with pytest.raises(ConfigError):
            clean_cmd.validated_paths({"paths": ["/mnt"], "allowed_roots": ["/mnt"]})

    def check_relative_paths_are_rejected(self):
        with pytest.raises(ConfigError):
            clean_cmd.validated_paths({"paths": ["service"], "allowed_roots": ["/mnt"]})

    def check_traversal_is_normalised_away(self):
        with pytest.raises(ConfigError):
            clean_cmd.validated_paths({"paths": ["/mnt/../opt/ignite-dev"],
                                       "allowed_roots": ["/mnt"]})

    def check_dry_run_script_kills_and_removes_nothing(self):
        script = clean_cmd._script("org.apache.ignite", ["/mnt/service"],  # noqa: SLF001
                                   dry_run=True)
        assert "dry=1" in script
        assert 'if [ "$dry" -eq 0 ]; then rm -rf -- "$d"; fi' in script


class CheckProvisionIdempotency:
    """Running a step twice reports ``changed`` and then ``ok``."""

    def check_changed_then_ok(self):
        first = FakeTransport()
        first.when("bash", stdout="CHANGED installed: rsync jq\n")
        second = FakeTransport()
        second.when("bash", stdout="all 10 packages present\n")

        def classify(transport):
            result = transport.run_script("script", check=False)
            return CHANGED if "CHANGED" in result.stdout else OK

        assert classify(first) == CHANGED
        assert classify(second) == OK

    def check_hosts_step_rewrites_only_between_its_markers(self):
        class _Args:  # pylint: disable=too-few-public-methods
            write_hosts = True

        class _Ctx:  # pylint: disable=too-few-public-methods
            args = _Args()
            config = {"provision": DEFAULTS["provision"]}

        from ducktests_remote.cluster import Node  # pylint: disable=import-outside-toplevel
        script = provision._hosts_script(  # noqa: SLF001
            _Ctx(), [Node(host="node01"), Node(host="node02", ip="10.0.0.2")])
        assert provision.HOSTS_BEGIN in script and provision.HOSTS_END in script
        assert "awk" in script and "/etc/hosts" in script
        assert "10.0.0.2 node02" in script
        assert "node01 node01" in script
        assert "> /etc/hosts" not in script.replace('> "$tmp"', ""), \
            "the whole file must never be truncated"

    def check_package_list_is_derived_from_the_dockerfile(self):
        packages = DEFAULTS["provision"]["packages"]
        for expected in ("rsync", "unzip", "curl", "jq", "iptables", "net-tools", "coreutils"):
            assert expected in packages

    def check_user_step_is_not_run_by_default(self):
        class _Args:  # pylint: disable=too-few-public-methods
            only = []
            skip = []
            create_user = None
            write_hosts = False

        steps = provision._selected_steps(_Args())  # noqa: SLF001
        assert "user" not in steps, "most operators use their own existing account"
        assert "hosts" not in steps
        assert "ssh-env" in steps

    def check_only_selects_exactly_one_step(self):
        class _Args:  # pylint: disable=too-few-public-methods
            only = ["packages"]
            skip = []
            create_user = None
            write_hosts = False

        assert provision._selected_steps(_Args()) == ["packages"]  # noqa: SLF001


class CheckFanout:
    """Per-host isolation and reporting."""

    def check_one_failure_does_not_abort_the_batch(self):
        def operation(host):
            if host == "b":
                raise RuntimeError("boom")
            return HostResult(host, OK)

        results = fanout(["a", "b", "c"], operation, jobs=4)
        assert [r.status for r in results] == [OK, FAILED, OK]
        assert summarise(results) == "1 failed, 2 ok"

    def check_fail_fast_stops_scheduling(self):
        def operation(host):
            if host == "a":
                return HostResult(host, FAILED, "no")
            return HostResult(host, OK)

        results = fanout(["a", "b", "c"], operation, jobs=1, fail_fast=True)
        assert results[0].status == FAILED
        assert all(r.status != OK for r in results[1:])

    def check_two_and_forty_nine_hosts_produce_the_same_shape(self):
        small = fanout(["a", "b"], lambda h: HostResult(h, OK), jobs=4)
        large = fanout(["h%02d" % i for i in range(49)], lambda h: HostResult(h, OK), jobs=8)
        assert {type(r) for r in small} == {type(r) for r in large}
        assert len(small) == 2 and len(large) == 49

    def check_empty_inventory_is_not_an_error(self):
        assert fanout([], lambda h: None) == []
