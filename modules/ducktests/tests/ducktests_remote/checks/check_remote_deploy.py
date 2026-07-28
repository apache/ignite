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
import tarfile
import time

import pytest
from fake_transport import FakeTransport

from ducktests_remote.cluster import Node
from ducktests_remote.commands import clean as clean_cmd
from ducktests_remote.commands import deploy, provision
from ducktests_remote.config import DEFAULTS, ConfigError
from ducktests_remote.fanout import CHANGED, FAILED, OK, HostResult, fanout, summarise
from ducktests_remote.transport import Result, make_tarball


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


class CheckExcludes:
    """``ignite-dev`` is normally a link to a checkout; only the built jars are wanted."""

    @staticmethod
    def _checkout(tmp_path):
        """A tree shaped like an Ignite source root after a build."""
        root = tmp_path / "ignite-dev"
        for rel in ("bin/ignite.sh",
                    "modules/core/src/main/java/Ignite.java",
                    "modules/core/target/classes/Ignite.class",
                    "modules/core/target/ignite-core.jar",
                    "modules/core/target/libs/dep.jar",
                    "modules/ducktests/tests/certs/truststore.jks"):
            path = root / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(rel, encoding="utf-8")
        return root

    @staticmethod
    def _ctx(tmp_path, exclude=None, config_exclude=None):
        class _Args:  # pylint: disable=too-few-public-methods
            pass

        class _Ctx:  # pylint: disable=too-few-public-methods
            pass

        ctx = _Ctx()
        ctx.args = _Args()
        ctx.args.exclude = list(exclude or [])
        ctx.config = {"deploy": {**DEFAULTS["deploy"], "exclude": list(config_exclude or [])}}
        ctx.dist_dir = tmp_path
        return ctx

    def check_manifest_leaves_out_excluded_files(self, tmp_path):
        root = self._checkout(tmp_path)
        whole = deploy.build_manifest(root)
        filtered = deploy.build_manifest(root, excludes=["src", "classes"])
        assert filtered["files"] == whole["files"] - 2
        assert filtered["excluded"] == 2
        assert filtered["bytes"] < whole["bytes"]
        assert filtered["hash"] != whole["hash"]

    def check_the_tarball_holds_exactly_what_the_manifest_counted(self, tmp_path):
        root = self._checkout(tmp_path)
        excludes = ["src", "classes"]
        archive = tmp_path / "payload.tar.gz"
        make_tarball(root, archive, excludes=excludes)
        with tarfile.open(archive) as tar:
            names = sorted(m.name for m in tar.getmembers())
        assert names == ["bin/ignite.sh",
                         "modules/core/target/ignite-core.jar",
                         "modules/core/target/libs/dep.jar",
                         "modules/ducktests/tests/certs/truststore.jks"]
        assert len(names) == deploy.build_manifest(root, excludes=excludes)["files"], \
            "a host called up to date must hold the same files the tarball carries"

    def check_no_excludes_ships_the_tree_whole(self, tmp_path):
        root = self._checkout(tmp_path)
        manifest = deploy.build_manifest(root)
        assert manifest["excluded"] == 0 and manifest["files"] == 6

    def check_command_line_beats_the_ignore_file_and_the_config(self, tmp_path):
        root = self._checkout(tmp_path)
        (root / deploy.IGNORE_NAME).write_text("classes\n", encoding="utf-8")
        ctx = self._ctx(tmp_path, exclude=["src"], config_exclude=["target"])
        assert deploy.resolve_excludes(ctx, root) == ["src", deploy.IGNORE_NAME]

    def check_the_ignore_file_beats_the_config(self, tmp_path):
        root = self._checkout(tmp_path)
        (root / deploy.IGNORE_NAME).write_text(
            "# only the jars are wanted on the workers\n"
            "src\n"
            "\n"
            "classes\n", encoding="utf-8")
        ctx = self._ctx(tmp_path, config_exclude=["target"])
        assert deploy.resolve_excludes(ctx, root) == ["src", "classes", deploy.IGNORE_NAME]

    def check_the_ignore_file_is_never_shipped(self, tmp_path):
        root = self._checkout(tmp_path)
        (root / deploy.IGNORE_NAME).write_text("src\n", encoding="utf-8")
        ctx = self._ctx(tmp_path)
        excludes = deploy.resolve_excludes(ctx, root)
        archive = tmp_path / "payload.tar.gz"
        make_tarball(root, archive, excludes=excludes)
        with tarfile.open(archive) as tar:
            assert deploy.IGNORE_NAME not in [m.name for m in tar.getmembers()]

    def check_the_config_applies_when_nothing_more_specific_exists(self, tmp_path):
        root = self._checkout(tmp_path)
        ctx = self._ctx(tmp_path, config_exclude=["src", "classes"])
        assert deploy.resolve_excludes(ctx, root) == ["src", "classes"]

    def check_the_default_is_no_filtering(self, tmp_path):
        assert deploy.resolve_excludes(self._ctx(tmp_path), self._checkout(tmp_path)) == []
        assert DEFAULTS["deploy"]["exclude"] == []


class _RsyncTransport(FakeTransport):
    """A worker whose rsync is present, with the ssh details rsync needs."""

    rsync = True

    @property
    def target(self):
        return "tester@w1"

    def ssh_options(self, *, for_scp=False):  # pylint: disable=unused-argument
        return ["-o", "BatchMode=yes", "-i", "/home/tester/.ssh/id_ed25519"]

    def has_rsync(self):
        """:return: whether this worker is on the incremental path."""
        return self.rsync


class CheckRsyncFastPath:
    """Rebuild one module, send one jar."""

    STATS = ("Number of files: 1,234 (reg: 1,200, dir: 34)\n"
             "Number of regular files transferred: 12\n"
             "Total file size: 524,288,000 bytes\n"
             "Total transferred file size: 4,194,304 bytes\n")

    @staticmethod
    def _args(**kw):
        class _Args:  # pylint: disable=too-few-public-methods
            force = False
            sudo = False
            owner = None
            checksum = False
            no_rsync = False
            via = None

        args = _Args()
        for key, value in kw.items():
            setattr(args, key, value)
        return args

    @classmethod
    def _ctx(cls, transport, **kw):
        class _Console:  # pylint: disable=too-few-public-methods
            verbose = False

            @staticmethod
            def detail(message):
                """Swallow the traced command line."""

        class _Ctx:  # pylint: disable=too-few-public-methods
            pass

        ctx = _Ctx()
        ctx.args = cls._args(**kw)
        ctx.config = {"deploy": dict(DEFAULTS["deploy"])}
        ctx.console = _Console()
        ctx.dry_run = False
        ctx.worker = lambda node: transport
        return ctx

    @staticmethod
    def _dist_and_payload(tmp_path):
        root = _dist(tmp_path / "d")
        return root, deploy._Payload(root, tmp_path / "p.tar.gz", ())  # noqa: SLF001

    # -- the command line --------------------------------------------------------

    def check_the_file_list_comes_from_stdin_not_from_rsync_patterns(self):
        argv = deploy.rsync_argv(_RsyncTransport(), "/dist/ignite-dev", "/opt/.tmp.1")
        assert "--files-from=-" in argv
        assert not [a for a in argv if a.startswith("--exclude")], \
            "rsync pattern matching differs from is_excluded; the exact list is sent instead"

    def check_unchanged_files_are_hardlinked_against_the_live_distribution(self):
        argv = deploy.rsync_argv(_RsyncTransport(), "/dist/ignite-dev", "/opt/.tmp.1",
                                 link_dest="/opt/ignite-dev")
        assert "--link-dest=/opt/ignite-dev" in argv

    def check_a_first_deployment_has_nothing_to_link_against(self):
        argv = deploy.rsync_argv(_RsyncTransport(), "/dist/ignite-dev", "/opt/.tmp.1")
        assert not [a for a in argv if a.startswith("--link-dest")]

    def check_it_lands_in_the_staging_directory_never_in_the_target(self):
        argv = deploy.rsync_argv(_RsyncTransport(), "/dist/ignite-dev", "/opt/.tmp.1",
                                 link_dest="/opt/ignite-dev")
        assert argv[-1] == "tester@w1:/opt/.tmp.1/", \
            "an interrupted transfer must not leave a live distribution half updated"
        assert argv[-2] == "/dist/ignite-dev/"

    def check_ssh_options_reach_rsync(self):
        argv = deploy.rsync_argv(_RsyncTransport(), "/dist/x", "/opt/.tmp.1")
        assert argv[argv.index("-e") + 1] == \
            "ssh -o BatchMode=yes -i /home/tester/.ssh/id_ed25519"

    def check_sudo_and_checksum_are_passed_through(self):
        argv = deploy.rsync_argv(_RsyncTransport(), "/dist/x", "/opt/.tmp.1",
                                 checksum=True, sudo=True)
        assert "--checksum" in argv and "--rsync-path=sudo -n rsync" in argv

    # -- stats -------------------------------------------------------------------

    def check_stats_are_read_back(self):
        assert deploy.parse_rsync_stats(self.STATS) == (12, 4194304)

    def check_rsync_2_x_wording_is_understood(self):
        assert deploy.parse_rsync_stats("Number of files transferred: 3\n"
                                        "Total transferred file size: 1024 bytes\n") == (3, 1024)

    def check_unreadable_stats_are_not_guessed(self):
        assert deploy.parse_rsync_stats("") is None
        assert deploy.parse_rsync_stats("all done") is None

    # -- when it is used ---------------------------------------------------------

    @staticmethod
    def _posix_with_rsync(monkeypatch):
        monkeypatch.setattr(deploy.shutil, "which", lambda _: "/usr/bin/rsync")
        monkeypatch.setattr(deploy.os, "name", "posix")

    def check_it_is_on_by_default_when_rsync_is_installed(self, monkeypatch):
        self._posix_with_rsync(monkeypatch)
        assert deploy.rsync_enabled(self._ctx(None)) is True

    def check_no_rsync_flag_and_config_both_turn_it_off(self, monkeypatch):
        self._posix_with_rsync(monkeypatch)
        assert deploy.rsync_enabled(self._ctx(None, no_rsync=True)) is False
        ctx = self._ctx(None)
        ctx.config["deploy"]["rsync"] = False
        assert deploy.rsync_enabled(ctx) is False

    def check_via_keeps_the_single_upload(self, monkeypatch):
        self._posix_with_rsync(monkeypatch)
        assert deploy.rsync_enabled(self._ctx(None, via="build-vm-01")) is False, \
            "--via exists so the payload crosses the slow link once, as one file"

    def check_a_coordinator_without_rsync_falls_back(self, monkeypatch):
        monkeypatch.setattr(deploy.os, "name", "posix")
        monkeypatch.setattr(deploy.shutil, "which", lambda _: None)
        assert deploy.rsync_enabled(self._ctx(None)) is False

    def check_a_windows_coordinator_falls_back(self, monkeypatch):
        monkeypatch.setattr(deploy.shutil, "which", lambda _: "rsync.exe")
        monkeypatch.setattr(deploy.os, "name", "nt")
        assert deploy.rsync_enabled(self._ctx(None)) is False, \
            "rsync would read C:/dist/ignite-dev as host C"

    # -- the transfer ------------------------------------------------------------

    def check_the_tarball_is_never_built_when_every_host_takes_rsync(self, tmp_path,
                                                                     monkeypatch):
        root, payload = self._dist_and_payload(tmp_path)
        transport = _RsyncTransport()
        calls = []

        def fake_run_local(argv, **kw):
            calls.append((argv, kw.get("input")))
            return Result(argv, 0, self.STATS, "", "local")

        monkeypatch.setattr(deploy, "run_local", fake_run_local)
        manifest = deploy.build_manifest(root)
        result = deploy._deploy_to_host(  # noqa: SLF001
            self._ctx(transport), Node(host="w1", user="tester"), "ignite-dev",
            "/opt/ignite-dev", manifest, "{}", payload, None, None,
            "\n".join(deploy.included_files(root)) + "\n")

        assert not (tmp_path / "p.tar.gz").exists(), \
            "compressing a linked checkout would cost more than the transfer saves"
        assert len(calls) == 1
        assert calls[0][1] == "bin/ignite.sh\nlibs/core.jar\n"
        assert result.status == CHANGED and "rsync: 12 of 2 file(s) changed" in result.message

    def check_the_staging_tree_is_still_swapped_into_place(self, tmp_path, monkeypatch):
        root, payload = self._dist_and_payload(tmp_path)
        transport = _RsyncTransport()
        monkeypatch.setattr(deploy, "run_local",
                            lambda argv, **kw: Result(argv, 0, self.STATS, "", "local"))
        deploy._deploy_to_host(  # noqa: SLF001
            self._ctx(transport), Node(host="w1", user="tester"), "ignite-dev",
            "/opt/ignite-dev", deploy.build_manifest(root), "{}", payload, None, None,
            "bin/ignite.sh\n")
        scripts = "\n".join(transport.scripts)
        assert 'mv -- "$staging" "$target"' in scripts
        assert deploy.MANIFEST_NAME in "".join(transport.files)

    def check_a_worker_without_rsync_gets_the_tarball(self, tmp_path, monkeypatch):
        root, payload = self._dist_and_payload(tmp_path)
        transport = _RsyncTransport()
        transport.rsync = False
        monkeypatch.setattr(deploy, "run_local",
                            lambda argv, **kw: pytest.fail("rsync must not run here"))
        result = deploy._deploy_to_host(  # noqa: SLF001
            self._ctx(transport), Node(host="w1", user="tester"), "ignite-dev",
            "/opt/ignite-dev", deploy.build_manifest(root), "{}", payload, None, None,
            "bin/ignite.sh\n")
        assert (tmp_path / "p.tar.gz").exists()
        assert transport.uploads and result.status == CHANGED

    def check_a_failed_rsync_leaves_the_target_alone(self, tmp_path, monkeypatch):
        root, payload = self._dist_and_payload(tmp_path)
        transport = _RsyncTransport()
        monkeypatch.setattr(deploy, "run_local",
                            lambda argv, **kw: Result(argv, 23, "", "permission denied",
                                                      "local"))
        result = deploy._deploy_to_host(  # noqa: SLF001
            self._ctx(transport), Node(host="w1", user="tester"), "ignite-dev",
            "/opt/ignite-dev", deploy.build_manifest(root), "{}", payload, None, None,
            "bin/ignite.sh\n")
        assert result.status == FAILED and "permission denied" in result.detail
        assert 'mv -- "$staging" "$target"' not in "\n".join(transport.scripts)

    def check_the_file_list_matches_the_manifest(self, tmp_path):
        root = CheckExcludes._checkout(tmp_path)  # noqa: SLF001
        excludes = ["src", "classes"]
        files = deploy.included_files(root, excludes)
        assert files == ["bin/ignite.sh",
                         "modules/core/target/ignite-core.jar",
                         "modules/core/target/libs/dep.jar",
                         "modules/ducktests/tests/certs/truststore.jks"]
        assert len(files) == deploy.build_manifest(root, excludes=excludes)["files"]
