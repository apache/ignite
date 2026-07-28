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

"""Checks for JDK resolution, the environment files, and the archive inspection."""

import io
import json
import tarfile

import pytest

from fake_transport import FakeTransport

from ducktests_remote import java
from ducktests_remote.cli import Console, Context
from ducktests_remote.cluster import Node
from ducktests_remote.commands import doctor, provision
from ducktests_remote.config import ConfigError, load_config

PROBE = """java_requested=17
java=openjdk version "11.0.19" 2023-04-18
java_path=/usr/bin/java
java_path_version=11.0.19
java_path_major=11
java_path_real=/usr/lib/jvm/java-11-openjdk/bin/java
java_env_home=
java_candidates=/usr/lib/jvm/java-11-openjdk:11:11.0.19,/opt/jdk-17.0.9:17:17.0.9,\
/opt/jdk-17.0.11:17:17.0.11
"""

MATCHING = """java_requested=17
java=openjdk version "17.0.11" 2024-04-16
java_path=/usr/bin/java
java_path_version=17.0.11
java_path_major=17
java_path_real=/opt/jdk-17.0.11/bin/java
java_env_home=/opt/jdk-17.0.11
java_candidates=/opt/jdk-17.0.11:17:17.0.11
"""


def _cfg(**kw):
    kw.setdefault("major", 17)
    kw.setdefault("search_paths", ["/opt", "/usr/lib/jvm"])
    return java.JavaConfig(**kw)


class CheckVersionParsing:
    """Mirrors ignitetest's jvm_utils.java_major_version."""

    @pytest.mark.parametrize("version,major", [
        ("1.8.0_292", 8), ("11.0.19", 11), ("17.0.11+9", 17), ("21", 21), ("11-ea", 11),
        ("", None), ("garbage", None),
    ])
    def check_major(self, version, major):
        assert java.major_of(version) == major

    def check_patch_levels_order_numerically(self):
        assert java.version_key("17.0.9") < java.version_key("17.0.11")


class CheckResolution:
    """The ladder, exercised against recorded probe output."""

    def check_the_newest_matching_jdk_is_chosen(self):
        res = java.parse_probe("w1", PROBE, _cfg())
        assert res.home == "/opt/jdk-17.0.11", "17.0.9 sorts after 17.0.11 as a string"
        assert res.source == java.SEARCH
        assert res.path_matches(17) is False, "PATH still points at Java 11"

    def check_a_matching_java_on_path_is_used_as_is(self):
        res = java.parse_probe("w1", MATCHING, _cfg())
        assert res.source == java.CURRENT
        assert res.home == "/opt/jdk-17.0.11"
        assert res.path_matches(17) and res.home_in_effect

    def check_an_explicit_home_short_circuits_the_search(self):
        res = java.parse_probe("w1", PROBE, _cfg(home="/opt/jdk-17.0.9"))
        assert res.home == "/opt/jdk-17.0.9" and res.source == java.EXPLICIT

    def check_an_explicit_home_that_is_absent_selects_nothing(self):
        # Deliberately not a fallback: an explicit java.home that silently resolves to a
        # different JVM is worse than a failure naming the host.
        assert not java.parse_probe("w1", PROBE, _cfg(home="/opt/nope")).selected

    def check_no_matching_jdk_selects_nothing_but_reports_what_is_there(self):
        res = java.parse_probe("w1", PROBE, _cfg(major=21))
        assert not res.selected
        assert ("/opt/jdk-17.0.11", 17, "17.0.11") in res.candidates

    def check_no_requested_version_accepts_whatever_is_there(self):
        res = java.parse_probe("w1", PROBE, _cfg(major=None))
        assert res.home == "/usr/lib/jvm/java-11-openjdk" and res.source == java.CURRENT

    def check_a_symlinked_java_still_counts_as_the_selected_home(self):
        res = java.parse_probe("w1", MATCHING, _cfg())
        assert res.home_in_effect, "/usr/bin/java resolves into the selected home"

    def check_an_empty_probe_is_survivable(self):
        res = java.parse_probe("w1", "", _cfg())
        assert not res.selected and res.path_major is None


class CheckDiscoveryScript:
    """The generated shell has to be safe to run from doctor."""

    def check_paths_are_quoted(self):
        script = java.discovery_script(_cfg(search_paths=["/opt/my jdks"],
                                            home="/opt/vendor jdk"))
        assert "'/opt/my jdks'" in script and "explicit='/opt/vendor jdk'" in script

    def check_it_never_writes_anything(self):
        script = java.discovery_script(_cfg())
        for mutation in ("mkdir", "rm ", "mv ", "install", ">>", "chmod"):
            assert mutation not in script, "discovery must be read-only: %r" % mutation

    def check_it_always_exits_zero(self):
        # One unusable host must not abort the fan-out; the status is in the fields.
        assert java.discovery_script(_cfg()).rstrip().endswith("exit 0")


class CheckEnvScript:
    """Both environment files, written from one resolved value."""

    def check_both_files_are_written(self):
        script = java.env_script(_cfg(), "/opt/jdk-17")
        assert "~/.ssh/environment" in script and "~/.bashrc" in script

    def check_each_file_can_be_switched_off(self):
        # Composed in Python rather than guarded at runtime, so --dry-run shows only what
        # the step will really do - and so a disabled section cannot print a note about
        # itself as the fallback.
        without_bashrc = java.env_script(_cfg(bashrc=False), "/opt/jdk-17")
        assert "~/.bashrc" not in without_bashrc
        assert "java.bashrc is off" in without_bashrc, "and the note says so"
        without_ssh = java.env_script(_cfg(ssh_environment=False), "/opt/jdk-17")
        assert "~/.ssh/environment" not in without_ssh and "~/.bashrc" in without_ssh

    def check_switching_both_off_is_refused(self):
        with pytest.raises(ConfigError):
            java.env_script(_cfg(ssh_environment=False, bashrc=False), "/opt/jdk-17")

    def check_the_bashrc_block_goes_above_the_interactivity_guard(self):
        # The stock ~/.bashrc returns early for non-interactive shells, which is exactly
        # the case ducktape runs in, so appending would be writing to /dev/null.
        script = java.env_script(_cfg(), "/opt/jdk-17")
        assert script.index(java.BLOCK_BEGIN) < script.index("awk 'BEGIN{skip=0}"), \
            "the block is emitted before the existing file is appended to it"

    def check_the_jdk_is_prepended_to_path_not_appended(self):
        assert 'want_path="PATH=$jh/bin:$base_path' in java.env_script(_cfg(), "/opt/jdk-17")

    def check_the_path_is_not_grown_on_every_run(self):
        # A host that honours ~/.ssh/environment feeds the composed PATH straight back in.
        script = java.env_script(_cfg(), "/opt/jdk-17", ["/opt/venv/bin"])
        assert 'strip="$jh/bin:/opt/venv/bin"' in script

    def check_it_refuses_a_home_without_java(self):
        assert '[ -x "$jh/bin/java" ]' in java.env_script(_cfg(), "/opt/not-a-jdk")


class CheckVerifyScript:
    """What a fresh session gets is the authority, so it must be parseable."""

    def check_it_reports_the_fields_the_resolver_reads(self):
        script = java.verify_script()
        for key in ("java_path", "java_path_version", "java_path_major", "java_env_home"):
            assert "say %s " % key in script

    def check_its_output_round_trips(self):
        verified = """java=openjdk version "17.0.11" 2024-04-16
java_path=/usr/bin/java
java_path_version=17.0.11
java_path_major=17
java_path_real=/opt/jdk-17.0.11/bin/java
"""
        assert java.parse_probe("w1", verified, _cfg()).path_matches(17)


def _tarball(tmp_path, names, top="jdk-17.0.11+9"):
    path = tmp_path / "jdk.tar.gz"
    with tarfile.open(path, "w:gz") as tar:
        for name in names:
            full = "%s/%s" % (top, name) if top else name
            info = tarfile.TarInfo(full)
            info.size = 4
            tar.addfile(info, io.BytesIO(b"data"))
    return path


class CheckArchivePlan:
    """A bad archive must fail on the coordinator, before it is copied anywhere."""

    def check_a_single_top_level_directory_is_stripped(self, tmp_path):
        plan = java.archive_plan(_tarball(tmp_path, ["bin/java", "lib/modules"]))
        assert plan.strip == 1 and plan.top_level == "jdk-17.0.11+9"
        assert plan.name == "jdk-17.0.11+9", "the target directory takes the JDK's own name"
        assert plan.bytes == 8

    def check_an_archive_without_bin_java_is_refused(self, tmp_path):
        # A macOS build has Contents/Home in between and is worth catching here rather
        # than on twelve hosts at once.
        with pytest.raises(ConfigError) as ex:
            java.archive_plan(_tarball(tmp_path, ["Contents/Home/bin/java"]))
        assert "bin/java" in str(ex.value)

    def check_a_flat_archive_is_not_stripped(self, tmp_path):
        plan = java.archive_plan(_tarball(tmp_path, ["bin/java", "lib/modules"], top=""))
        assert plan.strip == 0 and plan.top_level is None
        assert plan.name == "jdk"

    def check_a_directory_source_is_accepted(self, tmp_path):
        home = tmp_path / "jdk-17"
        (home / "bin").mkdir(parents=True)
        (home / "bin" / "java").write_text("#!/bin/sh\n", encoding="utf-8")
        plan = java.archive_plan(str(home))
        assert plan.kind == "dir" and plan.name == "jdk-17"

    def check_a_directory_without_bin_java_is_refused(self, tmp_path):
        (tmp_path / "empty").mkdir()
        with pytest.raises(ConfigError):
            java.archive_plan(str(tmp_path / "empty"))

    def check_a_zip_names_the_supported_formats(self, tmp_path):
        path = tmp_path / "jdk.zip"
        path.write_bytes(b"PK\x03\x04")
        with pytest.raises(ConfigError) as ex:
            java.archive_plan(str(path))
        assert ".tar.gz" in str(ex.value)

    def check_a_missing_archive_names_the_path(self, tmp_path):
        with pytest.raises(ConfigError) as ex:
            java.archive_plan(str(tmp_path / "absent.tar.gz"))
        assert "absent.tar.gz" in str(ex.value)

    def check_the_target_directory_can_be_renamed(self, tmp_path):
        plan = java.archive_plan(_tarball(tmp_path, ["bin/java"]))
        assert java.target_dir(_cfg(install_root="/opt", name="jdk-17"), plan) == "/opt/jdk-17"
        assert java.target_dir(_cfg(install_root="/opt"), plan) == "/opt/jdk-17.0.11+9"


def _facts(text):
    return {line.split("=", 1)[0]: line.split("=", 1)[1]
            for line in text.strip().splitlines() if "=" in line}


def _context(probe_output, **java_cfg):
    """A context whose single worker answers the JDK probe with ``probe_output``."""
    class Args:  # pylint: disable=too-few-public-methods
        """Minimal stand-in for the parsed command line."""

    args = Args()
    args.dry_run = False
    args.sudo = False
    args.force = False
    args.install_jdk = False
    args.num_nodes = None

    config = load_config(user_config=None)
    config["java"].update(java_cfg)
    config["cluster"]["nodes"] = [{"host": "w1"}]

    ctx = Context(config, args, Console(color=False))
    fake = FakeTransport(name="w1")
    fake.when("java_requested", probe_output)
    ctx._workers["w1"] = fake       # noqa: SLF001 - the point of the fake
    return ctx, fake


NODE = Node(host="w1", user="max", port=22, identity_file=None)


class CheckDelivery:
    """`provision --only jdk`, on one host, with no network and no processes."""

    def check_a_host_that_already_matches_is_left_alone(self):
        ctx, fake = _context(MATCHING)
        result = provision._jdk_on_host(  # noqa: SLF001
            ctx, NODE, java.config_of(ctx), None, java.discovery_script(java.config_of(ctx)))
        assert result.status == provision.OK
        assert not fake.uploads, "nothing may be sent to a host that is already correct"

    def check_a_host_without_a_match_gets_the_archive(self, tmp_path):
        archive = _tarball(tmp_path, ["bin/java", "lib/modules"])
        ctx, fake = _context(PROBE, major=21, archive=str(archive), install_root="/opt")
        cfg = java.config_of(ctx)
        plan = java.archive_plan(cfg.archive)
        result = provision._jdk_on_host(ctx, NODE, cfg, plan,  # noqa: SLF001
                                        java.discovery_script(cfg))

        assert result.status == provision.CHANGED
        assert fake.uploads, "the archive has to reach the host"
        scripts = "\n".join(fake.scripts)
        assert "--strip-components=1" in scripts, "a Temurin tarball has one top-level dir"
        assert "/opt/jdk-17.0.11+9" in scripts, "the swap targets the JDK's own name"
        manifest = [body for path, (body, _) in fake.files.items()
                    if path.endswith(provision.JAVA_MANIFEST_NAME)]
        assert manifest and json.loads(manifest[0])["hash"]

    def check_a_host_that_already_has_that_archive_is_skipped(self, tmp_path):
        archive = _tarball(tmp_path, ["bin/java"])
        ctx, fake = _context(PROBE, major=21, archive=str(archive), install_root="/opt")
        cfg = java.config_of(ctx)
        plan = java.archive_plan(cfg.archive)
        digest = provision._tar_manifest(plan)["hash"]  # noqa: SLF001
        fake.when("cat", json.dumps({"hash": digest}))

        result = provision._jdk_on_host(ctx, NODE, cfg, plan,  # noqa: SLF001
                                        java.discovery_script(cfg))
        assert result.status == provision.OK and not fake.uploads

    def check_no_archive_and_no_match_fails_with_the_config_keys(self):
        ctx, _ = _context(PROBE, major=21)
        cfg = java.config_of(ctx)
        result = provision._jdk_on_host(ctx, NODE, cfg, None,  # noqa: SLF001
                                        java.discovery_script(cfg))
        assert result.status == provision.FAILED
        assert "java.archive" in result.message and "java.home" in result.message

    def check_a_missing_explicit_home_is_never_papered_over(self, tmp_path):
        # Even with an archive available: java.home means that JDK, not a similar one.
        archive = _tarball(tmp_path, ["bin/java"])
        ctx, fake = _context(PROBE, home="/opt/vendor-jdk", archive=str(archive))
        cfg = java.config_of(ctx)
        result = provision._jdk_on_host(ctx, NODE, cfg,  # noqa: SLF001
                                        java.archive_plan(cfg.archive),
                                        java.discovery_script(cfg))
        assert result.status == provision.FAILED and "/opt/vendor-jdk" in result.message
        assert not fake.uploads


class CheckSshEnvStep:
    """The verification, not the edit, decides the outcome."""

    def check_a_session_that_still_gets_the_wrong_jvm_fails(self):
        ctx, fake = _context(PROBE)                      # PATH java is 11, selected is 17
        fake.when("say java_path", PROBE)                # ... and stays 11 after the write
        results = provision._run_ssh_env_step(ctx, [NODE])  # noqa: SLF001
        assert results[0].status == provision.FAILED
        assert "still gets" in results[0].message

    def check_a_session_that_gets_the_right_jvm_passes(self):
        ctx, fake = _context(PROBE)
        # Only the verify script matches this needle: the discovery script is answered by
        # the earlier `java_requested` response, so this is the session *after* the write.
        fake.when("say java_path", MATCHING)
        results = provision._run_ssh_env_step(ctx, [NODE])  # noqa: SLF001
        assert results[0].status in (provision.OK, provision.CHANGED)
        assert any("~/.bashrc" in script for script in fake.scripts)
        assert any("~/.ssh/environment" in script for script in fake.scripts)

    def check_nothing_to_point_at_fails_before_writing(self):
        ctx, fake = _context(PROBE, major=21)
        results = provision._run_ssh_env_step(ctx, [NODE])  # noqa: SLF001
        assert results[0].status == provision.FAILED
        assert "provision --only jdk" in results[0].message
        assert not any("~/.bashrc" in s for s in fake.scripts)


class CheckDoctorVerdicts:
    """The preflight judges what a non-interactive session gets, nothing else."""

    def check_a_matching_jdk_passes(self):
        checks = doctor._java_checks({"w1": _facts(MATCHING)}, _cfg())  # noqa: SLF001
        assert [c.status for c in checks] == [doctor.OK]

    def check_a_wrong_version_fails_and_names_the_remedy(self):
        checks = doctor._java_checks({"w1": _facts(PROBE)}, _cfg())  # noqa: SLF001
        assert checks[0].status == doctor.FAIL
        assert "/opt/jdk-17.0.11" in checks[0].message and "provision" in checks[0].message

    def check_a_wrong_version_with_nothing_installed_names_the_config_keys(self):
        checks = doctor._java_checks({"w1": _facts(PROBE)}, _cfg(major=21))  # noqa: SLF001
        assert checks[0].status == doctor.FAIL
        assert "java.archive" in checks[0].message

    def check_a_missing_java_fails(self):
        checks = doctor._java_checks({"w1": {}}, _cfg())  # noqa: SLF001
        assert checks[0].status == doctor.FAIL and "PATH" in checks[0].message

    def check_an_explicit_home_that_is_not_in_effect_only_warns(self):
        # Right version, wrong JDK: the tests will still run, so this is not a failure.
        checks = doctor._java_checks(  # noqa: SLF001
            {"w1": _facts(MATCHING)}, _cfg(home="/opt/jdk-17-vendor"))
        assert checks[0].status == doctor.WARN and "java.home" in checks[0].message

    def check_mixed_jdks_are_reported_once_for_the_cluster(self):
        facts = {"w1": _facts(MATCHING), "w2": _facts(MATCHING)}
        facts["w2"]["java_path_version"] = "17.0.2"
        checks = doctor._java_checks(facts, _cfg())  # noqa: SLF001
        assert [c for c in checks if c.name == "java-consistency"]
