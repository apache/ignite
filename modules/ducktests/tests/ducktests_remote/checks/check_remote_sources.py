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

"""Checks for finding the Ignite checkout and for the test paths handed to ducktape."""

import os

import pytest

from fake_transport import FakeTransport

from ducktests_remote.cli import Console, Context
from ducktests_remote.commands import run
from ducktests_remote.config import ConfigError, load_config

TESTS = ("modules", "ducktests", "tests")


def _checkout(tmp_path, name="ignite"):
    """Build the smallest tree that counts as an Ignite checkout."""
    root = tmp_path / name
    tests = root.joinpath(*TESTS)
    (tests / "docker").mkdir(parents=True, exist_ok=True)
    (tests / "docker" / "requirements.txt").write_text("ducktape==0.13.0\n", encoding="utf-8")
    suite = tests / "ignitetest" / "tests"
    suite.mkdir(parents=True, exist_ok=True)
    (suite / "smoke_test.py").write_text("# tests\n", encoding="utf-8")
    return root.resolve()


def _context(**args):
    class Args:  # pylint: disable=too-few-public-methods
        """Minimal stand-in for the parsed command line."""

    parsed = Args()
    parsed.dry_run = False
    parsed.source_root = None
    parsed.no_sync = False
    parsed.work_dir = None
    for key, value in args.items():
        setattr(parsed, key, value)
    ctx = Context(load_config(user_config=None), parsed, Console(color=False))
    ctx._runner = FakeTransport()  # noqa: SLF001 - the point of the fake
    return ctx


@pytest.fixture(name="in_dir")
def _in_dir():
    """Run a check from a chosen directory and restore the old one afterwards."""
    previous = os.getcwd()
    yield os.chdir
    os.chdir(previous)


class CheckSourceRoot:
    """Where the sources are taken from when nothing says so explicitly."""

    def check_the_checkout_root_is_found_from_the_tests_directory(self, tmp_path, in_dir):
        # The Docker flow is run from here, so this is where operators stand.
        root = _checkout(tmp_path)
        in_dir(str(root.joinpath(*TESTS)))
        assert run._source_root(_context()) == root.resolve()  # noqa: SLF001

    def check_the_checkout_root_is_found_from_deeper_still(self, tmp_path, in_dir):
        root = _checkout(tmp_path)
        in_dir(str(root.joinpath(*TESTS) / "ignitetest" / "tests"))
        assert run._source_root(_context()) == root.resolve()  # noqa: SLF001

    def check_the_root_itself_is_recognised(self, tmp_path, in_dir):
        root = _checkout(tmp_path)
        in_dir(str(root))
        assert run._source_root(_context()) == root.resolve()  # noqa: SLF001

    def check_an_explicit_root_is_taken_as_given(self, tmp_path, in_dir):
        root = _checkout(tmp_path)
        in_dir(str(tmp_path))
        ctx = _context(source_root=str(root))
        assert run._source_root(ctx) == root.resolve()  # noqa: SLF001

    def check_a_directory_outside_any_checkout_falls_back_to_itself(self, tmp_path, in_dir):
        # Kept as the old behaviour so the failure comes from the validation below, with
        # a message, rather than from an unrelated parent directory being picked up.
        elsewhere = tmp_path / "state"
        elsewhere.mkdir()
        in_dir(str(elsewhere))
        assert run._source_root(_context()) == elsewhere.resolve()  # noqa: SLF001


class CheckSourceRootValidation:
    """The mistake this exists for: syncing something that is not the checkout."""

    def check_a_non_checkout_is_refused_before_anything_is_uploaded(self, tmp_path):
        state = tmp_path / ".ducktests-remote"
        state.mkdir()
        with pytest.raises(ConfigError) as ex:
            run._check_source_root(_context(), state)  # noqa: SLF001
        message = str(ex.value)
        assert str(state) in message, "the message names the directory that was wrong"
        assert "modules/ducktests/tests/docker/requirements.txt" in message
        assert "--source-root" in message, "and how to fix it"

    def check_a_real_checkout_passes(self, tmp_path):
        run._check_source_root(_context(), _checkout(tmp_path))  # noqa: SLF001

    def check_no_sync_against_an_unseen_tree_is_allowed(self, tmp_path):
        # With --no-sync the path describes the runner, which the coordinator cannot check.
        run._check_source_root(_context(no_sync=True),  # noqa: SLF001
                               tmp_path / "on-the-runner-only")

    def check_no_sync_still_refuses_a_local_directory_that_is_wrong(self, tmp_path):
        state = tmp_path / ".ducktests-remote"
        state.mkdir()
        with pytest.raises(ConfigError):
            run._check_source_root(_context(no_sync=True), state)  # noqa: SLF001


class CheckTestPaths:
    """ducktape runs from the tests directory, so the paths it gets are relative to it."""

    @staticmethod
    def _paths(root, given, cwd=None):
        os.chdir(str(cwd or root.joinpath(*TESTS)))
        return run._test_paths(_context(), given, root,  # noqa: SLF001
                               "/state/runs/r/src")

    def check_a_docker_style_path_survives_unchanged(self, tmp_path, in_dir):
        in_dir(str(tmp_path))
        assert self._paths(_checkout(tmp_path), ["./ignitetest/tests/smoke_test.py"]) == \
            ["./ignitetest/tests/smoke_test.py"]

    def check_the_class_and_method_suffix_is_preserved(self, tmp_path, in_dir):
        in_dir(str(tmp_path))
        given = "./ignitetest/tests/smoke_test.py::SmokeServicesTest.test_ignite_start_stop"
        assert self._paths(_checkout(tmp_path), [given]) == [given]

    def check_a_repository_relative_path_is_shortened(self, tmp_path, in_dir):
        # What README used to require, typed from the checkout root.
        in_dir(str(tmp_path))
        root = _checkout(tmp_path)
        given = "./modules/ducktests/tests/ignitetest/tests/smoke_test.py"
        assert self._paths(root, [given], cwd=root) == ["./ignitetest/tests/smoke_test.py"]

    def check_an_absolute_path_is_shortened_too(self, tmp_path, in_dir):
        in_dir(str(tmp_path))
        root = _checkout(tmp_path)
        absolute = str(root.joinpath(*TESTS) / "ignitetest" / "tests" / "smoke_test.py")
        assert self._paths(root, [absolute]) == ["./ignitetest/tests/smoke_test.py"]

    def check_a_path_inside_the_checkout_but_outside_tests_becomes_absolute(
            self, tmp_path, in_dir):
        # Relative would be resolved against the tests directory and miss; the runner-side
        # absolute path is the only form that cannot be misread.
        in_dir(str(tmp_path))
        root = _checkout(tmp_path)
        (root / "extra").mkdir(exist_ok=True)
        (root / "extra" / "other_test.py").write_text("# t\n", encoding="utf-8")
        assert self._paths(root, ["./extra/other_test.py"], cwd=root) == \
            ["/state/runs/r/src/extra/other_test.py"]

    def check_an_unresolvable_path_is_passed_through(self, tmp_path, in_dir):
        # --no-sync against a checkout this machine does not have.
        in_dir(str(tmp_path))
        assert self._paths(_checkout(tmp_path), ["./ignitetest/tests/absent.py"]) == \
            ["./ignitetest/tests/absent.py"]


class CheckSyncProgress:
    """The 171 MB that goes to the runner before a run starts."""

    class _Paths:  # pylint: disable=too-few-public-methods
        src_dir = "/state/runs/r/src"

    def _sync(self, tmp_path, **args):
        root = _checkout(tmp_path)
        args.setdefault("exclude", [])
        ctx = _context(**args)
        run._sync_sources(ctx, root, self._Paths())  # noqa: SLF001
        return ctx._runner  # noqa: SLF001

    def check_the_sync_is_watched(self, tmp_path):
        runner = self._sync(tmp_path)
        assert runner.uploads and runner.uploads[0][2] == "dir"
        assert runner.reported, "a sync that takes minutes has to show movement"

    def check_no_progress_still_syncs(self, tmp_path):
        runner = self._sync(tmp_path, no_progress=True)
        assert runner.uploads, "the transfer happens either way"
        assert not runner.reported, "--no-progress means nothing is asked to report"
