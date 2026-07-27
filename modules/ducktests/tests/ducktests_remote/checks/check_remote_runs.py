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

"""Checks for run ids, run directory layout, state derivation, and run.sh rendering."""

from datetime import datetime

from fake_transport import FakeTransport

from ducktests_remote import runs

GOLDEN_RUN_SH = """set -euo pipefail

cd '/opt/my sources/ignite'
# activate the runner venv; `set +u` because older activate
# scripts read unset variables
set +u
. '/opt/venvs/dt env/bin/activate'
set -u

exec ducktape \\
  --results-root /state/runs/r/results \\
  --cluster-file /state/runs/r/cluster.json \\
  --globals /state/runs/r/globals.json \\
  --parameters /state/runs/r/parameters.json \\
  --repeat 3 \\
  --max-parallel 4 \\
  --test-runner-timeout 900000 \\
  --debug \\
  './tests/a b.py::Cls.test' './tests/{braces}.py'
"""


class CheckRunId:
    """Format and uniqueness."""

    def check_format(self):
        run_id = runs.new_run_id("max", now=datetime(2026, 7, 27, 14, 12, 33), entropy="9f2a")
        assert run_id == "max-20260727-141233-9f2a"
        assert runs.is_run_id(run_id)

    def check_unsafe_characters_in_the_user_are_replaced(self):
        run_id = runs.new_run_id("DOMAIN\\user", now=datetime(2026, 1, 1), entropy="0000")
        assert runs.is_run_id(run_id)

    def check_uniqueness_within_the_same_second(self):
        now = datetime(2026, 7, 27, 14, 12, 33)
        ids = {runs.new_run_id("max", now=now) for _ in range(200)}
        assert len(ids) > 190, "the hex suffix must keep same-second runs apart"

    def check_rejects_arbitrary_directory_names(self):
        assert not runs.is_run_id("latest")
        assert not runs.is_run_id("results")


class CheckPaths:
    """Run directory layout."""

    def check_layout(self):
        paths = runs.RunPaths("/state", "max-20260727-141233-9f2a")
        assert paths.run_dir == "/state/runs/max-20260727-141233-9f2a"
        assert paths.globals_file.endswith("/globals.json")
        assert paths.results_dir.endswith("/results")
        assert paths.latest_link == "/state/runs/latest"
        assert paths.src_dir == "/state/src/max-20260727-141233-9f2a"


class CheckStateDerivation:
    """The three observable facts map onto one state."""

    def check_running(self):
        assert runs.derive_state(pid_alive=True, exit_code=None, stopped=False) == runs.RUNNING

    def check_finished(self):
        assert runs.derive_state(pid_alive=False, exit_code=0, stopped=False) == runs.FINISHED

    def check_failed(self):
        assert runs.derive_state(pid_alive=False, exit_code=1, stopped=False) == runs.FAILED

    def check_stopped(self):
        assert runs.derive_state(pid_alive=False, exit_code=143, stopped=True) == runs.STOPPED

    def check_exit_code_beats_a_reused_pid(self):
        assert runs.derive_state(pid_alive=True, exit_code=0, stopped=False) == runs.FINISHED

    def check_unknown(self):
        assert runs.derive_state(pid_alive=False, exit_code=None, stopped=False) == runs.UNKNOWN

    def check_read_state_parses_the_probe_output(self):
        transport = FakeTransport()
        transport.when("bash", stdout=("pid=4242\npgid=4242\nalive=1\n"
                                       "meta={\"test_paths\": [\"a.py\"]}\n"))
        state = runs.read_state(transport, runs.RunPaths("/state", "r"))
        assert state.pid == 4242 and state.state == runs.RUNNING
        assert state.meta["test_paths"] == ["a.py"]


class CheckListing:
    """Only real run directories are listed."""

    def check_latest_symlink_is_not_a_run(self):
        transport = FakeTransport()
        transport.when("ls", stdout="latest\nmax-20260727-141233-9f2a\n"
                                    "max-20260726-090000-aaaa\n")
        ids = runs.list_run_ids(transport, "/state")
        assert ids == ["max-20260727-141233-9f2a", "max-20260726-090000-aaaa"]


class CheckRunScript:
    """Golden-file rendering, including paths with spaces and shell metacharacters."""

    def _render(self):
        return runs.render_run_script(
            version="0.1.0", timestamp="2026-07-27T00:00:00+00:00", author="max@laptop",
            work_dir="/opt/my sources/ignite",
            results_root="/state/runs/r/results",
            cluster_file="/state/runs/r/cluster.json",
            globals_file="/state/runs/r/globals.json",
            parameters_file="/state/runs/r/parameters.json",
            test_paths=["./tests/a b.py::Cls.test", "./tests/{braces}.py"],
            venv="/opt/venvs/dt env",
            repeat=3, max_parallel=4, test_runner_timeout=900000,
            extra_args=["--debug"])

    def check_golden_body(self):
        rendered = self._render()
        body = rendered[rendered.index("set -euo pipefail"):]
        assert body == GOLDEN_RUN_SH

    def check_header_records_provenance(self):
        rendered = self._render()
        assert "Generated by ducktests-remote 0.1.0" in rendered
        assert "max@laptop" in rendered

    def check_globals_is_passed_as_a_file_path(self):
        # ducktape 0.13 checks os.path.isfile before parsing --globals as JSON
        # (command_line/main.py::get_user_defined_globals), so the blob never has to
        # cross a shell command line.
        rendered = self._render()
        assert "--globals /state/runs/r/globals.json" in rendered
        assert "{" not in rendered.split("exec ducktape")[1].replace("{braces}", "")

    def check_optional_flags_are_omitted_when_unset(self):
        rendered = runs.render_run_script(
            version="0.1.0", timestamp="t", author="a", work_dir="/w",
            results_root="/r", cluster_file="/c", globals_file="/g",
            test_paths=["./t.py"], venv=None)
        assert "--parameters" not in rendered
        assert "--repeat" not in rendered
        assert "--max-parallel" not in rendered
        assert "no venv configured" in rendered

    def check_a_json_like_test_path_is_quoted(self):
        rendered = runs.render_run_script(
            version="0.1.0", timestamp="t", author="a", work_dir="/w",
            results_root="/r", cluster_file="/c", globals_file="/g",
            test_paths=['./t.py::C.m@{"x": 1}'], venv=None)
        assert """'./t.py::C.m@{"x": 1}'""" in rendered


class CheckLaunchScripts:
    """Detachment mechanics."""

    def check_launch_records_the_exit_code(self):
        script = runs.render_launch_script(runs.RunPaths("/state", "r"))
        assert 'echo $? > "$rd/exit_code"' in script
        assert 'pgid' in script

    def check_detach_prefers_setsid_and_falls_back(self):
        script = runs.render_detach_script(runs.RunPaths("/state", "r"))
        assert "setsid nohup bash" in script
        assert "disown" in script, "a runner without setsid must still detach"
        assert 'echo $! > "$rd/pid"' in script


class CheckFormatting:
    """Duration rendering."""

    def check_durations(self):
        assert runs.format_duration(None) == "-"
        assert runs.format_duration(42) == "42s"
        assert runs.format_duration(125) == "2m 05s"
        assert runs.format_duration(3852) == "1h 04m 12s"
