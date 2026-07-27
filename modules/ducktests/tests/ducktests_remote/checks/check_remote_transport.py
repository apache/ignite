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

"""Checks for the transport boundary, the import guard, and exit-code mapping."""

import subprocess
import sys
from pathlib import Path

import pytest
from fake_transport import FakeTransport

from ducktests_remote import cli, runs
from ducktests_remote.transport import (LocalTransport, ProxiedTransport, Result,
                                        SshTransport, TransportError, is_excluded)

PACKAGE_ROOT = Path(__file__).resolve().parent.parent


class CheckImportGuard:
    """``ducktests_remote`` must be usable on a coordinator with no ducktape at all."""

    def check_ducktape_is_not_in_the_import_graph(self):
        script = (
            "import sys\n"
            "import ducktests_remote.cli as c\n"
            "c.build_parser()\n"
            "leaked = [m for m in sys.modules if m == 'ducktape' "
            "or m.startswith('ducktape.')]\n"
            "assert not leaked, leaked\n"
            "print('clean')\n")
        result = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                text=True, check=False, cwd=str(PACKAGE_ROOT.parent))
        assert result.returncode == 0, result.stderr
        assert "clean" in result.stdout

    def check_no_source_file_imports_ducktape(self):
        offenders = []
        for path in PACKAGE_ROOT.rglob("*.py"):
            if "checks" in path.parts:
                continue
            text = path.read_text(encoding="utf-8")
            for line in text.splitlines():
                stripped = line.strip()
                if stripped.startswith(("import ducktape", "from ducktape")):
                    offenders.append("%s: %s" % (path.name, stripped))
        assert not offenders, offenders


class CheckTransportEquivalence:
    """Above the transport boundary, local and ssh runs are indistinguishable."""

    def check_identical_command_sequences(self):
        sequences = []
        for name in ("local", "build-vm-01"):
            transport = FakeTransport(name=name)
            transport.mkdirs("/state/runs/r")
            transport.run(["mkdir", "-p", "/state/runs/r/results"])
            transport.write_file("{}", "/state/runs/r/globals.json", mode=0o600)
            runs.read_state(transport, runs.RunPaths("/state", "r"))
            sequences.append((transport.commands, sorted(transport.files)))
        assert sequences[0] == sequences[1]


class CheckSshOptions:
    """The ssh client is the system one, driven with deliberate options."""

    def check_batch_mode_is_always_on(self):
        # An interactive password prompt inside a fan-out across N hosts hangs the whole
        # run, so a missing key is a diagnosable failure rather than a prompt.
        opts = SshTransport(name="h").ssh_options()
        assert "BatchMode=yes" in opts

    def check_identity_is_used_exclusively_when_given(self):
        opts = SshTransport(name="h", identity_file="/k").ssh_options()
        assert "IdentitiesOnly=yes" in opts and "/k" in opts

    def check_non_default_port_uses_the_right_flag(self):
        assert "-p" in SshTransport(name="h", port=2222).ssh_options()
        assert "-P" in SshTransport(name="h", port=2222).ssh_options(for_scp=True)

    def check_default_port_is_not_passed(self):
        assert "-p" not in SshTransport(name="h", port=22).ssh_options()

    def check_target_includes_the_user(self):
        assert SshTransport(name="h", user="max").target == "max@h"
        assert SshTransport(name="h").target == "h"


class CheckProxiedTransport:
    """``deploy --via`` and runner-side probing hop through another transport."""

    def check_command_is_wrapped_in_ssh_on_the_intermediate_host(self):
        via = FakeTransport(name="jump")
        proxied = ProxiedTransport(name="node01", via=via, user="max")
        proxied.run(["true"], check=False)
        argv = via.commands[-1]
        assert argv[0] == "ssh" and "max@node01" in argv


class CheckLocalTransport:
    """The local transport really does run things."""

    def check_run_captures_output(self):
        transport = LocalTransport()
        result = transport.run([sys.executable, "-c", "print('hi')"])
        assert result.out == "hi"

    def check_failure_raises_with_the_command_in_the_message(self):
        transport = LocalTransport()
        with pytest.raises(TransportError) as ex:
            transport.run([sys.executable, "-c", "import sys;sys.exit(3)"])
        assert "exit 3" in str(ex.value)

    def check_missing_binary_is_a_transport_error(self):
        with pytest.raises(TransportError):
            LocalTransport().run(["definitely-not-a-real-binary-xyz"])

    def check_dry_run_executes_nothing(self):
        printed = []
        transport = LocalTransport(dry_run=True, printer=printed.append)
        result = transport.run([sys.executable, "-c", "raise SystemExit(9)"])
        assert result.ok and printed and printed[0].startswith("[dry-run]")

    def check_tilde_expansion_uses_the_remote_home(self):
        transport = FakeTransport(home="/home/tester")
        assert transport.expand("~/.ducktests-remote") == "/home/tester/.ducktests-remote"
        assert transport.expand("/absolute") == "/absolute"


class CheckExcludes:
    """Sync exclusions behave like rsync patterns."""

    @pytest.mark.parametrize("path", [".git/config", "target/classes/A.class",
                                      "mod/target/x.jar", "a/b/__pycache__/c.pyc",
                                      "x/y.pyc", "ignitetest.egg-info/PKG-INFO"])
    def check_excluded(self, path):
        assert is_excluded(path, [".git", "target", "__pycache__", "*.pyc", "*.egg-info"])

    @pytest.mark.parametrize("path", ["ignitetest/tests/smoke_test.py", "README.md",
                                      "targeting/notes.txt"])
    def check_kept(self, path):
        assert not is_excluded(path, [".git", "target", "__pycache__", "*.pyc"])


class CheckExitCodes:
    """Jenkins needs 'tests failed' to be distinguishable from 'the cluster is broken'."""

    def check_values(self):
        assert (cli.EXIT_OK, cli.EXIT_USAGE, cli.EXIT_PREFLIGHT, cli.EXIT_BUSY,
                cli.EXIT_TESTS_FAILED, cli.EXIT_TRANSPORT, cli.EXIT_INTERRUPTED) \
            == (0, 1, 2, 3, 4, 5, 130)

    def check_tests_failed_is_not_a_transport_error(self):
        assert cli.EXIT_TESTS_FAILED != cli.EXIT_TRANSPORT

    def check_config_error_maps_to_usage(self):
        assert cli.main(["--config", "/nope/missing.yaml", "doctor"]) == cli.EXIT_USAGE

    def check_no_command_prints_help_and_reports_usage(self, capsys):
        assert cli.main([]) == cli.EXIT_USAGE
        assert "ducktests-remote" in capsys.readouterr().out

    def check_passthrough_split(self):
        head, tail = cli.split_passthrough(["run", "-t", "a.py", "--", "--debug", "--sample", "3"])
        assert head == ["run", "-t", "a.py"]
        assert tail == ["--debug", "--sample", "3"]

    def check_passthrough_absent(self):
        assert cli.split_passthrough(["status"]) == (["status"], [])


class CheckResult:
    """Result helpers."""

    def check_ok_and_out(self):
        result = Result(["true"], 0, "  value \n")
        assert result.ok and result.out == "value"
