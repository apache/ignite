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

"""``logs`` - print or follow a run's ``ducktape.log`` from the runner."""

import sys
import time

from ducktests_remote import runs
from ducktests_remote.cli import EXIT_OK, EXIT_USAGE

POLL_SEC = 1.5


def register(subparsers, common):
    """Wire up the ``logs`` subcommand."""
    parser = subparsers.add_parser(
        "logs", parents=[common], help="print or follow a run's log",
        description="Stream ducktape.log from the runner. Ctrl-C stops following; it "
                    "never touches the run.")
    parser.add_argument("run_id", nargs="?", help="run id (default: the most recent run)")
    parser.add_argument("-f", "--follow", action="store_true", help="keep streaming")
    parser.add_argument("-n", "--lines", type=int, default=200,
                        help="lines of history to print first (default 200)")
    parser.set_defaults(handler=execute)


def execute(ctx):
    """Print or follow the log. :return: the process exit code."""
    state_root = ctx.state_root_resolved()
    run_id = runs.resolve_run_id(ctx.runner, state_root, ctx.args.run_id)
    if not run_id:
        ctx.console.error("no runs found under %s on %s" % (state_root, ctx.runner_host))
        return EXIT_USAGE

    paths = runs.RunPaths(state_root, run_id)
    redact = ctx.console.redactor.redact

    history = ctx.runner.run(["tail", "-n", str(ctx.args.lines), paths.log_file], check=False)
    if history.stdout:
        sys.stdout.write(redact(history.stdout))
        sys.stdout.flush()

    if not ctx.args.follow:
        return EXIT_OK

    offset = _size(ctx, paths) + 1
    try:
        while True:
            chunk = ctx.runner.run(["tail", "-c", "+%d" % offset, paths.log_file], check=False)
            if chunk.stdout:
                offset += len(chunk.stdout.encode("utf-8"))
                sys.stdout.write(redact(chunk.stdout))
                sys.stdout.flush()
            state = runs.read_state(ctx.runner, paths)
            if state.exit_code is not None:
                ctx.console.out("")
                ctx.console.out("run %s %s (ducktape exit %s)"
                                % (run_id, state.state, state.exit_code))
                return EXIT_OK
            time.sleep(POLL_SEC)
    except KeyboardInterrupt:
        ctx.console.out("")
        ctx.console.out("Stopped following. The run is untouched; "
                        "`ducktests-remote stop %s` stops it." % run_id)
        return EXIT_OK


def _size(ctx, paths):
    result = ctx.runner.run(["wc", "-c", paths.log_file], check=False)
    try:
        return int(result.out.split()[0])
    except (IndexError, ValueError):
        return 0
