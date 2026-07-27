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

"""``status`` - what a run is doing, or a table of every run on the runner."""

import json

from ducktests_remote import runs
from ducktests_remote.cli import EXIT_OK, EXIT_USAGE


def register(subparsers, common):
    """Wire up the ``status`` subcommand."""
    parser = subparsers.add_parser(
        "status", parents=[common], help="show the state of a run",
        description="With no run id, report the most recent run on the runner. Run state "
                    "lives on the runner, so this works from any coordinator.")
    parser.add_argument("run_id", nargs="?", help="run id (default: the most recent run)")
    parser.add_argument("--all", action="store_true", help="table of every run, newest first")
    parser.add_argument("--json", action="store_true", help="machine-readable output")
    parser.add_argument("-n", "--lines", type=int, default=15,
                        help="lines of log tail to show (default 15)")
    parser.set_defaults(handler=execute)


def execute(ctx):
    """Print run status. :return: the process exit code."""
    state_root = ctx.state_root_resolved()

    if ctx.args.all:
        return _print_all(ctx, state_root)

    run_id = runs.resolve_run_id(ctx.runner, state_root, ctx.args.run_id)
    if not run_id:
        ctx.console.error("no runs found under %s on %s" % (state_root, ctx.runner_host))
        return EXIT_USAGE

    paths = runs.RunPaths(state_root, run_id)
    state = runs.read_state(ctx.runner, paths)

    if ctx.args.json:
        ctx.console.out(json.dumps(_as_dict(paths, state), indent=2, sort_keys=True))
        return EXIT_OK

    console = ctx.console
    meta = state.meta
    console.out("run id     : %s" % state.run_id)
    console.out("state      : %s" % console.paint(state.state, _style(state.state)))
    console.out("runner     : %s" % ctx.runner_host)
    console.out("pid        : %s" % (state.pid if state.pid is not None else "-"))
    console.out("started    : %s" % (state.started_at or "-"))
    console.out("elapsed    : %s" % runs.format_duration(state.elapsed))
    console.out("exit code  : %s" % (state.exit_code if state.exit_code is not None else "-"))
    console.out("tests      : %s" % ", ".join(meta.get("test_paths") or []) or "-")
    console.out("cluster    : %s (%d node(s))" % (meta.get("cluster") or "-",
                                                  len(meta.get("nodes") or [])))
    console.out("run dir    : %s" % paths.run_dir)
    console.out("results    : %s" % (meta.get("results_root") or paths.results_dir))

    tail = ctx.runner.run(["tail", "-n", str(ctx.args.lines), paths.log_file], check=False)
    if tail.stdout.strip():
        console.heading("last %d log lines" % ctx.args.lines)
        console.out(tail.stdout.rstrip())
    return EXIT_OK


def _print_all(ctx, state_root):
    run_ids = runs.list_run_ids(ctx.runner, state_root)
    if not run_ids:
        ctx.console.out("no runs under %s on %s" % (state_root, ctx.runner_host))
        return EXIT_OK

    rows = []
    for run_id in run_ids:
        paths = runs.RunPaths(state_root, run_id)
        state = runs.read_state(ctx.runner, paths)
        rows.append((run_id, state.state,
                     runs.format_duration(state.elapsed),
                     "-" if state.exit_code is None else str(state.exit_code),
                     ", ".join(state.meta.get("test_paths") or [])[:60]))

    if ctx.args.json:
        ctx.console.out(json.dumps(
            [dict(zip(("run_id", "state", "elapsed", "exit_code", "tests"), row))
             for row in rows], indent=2))
        return EXIT_OK

    headers = ("RUN ID", "STATE", "ELAPSED", "EXIT", "TESTS")
    widths = [max(len(str(r[i])) for r in ([headers] + rows)) for i in range(5)]
    ctx.console.out("  ".join(h.ljust(widths[i]) for i, h in enumerate(headers)).rstrip())
    ctx.console.out("  ".join("-" * widths[i] for i in range(5)))
    for row in rows:
        ctx.console.out("  ".join(str(c).ljust(widths[i]) for i, c in enumerate(row)).rstrip())
    return EXIT_OK


def _as_dict(paths, state):
    return {"run_id": state.run_id, "state": state.state, "pid": state.pid,
            "exit_code": state.exit_code, "run_dir": paths.run_dir,
            "elapsed_sec": state.elapsed, "meta": state.meta}


def _style(state):
    return {runs.RUNNING: "blue", runs.FINISHED: "green", runs.FAILED: "red",
            runs.STOPPED: "yellow"}.get(state, "dim")
