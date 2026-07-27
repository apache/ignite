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

"""``stop`` - terminate a detached run and, by default, clean the workers behind it."""

import shlex

from ducktests_remote import runs
from ducktests_remote.cli import EXIT_OK, EXIT_USAGE


def register(subparsers, common):
    """Wire up the ``stop`` subcommand."""
    parser = subparsers.add_parser(
        "stop", parents=[common], help="stop a run and clean up after it",
        description="SIGTERM the run's process group, wait, then optionally SIGKILL. "
                    "Cleans the workers afterwards unless --no-clean.")
    parser.add_argument("run_id", nargs="?", help="run id (default: the most recent run)")
    parser.add_argument("--kill", action="store_true",
                        help="SIGKILL survivors after the timeout")
    parser.add_argument("--timeout", type=int, default=60,
                        help="seconds to wait for a graceful exit (default 60)")
    parser.add_argument("--no-clean", dest="clean", action="store_false", default=True,
                        help="leave the workers alone; the JVMs will be someone else's problem")
    parser.add_argument("-n", "--num-nodes", type=int, default=None,
                        help="limit the follow-up clean to the first N inventory hosts")
    parser.set_defaults(handler=execute)


def execute(ctx):
    """Stop a run. :return: the process exit code."""
    state_root = ctx.state_root_resolved()
    run_id = runs.resolve_run_id(ctx.runner, state_root, ctx.args.run_id)
    if not run_id:
        ctx.console.error("no runs found under %s on %s" % (state_root, ctx.runner_host))
        return EXIT_USAGE
    paths = runs.RunPaths(state_root, run_id)
    return stop_run(ctx, paths, timeout=ctx.args.timeout, kill=ctx.args.kill,
                    clean=ctx.args.clean)


def stop_run(ctx, paths, *, timeout=60, kill=False, clean=True):
    """Stop the run described by ``paths``. :return: the process exit code."""
    console = ctx.console
    state = runs.read_state(ctx.runner, paths)

    if state.exit_code is not None:
        console.info("run %s already ended (%s, exit %s)"
                     % (paths.run_id, state.state, state.exit_code))
    else:
        console.info("stopping %s (pid %s, pgid %s)" % (paths.run_id, state.pid, state.pgid))
        result = ctx.runner.run_script(_stop_script(paths, timeout, kill), check=False)
        console.detail(result.stdout.strip())
        if not result.ok and not ctx.dry_run:
            console.warn("stop script exited %d: %s" % (result.returncode,
                                                        result.stderr.strip()[:200]))

    if clean:
        console.heading("CLEANING WORKERS")
        from ducktests_remote.commands import clean as clean_cmd  # pylint: disable=C0415
        clean_cmd.clean_hosts(ctx, ctx.all_nodes, dry_run=ctx.dry_run)

    console.out("stopped %s" % paths.run_id)
    return EXIT_OK


def _stop_script(paths, timeout, kill):
    return """set -u
rd=%(rd)s
touch "$rd/stopped"
pid=$(cat "$rd/pid" 2>/dev/null || true)
pgid=$(cat "$rd/pgid" 2>/dev/null || true)
target=""
if [ -n "$pgid" ]; then target="-$pgid"; elif [ -n "$pid" ]; then target="$pid"; fi
if [ -z "$target" ]; then echo "no pid recorded"; exit 0; fi
kill -TERM -- "$target" 2>/dev/null || true
i=0
while [ "$i" -lt %(timeout)d ]; do
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then sleep 1; i=$((i+1)); else break; fi
done
if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
  if [ %(kill)d -eq 1 ]; then
    echo "still alive after %(timeout)ds, sending SIGKILL"
    kill -KILL -- "$target" 2>/dev/null || true
  else
    echo "still alive after %(timeout)ds; rerun with --kill"
  fi
else
  echo "exited within %(timeout)ds"
fi
[ -f "$rd/exit_code" ] || echo 143 > "$rd/exit_code"
""" % {"rd": shlex.quote(paths.run_dir), "timeout": int(timeout), "kill": 1 if kill else 0}
