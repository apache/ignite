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
``clean`` - kill leftover Ignite JVMs and remove work directories.

Stale JVMs from an aborted run are the single most common source of baffling failures on
a shared VM cluster, and the second most common is a work directory that still holds a
previous run's persistence.  Both are removed here, and only here.
"""

import posixpath
import shlex

from ducktests_remote.cli import EXIT_OK, EXIT_TRANSPORT
from ducktests_remote.config import IGNITE_MAIN_CLASSES, ConfigError
from ducktests_remote.fanout import (CHANGED, FAILED, HostResult, OK, any_failed, fanout,
                                     render_table, summarise)


def register(subparsers, common):
    """Wire up the ``clean`` subcommand."""
    parser = subparsers.add_parser(
        "clean", parents=[common], help="kill stale Ignite processes and remove work dirs",
        description="Fan out across the workers, terminate anything matching "
                    "clean.process_pattern, and remove clean.paths. Run it with --dry-run "
                    "first: it prints exactly what it would kill and delete.")
    parser.add_argument("-n", "--num-nodes", type=int, default=None,
                        help="only clean the first N inventory hosts")
    parser.add_argument("--keep-paths", action="store_true",
                        help="kill processes but leave the work directories in place")
    parser.set_defaults(handler=execute)


def execute(ctx):
    """Clean the workers. :return: the process exit code."""
    nodes = ctx.all_nodes
    if not nodes:
        raise ConfigError("cluster.nodes is empty; there is nothing to clean")
    results = clean_hosts(ctx, nodes, dry_run=ctx.dry_run,
                          remove_paths=not ctx.args.keep_paths)
    return EXIT_TRANSPORT if any_failed(results) else EXIT_OK


def clean_hosts(ctx, nodes, *, dry_run=False, remove_paths=True):
    """Run the clean fan-out. :return: the per-host results."""
    pattern = ctx.config["clean"]["process_pattern"]
    paths = validated_paths(ctx.config["clean"]) if remove_paths else []
    script = _script(pattern, paths, dry_run=dry_run)

    ctx.console.info("pattern: %s" % pattern)
    ctx.console.info("paths  : %s" % (", ".join(paths) if paths else "(none)"))
    if dry_run:
        ctx.console.info("--dry-run: nothing will be killed or removed")

    def operation(node):
        result = ctx.worker(node).run_script(script, check=False)
        if not result.ok:
            return HostResult(node.host, FAILED, "clean failed", detail=result.stderr.strip())
        lines = [ln for ln in result.stdout.splitlines() if ln.strip()]
        killed = [ln for ln in lines if ln.startswith("proc ")]
        removed = [ln for ln in lines if ln.startswith("path ")]
        status = CHANGED if (killed or removed) else OK
        message = "%d process(es), %d path(s)" % (len(killed), len(removed))
        return HostResult(node.host, status, message, detail="\n".join(lines))

    results = fanout(nodes, operation, jobs=ctx.jobs,
                     fail_fast=getattr(ctx.args, "fail_fast", False))
    ctx.console.out(render_table(results, verbose=ctx.console.verbose))
    ctx.console.out("")
    ctx.console.out(summarise(results))
    return results


def validated_paths(clean_cfg):
    """
    Check every configured path against the allow-list before it is ever sent to a host.

    A bug here deletes distributions across every machine in the cluster at once, so the
    rule is deliberately blunt: a path must sit under one of ``clean.allowed_roots``, and
    the roots themselves are not removable.
    """
    allowed = [posixpath.normpath(p) for p in clean_cfg.get("allowed_roots") or []]
    checked = []
    for raw in clean_cfg.get("paths") or []:
        path = posixpath.normpath(str(raw))
        if not path.startswith("/"):
            raise ConfigError("clean.paths: %r must be absolute" % raw)
        if path in ("/", "") or path in allowed:
            raise ConfigError("clean.paths: refusing to remove %r" % raw)
        if not any(path == root or path.startswith(root.rstrip("/") + "/") for root in allowed):
            raise ConfigError(
                "clean.paths: %r is outside clean.allowed_roots (%s). Add the root "
                "explicitly if you really mean it." % (raw, ", ".join(allowed)))
        checked.append(path)
    return checked


def _script(pattern, paths, *, dry_run):
    quoted_paths = " ".join(shlex.quote(p) for p in paths)
    return """set -u
pattern=%(pattern)s
dry=%(dry)d
pids=$(pgrep -f "$pattern" 2>/dev/null || true)
for p in $pids; do
  cmd=$(ps -o args= -p "$p" 2>/dev/null | cut -c1-100)
  echo "proc $p $cmd"
done
if [ "$dry" -eq 0 ] && [ -n "$pids" ]; then
  kill -TERM $pids 2>/dev/null || true
  sleep 5
  left=$(pgrep -f "$pattern" 2>/dev/null || true)
  [ -n "$left" ] && kill -KILL $left 2>/dev/null || true
fi
for d in %(paths)s; do
  [ -e "$d" ] || continue
  echo "path $d ($(du -sh "$d" 2>/dev/null | cut -f1))"
  if [ "$dry" -eq 0 ]; then rm -rf -- "$d"; fi
done
exit 0
""" % {"pattern": shlex.quote(pattern), "dry": 1 if dry_run else 0, "paths": quoted_paths}


def known_main_classes():
    """:return: the Ignite main classes ignitetest launches, for documentation and help."""
    return IGNITE_MAIN_CLASSES
