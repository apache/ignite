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

"""``fetch`` - bring a run's reports back to the coordinator."""

import posixpath
import shlex
import tarfile
import tempfile
import uuid
from pathlib import Path

from ducktests_remote import runs
from ducktests_remote.cli import EXIT_OK, EXIT_USAGE
from ducktests_remote.config import expand_path

# ducktape writes these into <results-root>/<session-id>; see ducktape/tests/reporter.py.
DEFAULT_FILES = ("report.html", "report.txt", "report.json", "test_log.info", "session.log")

# globals.json holds the composed secrets. It never leaves the runner.
ALWAYS_EXCLUDED = ("globals.json",)


def register(subparsers, common):
    """Wire up the ``fetch`` subcommand."""
    parser = subparsers.add_parser(
        "fetch", parents=[common], help="download a run's results",
        description="Download the reports for a run. globals.json is always excluded.")
    parser.add_argument("run_id", nargs="?", help="run id (default: the most recent run)")
    parser.add_argument("--dest", metavar="DIR", default="./ducktests-results",
                        help="coordinator-side destination (default ./ducktests-results)")
    parser.add_argument("--full", action="store_true",
                        help="download the whole results tree, not just the reports")
    parser.set_defaults(handler=execute)


def execute(ctx):
    """Fetch results. :return: the process exit code."""
    state_root = ctx.state_root_resolved()
    run_id = runs.resolve_run_id(ctx.runner, state_root, ctx.args.run_id)
    if not run_id:
        ctx.console.error("no runs found under %s on %s" % (state_root, ctx.runner_host))
        return EXIT_USAGE

    paths = runs.RunPaths(state_root, run_id)
    dest = Path(expand_path(ctx.args.dest)) / run_id

    if ctx.dry_run:
        ctx.console.out("would download %s -> %s (%s), excluding %s"
                        % (paths.results_dir, dest,
                           "everything" if ctx.args.full else ", ".join(DEFAULT_FILES),
                           ", ".join(ALWAYS_EXCLUDED)))
        return EXIT_OK

    dest.mkdir(parents=True, exist_ok=True)
    # Staged inside the run directory rather than /tmp: it is known to exist, known to
    # be writable by this account, and disappears with the run.
    staged = paths.path(".fetch-%s.tar.gz" % uuid.uuid4().hex[:8])

    script = _archive_script(paths, staged, full=ctx.args.full)
    result = ctx.runner.run_script(script, check=False)
    if not result.ok:
        ctx.console.error("could not archive results on the runner: %s"
                          % (result.stderr.strip() or result.stdout.strip()))
        return EXIT_USAGE

    with tempfile.TemporaryDirectory() as tmp:
        local_archive = Path(tmp) / "results.tar.gz"
        ctx.runner.download(staged, local_archive)
        ctx.runner.run(["rm", "-f", "--", staged], check=False)
        with tarfile.open(local_archive, "r:gz") as tar:
            members = [m for m in tar.getmembers()
                       if posixpath.basename(m.name) not in ALWAYS_EXCLUDED]
            _safe_extract(tar, members, dest)

    ctx.runner.download(paths.log_file, dest / "ducktape.log")

    ctx.console.out("results: %s" % dest.resolve())
    for report in sorted(dest.rglob("report.*")):
        ctx.console.out("  %s" % report.relative_to(dest).as_posix())
    return EXIT_OK


def _archive_script(paths, staged, *, full):
    """
    Build the tar command that stages the results on the runner.

    Archiving first and downloading one file keeps the transport free of binary
    streaming, and makes the exclusion of globals.json explicit at both ends.
    """
    excludes = " ".join("--exclude=%s" % shlex.quote(name) for name in ALWAYS_EXCLUDED)
    # tar treats an output path containing a colon as host:path, so cd into the results
    # directory and write the archive through a relative name.
    out = posixpath.join("..", posixpath.basename(staged))
    common = ('set -eu\nroot=%s\n'
              '[ -d "$root" ] || { echo "no results directory at $root" >&2; exit 1; }\n'
              'cd "$root"\n' % shlex.quote(paths.results_dir))

    if full:
        return common + 'tar -czf %s %s .\n' % (shlex.quote(out), excludes)

    # tar has no --include, so select the report files with find.
    names = " -o ".join("-name %s" % shlex.quote(name) for name in DEFAULT_FILES)
    return common + ('find . \\( %s \\) -type f -print0 | tar -czf %s %s --null -T -\n'
                     % (names, shlex.quote(out), excludes))


def _safe_extract(tar, members, dest):
    """Extract ``members`` under ``dest``, refusing anything that escapes it."""
    dest = Path(dest).resolve()
    safe = []
    for member in members:
        target = (dest / member.name).resolve()
        if dest == target or dest in target.parents:
            safe.append(member)
    tar.extractall(str(dest), members=safe)  # noqa: S202 - members filtered above
