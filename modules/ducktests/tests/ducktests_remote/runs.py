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
Run directory layout and state, all of it living on the runner.

Nothing about a run is kept on the coordinator.  That is what lets any coordinator
inspect, follow or stop a run that a different one started - including after the machine
that launched it has been closed and put in a bag.
"""

import json
import os
import posixpath
import re
import secrets
import shlex
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

RUN_ID_RE = re.compile(r"^[A-Za-z0-9._-]+-\d{8}-\d{6}-[0-9a-f]{4}$")

RUNNING = "running"
FINISHED = "finished"
FAILED = "failed"
STOPPED = "stopped"
UNKNOWN = "unknown"


def new_run_id(user, now=None, entropy=None):
    """
    :return: a run id of the form ``max-20260727-141233-9f2a``.

    The user and timestamp make it readable in a directory listing; the four hex
    characters keep two runs started in the same second apart.
    """
    now = now or datetime.now()
    safe_user = re.sub(r"[^A-Za-z0-9._-]", "_", str(user or "unknown")) or "unknown"
    suffix = entropy if entropy is not None else secrets.token_hex(2)
    return "%s-%s-%s" % (safe_user, now.strftime("%Y%m%d-%H%M%S"), suffix)


def is_run_id(value):
    """:return: True when ``value`` looks like a run id."""
    return bool(value) and bool(RUN_ID_RE.match(value))


@dataclass
class RunPaths:
    """Absolute, runner-side paths for one run."""

    state_root: str
    run_id: str

    @property
    def runs_root(self):
        """:return: ``<state_root>/runs``."""
        return posixpath.join(self.state_root, "runs")

    @property
    def run_dir(self):
        """:return: ``<state_root>/runs/<run-id>``."""
        return posixpath.join(self.runs_root, self.run_id)

    @property
    def latest_link(self):
        """:return: the ``latest`` symlink beside the run directories."""
        return posixpath.join(self.runs_root, "latest")

    @property
    def src_dir(self):
        """:return: where this run's sources are synced."""
        return posixpath.join(self.state_root, "src", self.run_id)

    def path(self, *parts):
        """:return: a path inside the run directory."""
        return posixpath.join(self.run_dir, *parts)

    meta = property(lambda self: self.path("meta.json"))
    cluster_file = property(lambda self: self.path("cluster.json"))
    globals_file = property(lambda self: self.path("globals.json"))
    parameters_file = property(lambda self: self.path("parameters.json"))
    run_script = property(lambda self: self.path("run.sh"))
    launch_script = property(lambda self: self.path("launch.sh"))
    pid_file = property(lambda self: self.path("pid"))
    pgid_file = property(lambda self: self.path("pgid"))
    exit_code_file = property(lambda self: self.path("exit_code"))
    stopped_file = property(lambda self: self.path("stopped"))
    log_file = property(lambda self: self.path("ducktape.log"))
    results_dir = property(lambda self: self.path("results"))


@dataclass
class RunState:
    """Everything ``status`` needs, derived from files on the runner."""

    run_id: str
    state: str = UNKNOWN
    pid: Optional[int] = None
    pgid: Optional[int] = None
    exit_code: Optional[int] = None
    meta: dict = field(default_factory=dict)

    @property
    def started_at(self):
        """:return: ISO timestamp recorded at launch, or None."""
        return self.meta.get("started_at")

    @property
    def elapsed(self):
        """:return: seconds since launch, or None when the start time is unknown."""
        started = self.meta.get("started_epoch")
        if not started:
            return None
        end = self.meta.get("finished_epoch") or time.time()
        return max(0.0, float(end) - float(started))


def derive_state(*, pid_alive, exit_code, stopped):
    """
    Turn the three observable facts into a state.

    ``exit_code`` present wins over liveness: the file is written last, so a process that
    has already exited is never reported as running just because a pid got reused.
    """
    if exit_code is not None:
        if stopped:
            return STOPPED
        return FINISHED if exit_code == 0 else FAILED
    if pid_alive:
        return RUNNING
    if stopped:
        return STOPPED
    return UNKNOWN


def read_state(transport, paths: RunPaths) -> RunState:
    """Read one run's state from the runner in a single round trip."""
    script = _STATE_SCRIPT % {"run_dir": shlex.quote(paths.run_dir)}
    result = transport.run_script(script, check=False)
    fields = _parse_kv(result.stdout)

    pid = _as_int(fields.get("pid"))
    pgid = _as_int(fields.get("pgid"))
    exit_code = _as_int(fields.get("exit_code"))
    meta = {}
    if fields.get("meta"):
        try:
            meta = json.loads(fields["meta"])
        except ValueError:
            meta = {}
    state = derive_state(pid_alive=fields.get("alive") == "1",
                         exit_code=exit_code,
                         stopped=fields.get("stopped") == "1")
    return RunState(paths.run_id, state, pid, pgid, exit_code, meta)


_STATE_SCRIPT = """
set -u
rd=%(run_dir)s
say() { printf '%%s=%%s\\n' "$1" "$2"; }
pid=""
[ -f "$rd/pid" ] && pid=$(cat "$rd/pid" 2>/dev/null || true)
say pid "$pid"
[ -f "$rd/pgid" ] && say pgid "$(cat "$rd/pgid" 2>/dev/null || true)"
[ -f "$rd/exit_code" ] && say exit_code "$(cat "$rd/exit_code" 2>/dev/null || true)"
[ -f "$rd/stopped" ] && say stopped 1
if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then say alive 1; else say alive 0; fi
if [ -f "$rd/meta.json" ]; then printf 'meta=%%s\\n' "$(tr -d '\\n' < "$rd/meta.json")"; fi
"""


def list_run_ids(transport, state_root) -> List[str]:
    """:return: run ids present on the runner, newest first."""
    runs_root = posixpath.join(state_root, "runs")
    result = transport.run(["ls", "-1", runs_root], check=False)
    if not result.ok:
        return []
    ids = [line.strip() for line in result.stdout.splitlines() if is_run_id(line.strip())]
    return sorted(ids, reverse=True)


def resolve_run_id(transport, state_root, run_id=None):
    """:return: ``run_id`` when given, otherwise the most recent run on the runner."""
    if run_id:
        return run_id
    ids = list_run_ids(transport, state_root)
    if not ids:
        return None
    return ids[0]


def write_meta(transport, paths: RunPaths, meta):
    """Write ``meta.json`` into the run directory."""
    transport.write_file(json.dumps(meta, indent=2, sort_keys=True) + "\n", paths.meta)


def utc_now_iso():
    """:return: the current time as an ISO-8601 UTC string."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def format_duration(seconds):
    """:return: ``1h 04m 12s`` style duration, or ``-`` for None."""
    if seconds is None:
        return "-"
    seconds = int(seconds)
    hours, rest = divmod(seconds, 3600)
    minutes, secs = divmod(rest, 60)
    if hours:
        return "%dh %02dm %02ds" % (hours, minutes, secs)
    if minutes:
        return "%dm %02ds" % (minutes, secs)
    return "%ds" % secs


def _parse_kv(text):
    fields = {}
    for line in (text or "").split("\n"):
        if "=" in line:
            key, value = line.split("=", 1)
            fields[key.strip()] = value.strip()
    return fields


def _as_int(value):
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def default_state_root(state_root):
    """:return: the state root with ``~`` left alone (it is expanded on the runner)."""
    return state_root or "~/.ducktests-remote"


def local_fetch_dir(base, run_id):
    """:return: the coordinator-side directory ``fetch`` writes into."""
    return os.path.join(str(base), run_id)


TEMPLATE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")


def render_template(name, mapping):
    """Substitute ``{{key}}`` placeholders in a packaged template."""
    with open(os.path.join(TEMPLATE_DIR, name), "r", encoding="utf-8") as handle:
        text = handle.read()
    for key, value in mapping.items():
        text = text.replace("{{%s}}" % key, str(value))
    left = re.search(r"\{\{(\w+)\}\}", text)
    if left:
        raise KeyError("template %s has an unfilled placeholder %r" % (name, left.group(1)))
    return text


def render_run_script(*, version, timestamp, author, work_dir, results_root, cluster_file,
                      globals_file, test_paths, venv=None, parameters_file=None,
                      repeat=None, max_parallel=None, test_runner_timeout=None,
                      extra_args=()):
    """
    Render the ``run.sh`` that the runner executes.

    ducktape 0.13 accepts a *file path* for ``--globals`` (``command_line/main.py``
    checks ``os.path.isfile`` before parsing the argument as JSON), so the composed
    blob is referenced by path and never crosses a shell command line.

    Every interpolated value is shell-quoted, which is why paths with spaces and JSON
    with braces survive the round trip.
    """
    extra = []
    if parameters_file:
        extra.append("--parameters %s" % shlex.quote(str(parameters_file)))
    if repeat:
        extra.append("--repeat %d" % int(repeat))
    if max_parallel:
        extra.append("--max-parallel %d" % int(max_parallel))
    if test_runner_timeout:
        extra.append("--test-runner-timeout %d" % int(test_runner_timeout))
    for arg in extra_args:
        extra.append(shlex.quote(str(arg)))

    extra_lines = "".join(" \\\n  %s" % item for item in extra)

    if venv:
        activate = shlex.quote(posixpath.join(str(venv), "bin", "activate"))
        venv_activate = ("# activate the runner venv; `set +u` because older activate\n"
                         "# scripts read unset variables\nset +u\n"
                         ". %s\nset -u\n" % activate)
    else:
        venv_activate = "# no venv configured: ducktape is expected on PATH\n"

    return render_template("run.sh.tmpl", {
        "version": version,
        "timestamp": timestamp,
        "author": author,
        "work_dir": shlex.quote(str(work_dir)),
        "venv_activate": venv_activate,
        "results_root": shlex.quote(str(results_root)),
        "cluster_file": shlex.quote(str(cluster_file)),
        "globals_file": shlex.quote(str(globals_file)),
        "extra_lines": extra_lines,
        "test_paths": " ".join(shlex.quote(str(p)) for p in test_paths),
    })


def render_launch_script(paths: RunPaths):
    """
    Render the wrapper that detaches the run and records its exit code.

    The run is detached from second zero rather than "detached later": the failure worth
    surviving is the coordinator going away, and a run that is only detachable after the
    fact does not survive it.  ``setsid`` puts ducktape in its own session so a dropped
    SSH connection cannot SIGHUP it and leave Ignite JVMs alive on every worker; where
    ``setsid`` is missing we fall back to ``nohup`` plus ``disown``.
    """
    run_dir = shlex.quote(paths.run_dir)
    return """#!/usr/bin/env bash
# Generated by ducktests-remote. Waits on run.sh and records its exit code.
rd=%s
ps -o pgid= -p $$ 2>/dev/null | tr -d ' ' > "$rd/pgid" || true
bash "$rd/run.sh" >> "$rd/ducktape.log" 2>&1 < /dev/null
echo $? > "$rd/exit_code"
""" % run_dir


def render_detach_script(paths: RunPaths):
    """Render the snippet that starts ``launch.sh`` detached and records its pid."""
    run_dir = shlex.quote(paths.run_dir)
    return """set -eu
rd=%s
cd "$rd"
: > "$rd/ducktape.log"
if command -v setsid >/dev/null 2>&1; then
  setsid nohup bash "$rd/launch.sh" > /dev/null 2>&1 < /dev/null &
else
  nohup bash "$rd/launch.sh" > /dev/null 2>&1 < /dev/null &
  disown 2>/dev/null || true
fi
echo $! > "$rd/pid"
cat "$rd/pid"
""" % run_dir
