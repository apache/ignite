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
``doctor`` - preflight, run standalone or implicitly before ``run``.

Every check is driven by the inventory, so a two-node cluster produces the same output
shape as a forty-nine node one.  The command never stops at the first failure: an
operator who has to ask an administrator for access needs the complete list in one go.
"""

import json
import posixpath
import re
import shlex
import shutil
import time

from ducktests_remote import sshdiag
from ducktests_remote.cli import EXIT_OK, EXIT_PREFLIGHT
from ducktests_remote.config import (REQUIREMENTS_RELPATH, SUDO_DEPENDENT_TESTS,
                                     expand_path)
from ducktests_remote.fanout import HostResult, fanout
from ducktests_remote.transport import TransportError, run_local

OK = "OK"
WARN = "WARN"
FAIL = "FAIL"

DISK_WARN_GB = 10
CLOCK_WARN_SEC = 5
CLOCK_FAIL_SEC = 60


class Check:
    """One preflight observation."""

    def __init__(self, scope, host, name, status, message=""):
        self.scope = scope
        self.host = host
        self.name = name
        self.status = status
        self.message = message

    def as_dict(self):
        """:return: a JSON-serialisable view."""
        return {"scope": self.scope, "host": self.host, "name": self.name,
                "status": self.status, "message": self.message}


def register(subparsers, common):
    """Wire up the ``doctor`` subcommand."""
    parser = subparsers.add_parser(
        "doctor", parents=[common],
        help="check the coordinator, the runner and every worker",
        description="Check that this cluster can actually run ducktests, and when it "
                    "cannot, say precisely what is missing and who has to grant it.")
    parser.add_argument("--json", action="store_true", help="machine-readable output")
    parser.add_argument("-n", "--num-nodes", type=int, default=None,
                        help="only check the first N inventory hosts")
    parser.set_defaults(handler=execute)


def execute(ctx):
    """Run every check and print the report. :return: the process exit code."""
    checks, diagnoses = run_checks(ctx)

    if getattr(ctx.args, "json", False):
        ctx.console.out(json.dumps({
            "checks": [c.as_dict() for c in checks],
            "ssh": [{"host": d.host, "classification": d.classification,
                     "advice": d.advice} for d in diagnoses],
            "ok": not has_failures(checks),
        }, indent=2))
    else:
        print_report(ctx, checks, diagnoses)

    return EXIT_PREFLIGHT if has_failures(checks) else EXIT_OK


def print_report(ctx, checks, diagnoses):
    """Print the human-readable doctor report."""
    console = ctx.console
    width_host = max([len(c.host) for c in checks] + [4])
    width_name = max([len(c.name) for c in checks] + [5])

    last_scope = None
    for check in checks:
        if check.scope != last_scope:
            console.heading(check.scope.upper())
            last_scope = check.scope
        style = {"OK": "green", "WARN": "yellow", "FAIL": "red"}[check.status]
        console.out("  %s  %s  %s  %s" % (
            console.paint(check.status.ljust(4), style),
            check.host.ljust(width_host),
            check.name.ljust(width_name),
            check.message))

    counts = {}
    for check in checks:
        counts[check.status] = counts.get(check.status, 0) + 1
    console.out("")
    console.out("%d checks: %s" % (
        len(checks),
        ", ".join("%d %s" % (counts[s], s) for s in (OK, WARN, FAIL) if s in counts)))

    block = sshdiag.admin_request_block(
        diagnoses, user=ctx.cluster_cfg.get("user"),
        identity_file=expand_path(ctx.identity_file))
    if block:
        console.out(block)


def has_failures(checks):
    """:return: True when any check failed."""
    return any(c.status == FAIL for c in checks)


def run_checks(ctx, versions=None):
    """
    Run every preflight check.

    :param versions: ``globals.ignite_versions`` when known, used for the install-root check.
    :return: ``(checks, ssh_diagnoses)``.
    """
    checks = []
    checks += coordinator_checks(ctx)
    checks += runner_checks(ctx)

    nodes = ctx.all_nodes
    if not nodes:
        checks.append(Check("workers", "-", "inventory", FAIL,
                            "cluster.nodes is empty; nothing to check"))
        return checks, []

    diagnoses = probe_workers(ctx, nodes)
    by_host = {d.host: d for d in diagnoses}
    for node in nodes:
        diagnosis = by_host.get(node.host)
        if diagnosis and not diagnosis.ok:
            checks.append(Check("workers", node.host, "ssh", FAIL,
                                "%s: %s" % (diagnosis.classification, diagnosis.advice)))
        else:
            checks.append(Check("workers", node.host, "ssh", OK, "reachable"))

    reachable = [n for n in nodes if by_host.get(n.host) and by_host[n.host].ok]
    if reachable:
        checks += worker_checks(ctx, reachable, versions or _configured_versions(ctx))
        checks += resolution_checks(ctx, reachable)
    checks += runner_to_worker_checks(ctx, reachable)
    return checks, diagnoses


# -- coordinator ------------------------------------------------------------------


def coordinator_checks(ctx):
    """Checks about the machine the CLI is running on."""
    if ctx.runner_host != "local":
        checks = []
        for tool in ("ssh", "scp"):
            found = _which(tool)
            checks.append(Check("coordinator", "local", tool,
                                OK if found else FAIL,
                                "on PATH" if found else "not on PATH; install openssh-client"))
        return checks

    if ctx.dry_run:
        return [Check("coordinator", "local", "ducktape", OK, "not probed (--dry-run)")]

    importable, version = _local_ducktape(_venv_python(ctx))
    if not importable:
        return [Check("coordinator", "local", "ducktape", FAIL,
                      "--runner local but ducktape is not importable here")]
    pinned = pinned_ducktape_version(ctx)
    status = OK if (pinned is None or version == pinned) else WARN
    return [Check("coordinator", "local", "ducktape", status,
                  "%s (pinned %s)" % (version, pinned or "unknown"))]


def _which(tool):
    return shutil.which(tool) is not None


def _local_ducktape(python):
    """
    :return: ``(importable, version)`` for a locally installed ducktape.

    Done in a subprocess on purpose: ``ducktests_remote`` must never import ducktape, so
    the boundary holds even for this check.
    """
    result = run_local([python, "-c",
                        "import ducktape,sys;sys.stdout.write(ducktape.__version__)"])
    return result.ok, result.out


# -- runner -----------------------------------------------------------------------


def runner_checks(ctx):
    """Checks about the host that will run the ducktape process."""
    host = ctx.runner_host
    checks = []
    try:
        # `exit 0` at the end: a missing setsid is a WARN, not a reason for the whole
        # probe to report the runner as unreachable.
        probe = ctx.runner.run_script("for t in bash python3 setsid; do\n"
                                      "  command -v \"$t\" >/dev/null 2>&1 && echo \"$t\"\n"
                                      "done\nexit 0\n",
                                      check=False)
    except TransportError as ex:
        diagnosis = sshdiag.diagnose_exception(ex, host=host,
                                               user=ctx.cluster_cfg.get("user"),
                                               port=ctx.cluster_cfg.get("port", 22))
        return [Check("runner", host, "ssh", FAIL,
                      "%s: %s" % (diagnosis.classification, diagnosis.advice))]

    if not probe.ok and not ctx.dry_run:
        detail = (probe.stderr.strip() or probe.stdout.strip()
                  or "exit %d with no output" % probe.returncode)
        return [Check("runner", host, "reachable", FAIL, detail[:160])]

    present = set(probe.stdout.split())
    for tool in ("bash", "python3"):
        checks.append(Check("runner", host, tool, OK if tool in present or ctx.dry_run else FAIL,
                            "present" if tool in present or ctx.dry_run else "missing"))
    checks.append(Check("runner", host, "setsid",
                        OK if "setsid" in present or ctx.dry_run else WARN,
                        "present" if "setsid" in present or ctx.dry_run
                        else "missing; falling back to nohup + disown"))

    checks.append(_runner_ducktape_check(ctx))
    checks.append(_identity_check(ctx))
    checks.append(_runner_disk_check(ctx))
    return checks


def _runner_disk_check(ctx):
    path = ctx.runner.expand(ctx.state_root)
    result = ctx.runner.run_script(
        "set -u\nmkdir -p %s 2>/dev/null || true\n"
        "df -Pk %s 2>/dev/null | awk 'NR==2{printf \"%%d\", $4/1048576}'\n"
        % (shlex.quote(path), shlex.quote(path)), check=False)
    if ctx.dry_run:
        return Check("runner", ctx.runner_host, "disk", OK, "not probed (--dry-run)")
    free = _as_int(result.out)
    if free is None:
        return Check("runner", ctx.runner_host, "disk", WARN,
                     "could not read free space on %s" % path)
    return Check("runner", ctx.runner_host, "disk",
                 WARN if free < DISK_WARN_GB else OK, "%d GB free on %s" % (free, path))


def _runner_ducktape_check(ctx):
    host = ctx.runner_host
    pinned = pinned_ducktape_version(ctx)
    python = _venv_python(ctx)
    result = ctx.runner.run([python, "-c",
                             "import ducktape,sys;sys.stdout.write(ducktape.__version__)"],
                            check=False)
    if ctx.dry_run:
        return Check("runner", host, "ducktape", OK, "not probed (--dry-run)")
    if not result.ok:
        return Check("runner", host, "ducktape", FAIL,
                     "not importable via %s; run `ducktests-remote provision --only python`"
                     % python)
    if pinned and result.out != pinned:
        return Check("runner", host, "ducktape", WARN,
                     "%s installed, %s pinned in %s" % (result.out, pinned, REQUIREMENTS_RELPATH))
    return Check("runner", host, "ducktape", OK, result.out)


def _identity_check(ctx):
    host = ctx.runner_host
    identity = ctx.identity_file
    if not identity:
        return Check("runner", host, "identity", WARN,
                     "cluster.identity_file is unset; ducktape will rely on the runner's "
                     "ssh agent, which a detached run cannot keep")
    remote = ctx.runner.expand(identity)
    result = ctx.runner.run_script(
        "set -e\nf=%s\n[ -f \"$f\" ] || { echo missing; exit 0; }\n"
        "printf '%%s\\n' \"$(ls -l \"$f\" | cut -c1-10)\"\n" % shlex.quote(remote),
        check=False)
    if ctx.dry_run:
        return Check("runner", host, "identity", OK, "not probed (--dry-run)")
    text = result.out
    if not result.ok or text == "missing":
        return Check("runner", host, "identity", FAIL,
                     "%s does not exist on the runner. identity_file is a RUNNER-side path, "
                     "not a coordinator-side one; `ducktests-remote keys push` installs it."
                     % remote)
    if len(text) >= 10 and text[4:10] != "------":
        return Check("runner", host, "identity", WARN,
                     "%s is %s; ssh wants 0600 or stricter" % (remote, text))
    return Check("runner", host, "identity", OK, remote)


def _venv_python(ctx):
    venv = ctx.config["runner"].get("venv")
    if venv:
        return posixpath.join(ctx.runner.expand(venv), "bin", "python3")
    return ctx.config["runner"].get("python", "python3")


def pinned_ducktape_version(ctx):
    """:return: the ducktape version pinned in docker/requirements.txt, or None."""
    source_root = ctx.config["run"].get("source_root") or "."
    path = expand_path(posixpath.join(str(source_root).replace("\\", "/"), REQUIREMENTS_RELPATH))
    try:
        with open(path, "r", encoding="utf-8") as handle:
            text = handle.read()
    except OSError:
        return None
    match = re.search(r"^ducktape==([^\s#]+)", text, re.MULTILINE)
    return match.group(1) if match else None


# -- workers ----------------------------------------------------------------------


def probe_workers(ctx, nodes):
    """
    Probe every worker from the coordinator, classifying each failure.

    This is one parallel pass over the whole inventory; nothing short-circuits, because a
    partial list of broken hosts cannot be turned into a single request to an
    administrator.
    """
    def probe(node):
        transport = ctx.worker(node)
        try:
            result = transport.run(["true"], check=False)
            diagnosis = sshdiag.diagnose(result, host=node.host, user=node.user, port=node.port)
        except TransportError as ex:
            diagnosis = sshdiag.diagnose_exception(ex, host=node.host, user=node.user,
                                                   port=node.port)
        return HostResult(node.host, "ok" if diagnosis.ok else "failed",
                          diagnosis.classification, data=diagnosis)

    results = fanout(nodes, probe, jobs=ctx.jobs)
    return [r.data for r in results if r.data is not None]


def worker_checks(ctx, nodes, versions):
    """Substantive per-worker checks: java, disk, clock, stale JVMs, install dirs, sudo."""
    if ctx.dry_run:
        return [Check("workers", node.host, "probe", OK,
                      "java, disk, clock, stale JVMs, %s (not probed: --dry-run)"
                      % ("install dirs for " + ", ".join(versions) if versions
                         else "install dirs"))
                for node in nodes]

    reference = time.time()
    install_root = ctx.cluster_cfg.get("install_root", "/opt")
    persistent = (ctx.config["clean"]["paths"] or ["/mnt/service"])[0]
    pattern = ctx.config["clean"]["process_pattern"]

    script = _WORKER_SCRIPT % {
        "install_root": shlex.quote(install_root),
        "persistent": shlex.quote(persistent),
        "pattern": shlex.quote(pattern),
    }

    def probe(node):
        result = ctx.worker(node).run_script(script, check=False)
        return HostResult(node.host, "ok" if result.ok else "failed",
                          "", detail=result.stderr, data=_parse_kv(result.stdout))

    results = fanout(nodes, probe, jobs=ctx.jobs)
    facts = {r.host: (r.data or {}) for r in results}

    checks = []
    checks += _java_checks(facts)
    for host in sorted(facts):
        fact = facts[host]
        checks.append(_disk_from_facts(host, "install_free_gb", install_root, fact))
        checks.append(_disk_from_facts(host, "work_free_gb", persistent, fact))
        checks.append(_clock_check(host, fact, reference))
        checks.append(_stale_jvm_check(host, fact, pattern))
        checks += _privilege_checks(host, fact, ctx, install_root, persistent)
        checks += _install_dir_checks(host, fact, install_root, versions)
    return [c for c in checks if c is not None]


_WORKER_SCRIPT = """
set -u
say() { printf '%%s=%%s\\n' "$1" "$2"; }
say epoch "$(date +%%s)"
say whoami "$(id -un 2>/dev/null || echo '?')"
say java "$(java -version 2>&1 | head -n1 | tr -d '\\r' || echo missing)"
say install_free_gb "$(df -Pk %(install_root)s 2>/dev/null | awk 'NR==2{printf "%%d", $4/1048576}')"
say install_writable "$([ -w %(install_root)s ] && echo 1 || echo 0)"
if [ -d %(persistent)s ]; then
  say work_free_gb "$(df -Pk %(persistent)s 2>/dev/null | awk 'NR==2{printf "%%d", $4/1048576}')"
  say work_writable "$([ -w %(persistent)s ] && echo 1 || echo 0)"
else
  parent=$(dirname %(persistent)s)
  say work_free_gb "$(df -Pk "$parent" 2>/dev/null | awk 'NR==2{printf "%%d", $4/1048576}')"
  say work_writable "$([ -w "$parent" ] && echo 1 || echo 0)"
fi
say stale "$(pgrep -f %(pattern)s 2>/dev/null | wc -l | tr -d ' ')"
if sudo -n true 2>/dev/null; then say sudo 1; else say sudo 0; fi
say install_dirs "$(ls -1 %(install_root)s 2>/dev/null | tr '\\n' ',' )"
"""


def _java_checks(facts):
    checks = []
    versions = {}
    for host, fact in facts.items():
        java = fact.get("java", "missing")
        if not java or "missing" in java or "not found" in java:
            checks.append(Check("workers", host, "java", FAIL,
                                "java not on the non-interactive PATH; see `provision --only "
                                "ssh-env`"))
            continue
        versions.setdefault(java, []).append(host)
        checks.append(Check("workers", host, "java", OK, java))
    if len(versions) > 1:
        majority = max(versions, key=lambda k: len(versions[k]))
        outliers = [h for k, hosts in versions.items() if k != majority for h in hosts]
        checks.append(Check("workers", "-", "java-consistency", WARN,
                            "mixed JDKs; majority %r, outliers: %s"
                            % (majority, ", ".join(sorted(outliers)))))
    return checks


def _disk_from_facts(host, key, path, fact=None):
    if fact is None:
        return None
    free = _as_int(fact.get(key))
    if free is None:
        return Check("workers", host, "disk", WARN, "could not read free space on %s" % path)
    status = WARN if free < DISK_WARN_GB else OK
    return Check("workers", host, "disk", status, "%d GB free on %s" % (free, path))


def _clock_check(host, fact, reference):
    epoch = _as_int(fact.get("epoch"))
    if epoch is None:
        return Check("workers", host, "clock", WARN, "could not read the clock")
    skew = abs(epoch - reference)
    if skew > CLOCK_FAIL_SEC:
        return Check("workers", host, "clock", FAIL, "%.0fs skew; enable NTP" % skew)
    if skew > CLOCK_WARN_SEC:
        return Check("workers", host, "clock", WARN, "%.0fs skew" % skew)
    return Check("workers", host, "clock", OK, "%.0fs skew" % skew)


def _stale_jvm_check(host, fact, pattern):
    count = _as_int(fact.get("stale")) or 0
    if count:
        return Check("workers", host, "stale-jvm", FAIL,
                     "%d process(es) matching %r are still running from an earlier run; "
                     "run `ducktests-remote clean --dry-run` then `clean`" % (count, pattern))
    return Check("workers", host, "stale-jvm", OK, "none")


def _privilege_checks(host, fact, ctx, install_root, persistent):
    checks = []
    user = fact.get("whoami") or ctx.cluster_cfg.get("user")
    checks.append(Check("workers", host, "account", OK, "connected as %s" % user))
    checks.append(Check("workers", host, "write-install",
                        OK if fact.get("install_writable") == "1" else WARN,
                        "%s %s" % (install_root,
                                   "writable" if fact.get("install_writable") == "1"
                                   else "NOT writable; deploy needs --sudo here")))
    checks.append(Check("workers", host, "write-work",
                        OK if fact.get("work_writable") == "1" else FAIL,
                        "%s %s" % (persistent,
                                   "writable" if fact.get("work_writable") == "1"
                                   else "NOT writable; tests cannot create their work dirs")))
    if fact.get("sudo") == "1":
        checks.append(Check("workers", host, "sudo", OK, "passwordless"))
    else:
        checks.append(Check("workers", host, "sudo", WARN,
                            "no passwordless sudo; only the network-segmentation suites need "
                            "it (%s)" % ", ".join(SUDO_DEPENDENT_TESTS)))
    return checks


def _install_dir_checks(host, fact, install_root, versions):
    """
    Report the distributions found under the install root.

    ``ignitetest`` resolves a home directory as ``install_root/str(IgniteVersion(v))``
    (services/utils/path.py ``get_home_dir`` with ``IgniteAwareService.product``), and
    ``IgniteVersion.__str__`` normalises the string, so ``ise--6`` maps to ``ise-6``.
    Because a fork can override ``product``, a missing directory is reported as a WARN
    listing what *was* found rather than as a hard failure on a guessed mapping.
    """
    found = [d for d in (fact.get("install_dirs") or "").split(",") if d]
    if not versions:
        return [Check("workers", host, "install-root", OK,
                      "%d distribution(s) under %s" % (len(found), install_root))]
    missing = [v for v in versions if v not in found]
    if missing:
        return [Check("workers", host, "install-root", WARN,
                      "no directory for %s under %s; found: %s"
                      % (", ".join(missing), install_root, ", ".join(sorted(found)) or "(none)"))]
    return [Check("workers", host, "install-root", OK,
                  "found %s" % ", ".join(versions))]


def resolution_checks(ctx, nodes):
    """
    Verify node-to-node name resolution from a single probe host.

    The Docker flow writes every node into ``/etc/hosts`` explicitly.  On real VMs name
    resolution has to already work worker-to-worker, and when it does not, tests fail
    deep inside discovery with no message that points here.
    """
    if len(nodes) < 2:
        return []
    probe_node = nodes[0]
    targets = [n.host for n in nodes if n.host != probe_node.host]
    script = "for h in %s; do\n  if getent hosts \"$h\" >/dev/null 2>&1; then :; " \
             "else echo \"$h\"; fi\ndone\n" % " ".join(shlex.quote(t) for t in targets)
    result = ctx.worker(probe_node).run_script(script, check=False)
    if ctx.dry_run:
        return [Check("workers", probe_node.host, "dns", OK, "not probed (--dry-run)")]
    if not result.ok:
        return [Check("workers", probe_node.host, "dns", WARN,
                      "could not run the resolution probe")]
    unresolved = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    if unresolved:
        return [Check("workers", probe_node.host, "dns", WARN,
                      "cannot resolve %d peer(s) from this host: %s. Run `provision "
                      "--write-hosts` or fix cluster DNS."
                      % (len(unresolved), ", ".join(unresolved[:8])))]
    return [Check("workers", probe_node.host, "dns", OK,
                  "resolves all %d peers" % len(targets))]


def runner_to_worker_checks(ctx, nodes):
    """
    Verify the connection ducktape will actually make: runner to worker, with the
    runner-side identity file.  A coordinator that can reach a worker proves nothing
    about the runner being able to.
    """
    if ctx.runner_host == "local" or not nodes:
        return []
    identity = ctx.runner.expand(ctx.identity_file) if ctx.identity_file else None
    opts = "-o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10"
    if identity:
        opts += " -o IdentitiesOnly=yes -i %s" % shlex.quote(identity)
    lines = []
    for node in nodes:
        port = "" if int(node.port) == 22 else " -p %d" % int(node.port)
        lines.append("if ssh %s%s %s true >/dev/null 2>&1; then echo \"%s=1\"; "
                     "else echo \"%s=0\"; fi"
                     % (opts, port, shlex.quote(node.target), node.host, node.host))
    result = ctx.runner.run_script("\n".join(lines) + "\n", check=False)
    if ctx.dry_run:
        return [Check("runner->workers", ctx.runner_host, "ssh", OK, "not probed (--dry-run)")]
    facts = _parse_kv(result.stdout)
    checks = []
    for node in nodes:
        reachable = facts.get(node.host) == "1"
        checks.append(Check("runner->workers", node.host, "ssh",
                            OK if reachable else FAIL,
                            "reachable from %s" % ctx.runner_host if reachable else
                            "the runner cannot ssh to %s as %s with %s. This is the connection "
                            "ducktape makes; fix it with `ducktests-remote keys push`."
                            % (node.host, node.user, identity or "the default identity")))
    return checks


def _configured_versions(ctx):
    versions = ctx.config.get("globals", {}).get("ignite_versions")
    if isinstance(versions, str):
        return [versions]
    return list(versions or [])


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
