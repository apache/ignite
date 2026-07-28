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
``provision`` - bring an unconfigured VM up to the state the Docker image guarantees.

``modules/ducktests/tests/docker/Dockerfile`` is the source of truth for what that state
is; the package list lives in :data:`ducktests_remote.config.DOCKERFILE_PACKAGES` next to
a note saying so.  This is not configuration management and must not grow into it: it
covers the specific package / directory / ssh-environment set the image installs, each
step idempotent and independently selectable.
"""

import hashlib
import json
import posixpath
import shlex
import tempfile
import uuid
from pathlib import Path

from ducktests_remote import java
from ducktests_remote.cli import EXIT_OK, EXIT_PREFLIGHT, EXIT_TRANSPORT
from ducktests_remote.commands import deploy, doctor
from ducktests_remote.config import ConfigError
from ducktests_remote.fanout import (CHANGED, FAILED, HostResult, OK, SKIPPED, any_failed,
                                     fanout, render_table, summarise)
from ducktests_remote.transport import make_tarball

STEPS = ("packages", "jdk", "python", "user", "ssh-env", "dirs", "hosts")

HOSTS_BEGIN = "# BEGIN ducktests-remote"
HOSTS_END = "# END ducktests-remote"

JAVA_MANIFEST_NAME = ".ducktests-java.json"


def register(subparsers, common):
    """Wire up the ``provision`` subcommand."""
    parser = subparsers.add_parser(
        "provision", parents=[common],
        help="bring the workers up to the state the Docker image guarantees",
        description="Idempotent, per-step preparation of the worker hosts. Run it with "
                    "--dry-run first: it prints the exact commands per host and changes "
                    "nothing. Steps: " + ", ".join(STEPS))
    parser.add_argument("--only", action="append", default=[], choices=STEPS, metavar="STEP",
                        help="run only these steps; repeatable")
    parser.add_argument("--skip", action="append", default=[], choices=STEPS, metavar="STEP",
                        help="skip these steps; repeatable")
    parser.add_argument("--sudo", action="store_true",
                        help="allow steps that need root to use `sudo -n`")
    parser.add_argument("--install-jdk", action="store_true",
                        help="let the jdk step fall back to the distribution's own JDK "
                             "package when nothing else resolves; needs --sudo")
    parser.add_argument("--java-home", metavar="PATH",
                        help="use exactly this JDK home on the workers (sets java.home)")
    parser.add_argument("--java-major", type=int, default=None, metavar="N",
                        help="Java major version the tests need (sets java.major)")
    parser.add_argument("--java-archive", metavar="PATH",
                        help="coordinator-side JDK tarball or directory to deliver to "
                             "hosts that have no matching JDK (sets java.archive)")
    parser.add_argument("--force", action="store_true",
                        help="deliver the JDK again even when the host already has it")
    parser.add_argument("--create-user", metavar="NAME",
                        help="create this account (step `user`); needs --sudo")
    parser.add_argument("--authorize-key", metavar="PATH",
                        help="public key appended to the created account's authorized_keys")
    parser.add_argument("--write-hosts", action="store_true",
                        help="write the inventory into /etc/hosts (step `hosts`); needs --sudo")
    parser.add_argument("-n", "--num-nodes", type=int, default=None,
                        help="only provision the first N inventory hosts")
    parser.add_argument("--json", action="store_true", help="machine-readable output")
    parser.set_defaults(handler=execute)


def execute(ctx):
    """Provision the workers. :return: the process exit code."""
    nodes = ctx.all_nodes
    if not nodes:
        raise ConfigError("cluster.nodes is empty; there is nothing to provision")

    selected = _selected_steps(ctx.args)
    ctx.console.info("steps: %s" % ", ".join(selected))
    if ctx.dry_run:
        ctx.console.info("--dry-run: printing commands only")

    all_results = {}
    skipped_for_sudo = []
    for step in selected:
        ctx.console.heading("STEP %s" % step)
        if step in _PYTHON_DRIVEN:
            # These two resolve a JDK per host before they can act, and `jdk` may have to
            # upload one, so they are not expressible as a single canned script.
            results = _PYTHON_DRIVEN[step](ctx, nodes)
        else:
            script, needs_sudo = _step_script(ctx, step, nodes)
            if script is None:
                ctx.console.info("nothing to do for this step")
                continue
            if needs_sudo and not ctx.args.sudo:
                ctx.console.warn("step %r needs root; rerun with --sudo. Skipping it and "
                                 "continuing with the rest." % step)
                skipped_for_sudo.append(step)
                continue
            results = _run_step(ctx, nodes, step, script)
        all_results[step] = results
        ctx.console.out(render_table(results, verbose=ctx.console.verbose))
        ctx.console.out(summarise(results))

    if ctx.args.json:
        ctx.console.out(json.dumps(
            {step: [{"host": r.host, "status": r.status, "message": r.message} for r in res]
             for step, res in all_results.items()}, indent=2))

    if skipped_for_sudo:
        ctx.console.warn("skipped for lack of --sudo: %s" % ", ".join(skipped_for_sudo))

    failed = any(any_failed(res) for res in all_results.values())

    if not ctx.dry_run:
        # Always finish with evidence rather than an assumption.
        ctx.console.heading("VERIFYING")
        checks, diagnoses = doctor.run_checks(ctx)
        doctor.print_report(ctx, checks, diagnoses)
        if doctor.has_failures(checks):
            return EXIT_PREFLIGHT

    return EXIT_TRANSPORT if failed else EXIT_OK


def _selected_steps(args):
    steps = list(args.only) if args.only else list(STEPS)
    steps = [s for s in steps if s not in (args.skip or [])]
    if "user" in steps and not args.create_user and not args.only:
        # Most operators use their own existing account; creating one is an escape hatch.
        steps.remove("user")
    if "hosts" in steps and not args.write_hosts and not args.only:
        steps.remove("hosts")
    return steps


def _run_step(ctx, nodes, step, script):
    def operation(node):
        transport = ctx.worker(node)
        if ctx.dry_run:
            ctx.console.out("[dry-run] %s: step %s" % (node.host, step))
            ctx.console.detail(script)
            return HostResult(node.host, SKIPPED, "dry-run")
        result = transport.run_script(script, check=False)
        text = result.stdout.strip()
        if not result.ok:
            return HostResult(node.host, FAILED, _summary_line(result), detail=text or
                              result.stderr.strip())
        status = CHANGED if "CHANGED" in text else OK
        return HostResult(node.host, status, _summary_line(result), detail=text)

    return fanout(nodes, operation, jobs=ctx.jobs,
                  fail_fast=getattr(ctx.args, "fail_fast", False))


def _summary_line(result):
    lines = [ln for ln in (result.stdout or "").splitlines() if ln.strip()]
    if lines:
        return lines[-1][:100]
    return (result.stderr or "").strip().splitlines()[-1][:100] if result.stderr else ""


def _step_script(ctx, step, nodes):
    """:return: ``(script, needs_sudo)`` for one step, or ``(None, False)`` when inert."""
    if step == "packages":
        return _packages_script(ctx), True
    if step == "python":
        return _python_script(ctx), False
    if step == "user":
        if not ctx.args.create_user:
            return None, False
        return _user_script(ctx), True
    if step == "dirs":
        return _dirs_script(ctx), True
    if step == "hosts":
        if not ctx.args.write_hosts:
            return None, False
        return _hosts_script(ctx, nodes), True
    raise ConfigError("unknown provision step %r" % step)


def _packages_script(ctx):
    packages = ctx.config["provision"]["packages"]
    return """set -u
missing=""
for p in %(pkgs)s; do
  if command -v dpkg-query >/dev/null 2>&1; then
    dpkg-query -W -f='${Status}' "$p" 2>/dev/null | grep -q "install ok installed" \\
      || missing="$missing $p"
  elif command -v rpm >/dev/null 2>&1; then
    rpm -q "$p" >/dev/null 2>&1 || missing="$missing $p"
  else
    echo "unsupported package manager: neither dpkg nor rpm found" >&2
    exit 2
  fi
done
if [ -z "$missing" ]; then echo "all %(count)d packages present"; exit 0; fi
echo "CHANGED installing:$missing"
if command -v apt-get >/dev/null 2>&1; then
  sudo -n env DEBIAN_FRONTEND=noninteractive apt-get update -qq
  sudo -n env DEBIAN_FRONTEND=noninteractive apt-get install -y -qq $missing
elif command -v dnf >/dev/null 2>&1; then
  sudo -n dnf install -y -q $missing
elif command -v yum >/dev/null 2>&1; then
  sudo -n yum install -y -q $missing
else
  echo "unsupported package manager: no apt-get, dnf or yum" >&2
  exit 2
fi
echo "CHANGED installed:$missing"
""" % {"pkgs": " ".join(shlex.quote(p) for p in packages), "count": len(packages)}


def _run_jdk_step(ctx, nodes):
    """
    Put a JDK of the requested major on every worker, and say which one it is.

    Rungs 1-3 of the ladder are discovery and cost one round trip per host; only the hosts
    that come back empty reach rung 4 (deliver ``java.archive``) or rung 5 (fail with the
    list of JDKs that *were* found).  Nothing is uploaded to a host that already has what
    it needs.
    """
    cfg = java.config_of(ctx)
    plan = java.archive_plan(cfg.archive, cfg.name) if cfg.archive else None
    if plan:
        target = java.target_dir(cfg, plan)
        ctx.console.info("java: %s (%s) available for delivery to %s"
                         % (plan.path, deploy.human(plan.bytes), target))
        if len(nodes) > 3 and plan.bytes > 200 * 1024 * 1024:
            ctx.console.warn("that is up to %s over the wire from this machine, if every "
                             "host turns out to need it."
                             % deploy.human(plan.bytes * len(nodes)))

    script = java.discovery_script(cfg)

    def operation(node):
        if ctx.dry_run:
            ctx.console.out("[dry-run] %s: probe for a Java %s JDK"
                            % (node.host, cfg.major or "any"))
            ctx.console.detail(script)
            return HostResult(node.host, SKIPPED, "dry-run")
        return _jdk_on_host(ctx, node, cfg, plan, script)

    return fanout(nodes, operation, jobs=ctx.jobs,
                  fail_fast=getattr(ctx.args, "fail_fast", False))


def _jdk_on_host(ctx, node, cfg, plan, script):
    transport = ctx.worker(node)
    probe = transport.run_script(script, check=False)
    if not probe.ok:
        return HostResult(node.host, FAILED, "could not probe for a JDK",
                          detail=probe.stderr.strip())

    res = java.parse_probe(node.host, probe.stdout, cfg)
    if res.selected:
        return HostResult(node.host, OK, res.summary(), detail=probe.stdout.strip())

    if cfg.home:
        # An explicit java.home that is not there is a configuration error, not something
        # to paper over by installing a different JDK.
        return HostResult(node.host, FAILED,
                          "java.home %s has no usable bin/java on this host" % cfg.home,
                          detail=_found(res))

    if plan is not None:
        return _deliver_jdk(ctx, node, cfg, plan)

    if ctx.args.install_jdk:
        return _install_jdk(ctx, node, cfg)

    return HostResult(node.host, FAILED,
                      "no Java %s here; set java.archive to deliver one, java.home to "
                      "name an existing one, or pass --install-jdk" % cfg.major,
                      detail=_found(res))


def _found(res):
    if not res.candidates:
        return "no JDK found at all"
    return "found: " + ", ".join("%s (Java %s)" % (home, major)
                                 for home, major, _ in res.candidates)


def _deliver_jdk(ctx, node, cfg, plan):
    """
    Copy the JDK to one worker, reusing ``deploy``'s staging and atomic swap.

    A half-extracted JDK that looks present is exactly as bad as a half-extracted
    distribution, which is why this does not extract in place.
    """
    transport = ctx.worker(node)
    target = java.target_dir(cfg, plan)
    manifest = deploy.build_manifest(plan.path) if plan.kind == "dir" else _tar_manifest(plan)

    if not ctx.args.force:
        existing = transport.read_file(posixpath.join(target, JAVA_MANIFEST_NAME))
        if existing:
            try:
                if json.loads(existing).get("hash") == manifest["hash"]:
                    return HostResult(node.host, OK, "%s already delivered" % target)
            except ValueError:
                pass

    install_root = posixpath.dirname(target)
    writable = transport.run(["test", "-w", install_root], check=False).ok
    if not writable and not ctx.args.sudo:
        return HostResult(node.host, FAILED,
                          "%s is not writable by %s and --sudo was not passed"
                          % (install_root, node.user or "this account"))

    staging = "%s/.%s.tmp.%s" % (install_root, posixpath.basename(target),
                                 uuid.uuid4().hex[:8])
    transport.run_script(deploy.prepare_script(staging, ctx.args.sudo)).check()

    with tempfile.TemporaryDirectory() as tmp:
        if plan.kind == "dir":
            archive = Path(tmp) / "jdk.tar.gz"
            make_tarball(plan.path, archive)
            strip = 0
        else:
            archive = plan.path
            strip = plan.strip
        remote = "%s/.payload.tar.gz" % staging
        transport.upload(archive, remote)
        transport.run_script(
            "set -eu\ntar -xzf %s -C %s%s\nrm -f -- %s\n"
            % (shlex.quote(remote), shlex.quote(staging),
               " --strip-components=%d" % strip if strip else "",
               shlex.quote(remote))).check()

    check = transport.run(["test", "-x", "%s/bin/java" % staging], check=False)
    if not check.ok:
        transport.run(["rm", "-rf", "--", staging], check=False)
        return HostResult(node.host, FAILED,
                          "the delivered archive has no bin/java under %s" % staging)

    transport.write_file(json.dumps(manifest, indent=2, sort_keys=True),
                         posixpath.join(staging, JAVA_MANIFEST_NAME))
    transport.run_script(deploy.swap_script(staging, target, ctx.args.sudo, None)).check()
    return HostResult(node.host, CHANGED, "delivered %s to %s"
                      % (deploy.human(plan.bytes), target))


def _tar_manifest(plan):
    """:return: a manifest keyed on the archive itself, so redelivery is skipped."""
    stat = plan.path.stat()
    digest = hashlib.sha256(
        ("%s\0%d\0%d" % (plan.path.name, stat.st_size, int(stat.st_mtime))).encode("utf-8")
    ).hexdigest()
    return {"hash": digest, "source": plan.path.name, "bytes": plan.bytes,
            "mode": "archive size+mtime"}


def _install_jdk(ctx, node, cfg):
    """Last rung: the distribution's own JDK package. Opt-in, and it needs root."""
    if not ctx.args.sudo:
        return HostResult(node.host, FAILED,
                          "--install-jdk needs --sudo as well")
    script = """set -u
major=%(major)d
if command -v apt-get >/dev/null 2>&1; then
  echo "CHANGED installing openjdk-$major-jdk"
  sudo -n env DEBIAN_FRONTEND=noninteractive apt-get update -qq
  sudo -n env DEBIAN_FRONTEND=noninteractive apt-get install -y -qq "openjdk-$major-jdk"
elif command -v dnf >/dev/null 2>&1; then
  echo "CHANGED installing java-$major-openjdk-devel"
  sudo -n dnf install -y -q "java-$major-openjdk-devel"
elif command -v yum >/dev/null 2>&1; then
  echo "CHANGED installing java-$major-openjdk-devel"
  sudo -n yum install -y -q "java-$major-openjdk-devel"
else
  echo "no supported package manager for an automatic JDK install" >&2
  exit 2
fi
""" % {"major": int(cfg.major or 0)}
    result = ctx.worker(node).run_script(script, check=False)
    if not result.ok:
        return HostResult(node.host, FAILED, "could not install a JDK",
                          detail=(result.stderr or result.stdout).strip())
    return HostResult(node.host, CHANGED, "installed the distribution's Java %s package"
                      % cfg.major)


def _python_script(ctx):
    """
    Ensure a usable Python on the host.

    The workers do not need Python: ducktape drives them over plain SSH and runs no
    Python there.  Only the runner needs the venv, and ``run`` creates it on demand.
    This step therefore verifies rather than installs, and says so.
    """
    return """set -u
if command -v python3 >/dev/null 2>&1; then
  echo "ok: $(python3 --version 2>&1)"
else
  echo "python3 missing. Workers do not need it - ducktape drives them over plain ssh -"
  echo "but nothing here will install it either."
fi
exit 0
"""


def _user_script(ctx):
    name = ctx.args.create_user
    key = ""
    if ctx.args.authorize_key:
        with open(ctx.args.authorize_key, "r", encoding="utf-8") as handle:
            key = handle.read().strip()
    return """set -eu
user=%(user)s
key=%(key)s
if id -u "$user" >/dev/null 2>&1; then
  echo "account $user already exists"
else
  echo "CHANGED creating $user"
  sudo -n useradd -m -s /bin/bash "$user"
fi
if [ -n "$key" ]; then
  home=$(getent passwd "$user" | cut -d: -f6)
  sudo -n mkdir -p "$home/.ssh"
  if sudo -n grep -qxF "$key" "$home/.ssh/authorized_keys" 2>/dev/null; then
    echo "key already authorised"
  else
    echo "CHANGED authorising key"
    printf '%%s\\n' "$key" | sudo -n tee -a "$home/.ssh/authorized_keys" >/dev/null
  fi
  sudo -n chown -R "$user" "$home/.ssh"
  sudo -n chmod 700 "$home/.ssh"
  sudo -n chmod 600 "$home/.ssh/authorized_keys"
fi
echo done
""" % {"user": shlex.quote(name), "key": shlex.quote(key)}


def _run_ssh_env_step(ctx, nodes):
    """
    Make the selected JDK the one a *non-interactive* ssh session gets.

    The step that is easiest to forget and hardest to diagnose.  ducktape runs every
    command over non-interactive ssh, where ``~/.profile`` is not sourced, so a ``java``
    that works when you log in by hand is simply absent during a test run and the failure
    surfaces as an unrelated timeout.

    The JDK is resolved with the same ladder the ``jdk`` step uses - not with whatever
    ``java`` happens to be first on PATH - so ``provision --only ssh-env`` on its own
    still installs the JDK the operator asked for.  Both ``~/.ssh/environment`` and
    ``~/.bashrc`` are written, and neither is trusted: the step ends by opening a fresh
    connection and reporting the JDK that one actually gets.
    """
    cfg = java.config_of(ctx)
    discover = java.discovery_script(cfg)
    extra = ctx.config["provision"].get("ssh_env_path_extra") or []

    def operation(node):
        if ctx.dry_run:
            ctx.console.out("[dry-run] %s: resolve the JDK, then write ~/.ssh/environment%s"
                            % (node.host, " and ~/.bashrc" if cfg.bashrc else ""))
            ctx.console.detail(discover)
            return HostResult(node.host, SKIPPED, "dry-run")

        transport = ctx.worker(node)
        probe = transport.run_script(discover, check=False)
        res = java.parse_probe(node.host, probe.stdout, cfg)
        if not res.selected:
            return HostResult(node.host, FAILED,
                              "no Java %s to point at; run `provision --only jdk` first"
                              % (cfg.major or "JDK"), detail=_found(res))

        written = transport.run_script(java.env_script(cfg, res.home, extra), check=False)
        if not written.ok:
            return HostResult(node.host, FAILED, "could not write the environment files",
                              detail=(written.stderr or written.stdout).strip())

        return _verify_ssh_env(ctx, node, cfg, res, written)

    return fanout(nodes, operation, jobs=ctx.jobs,
                  fail_fast=getattr(ctx.args, "fail_fast", False))


def _verify_ssh_env(ctx, node, cfg, res, written):
    """Ask a fresh session what it gets. That answer, not the edit, is the result."""
    check = java.parse_probe(node.host,
                             ctx.worker(node).run_script(java.verify_script(), check=False).stdout,
                             cfg)
    detail = "\n".join(part for part in (written.stdout.strip(),
                                         "verified: %s" % (check.path_java or "no java")) if part)
    if not check.path_matches(cfg.major):
        return HostResult(node.host, FAILED,
                          "a fresh non-interactive session still gets %s, not Java %s. "
                          "sshd may ignore ~/.ssh/environment and the login shell may not "
                          "be bash; set java.home on a PATH the site already provides."
                          % (check.path_version or "no java", cfg.major),
                          detail=detail)
    status = CHANGED if "CHANGED" in written.stdout else OK
    return HostResult(node.host, status, "non-interactive java is %s from %s"
                      % (check.path_version or "unknown", res.home), detail=detail)


def _dirs_script(ctx):
    """
    Create the directories the services write into.

    ``ignitetest``'s ``PathAware`` builds every path under ``persistent_root``, default
    ``/mnt/service`` (services/utils/path.py), and the Dockerfile chowns ``/mnt``,
    ``/var/log`` and ``/opt`` to the test account.
    """
    dirs = list(ctx.config["provision"]["dirs"]) + [ctx.cluster_cfg.get("install_root", "/opt")]
    owner = ctx.cluster_cfg.get("user")
    return """set -u
changed=0
for d in %(dirs)s; do
  if [ -d "$d" ]; then
    [ -w "$d" ] || { sudo -n chown -R %(owner)s "$d" && changed=1; }
  else
    sudo -n mkdir -p "$d" && sudo -n chown -R %(owner)s "$d" && changed=1
  fi
done
if [ "$changed" -eq 1 ]; then echo "CHANGED prepared %(count)d directories"
else echo "%(count)d directories already usable"; fi
""" % {"dirs": " ".join(shlex.quote(d) for d in dirs),
       "owner": shlex.quote(str(owner)), "count": len(dirs)}


def _hosts_script(ctx, nodes):
    """
    Write the inventory into ``/etc/hosts`` between explicit markers.

    Only the block between the markers is ever rewritten.  This is the escape hatch for
    clusters whose DNS does not resolve node-to-node, mirroring what ``ducker_up`` does
    for the Docker network; it is not a substitute for working DNS.
    """
    entries = []
    for node in nodes:
        entries.append("%s %s" % (node.ip or node.host, node.host))
    block = "\n".join(entries)
    return """set -eu
block=%(block)s
tmp=$(mktemp)
awk 'BEGIN{skip=0}
     /^# BEGIN ducktests-remote$/{skip=1; next}
     /^# END ducktests-remote$/{skip=0; next}
     skip==0{print}' /etc/hosts > "$tmp"
{
  printf '%%s\\n' "%(begin)s"
  printf '%%s\\n' "$block"
  printf '%%s\\n' "%(end)s"
} >> "$tmp"
if cmp -s "$tmp" /etc/hosts; then
  rm -f "$tmp"; echo "/etc/hosts already up to date"
else
  sudo -n cp "$tmp" /etc/hosts
  rm -f "$tmp"
  echo "CHANGED rewrote the ducktests-remote block in /etc/hosts"
fi
""" % {"block": shlex.quote(block), "begin": HOSTS_BEGIN, "end": HOSTS_END}


def venv_bin(venv, name):
    """:return: ``<venv>/bin/<name>``."""
    return posixpath.join(venv, "bin", name)


# Steps that need per-host decisions in Python rather than one canned script.
_PYTHON_DRIVEN = {"jdk": _run_jdk_step, "ssh-env": _run_ssh_env_step}
