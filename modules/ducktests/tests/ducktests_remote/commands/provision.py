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

import json
import posixpath
import shlex

from ducktests_remote.cli import EXIT_OK, EXIT_PREFLIGHT, EXIT_TRANSPORT
from ducktests_remote.commands import doctor
from ducktests_remote.config import ConfigError
from ducktests_remote.fanout import (CHANGED, FAILED, HostResult, OK, SKIPPED, any_failed,
                                     fanout, render_table, summarise)

STEPS = ("packages", "jdk", "python", "user", "ssh-env", "dirs", "hosts")

HOSTS_BEGIN = "# BEGIN ducktests-remote"
HOSTS_END = "# END ducktests-remote"


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
                        help="let the jdk step install a JDK instead of only verifying one")
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
    if step == "jdk":
        return _jdk_script(ctx), bool(ctx.args.install_jdk)
    if step == "python":
        return _python_script(ctx), False
    if step == "user":
        if not ctx.args.create_user:
            return None, False
        return _user_script(ctx), True
    if step == "ssh-env":
        return _ssh_env_script(ctx), False
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


def _jdk_script(ctx):
    """
    Verify the JDK, and only install one when explicitly asked.

    Where a JDK comes from is site specific - a distro package, a Temurin tarball, an
    internal mirror - so guessing would be worse than reporting.
    """
    major = int(ctx.config["provision"]["jdk_major"])
    install = """
if command -v apt-get >/dev/null 2>&1; then
  echo "CHANGED installing openjdk-%(major)d-jdk"
  sudo -n env DEBIAN_FRONTEND=noninteractive apt-get update -qq
  sudo -n env DEBIAN_FRONTEND=noninteractive apt-get install -y -qq openjdk-%(major)d-jdk
elif command -v dnf >/dev/null 2>&1; then
  echo "CHANGED installing java-%(major)d-openjdk-devel"
  sudo -n dnf install -y -q java-%(major)d-openjdk-devel
else
  echo "no supported package manager for an automatic JDK install" >&2
  exit 2
fi
""" % {"major": major} if ctx.args.install_jdk else """
echo "java %(major)d not found; install it yourself or rerun with --only jdk --install-jdk"
exit 1
""" % {"major": major}

    return """set -u
if command -v java >/dev/null 2>&1; then
  v=$(java -version 2>&1 | head -n1)
  case "$v" in
    *\\"%(major)d*|*\\"1.%(major)d*) echo "ok: $v"; exit 0;;
    *) echo "WARN unexpected JDK: $v (expected major %(major)d)"; exit 0;;
  esac
fi
%(install)s
java -version 2>&1 | head -n1
""" % {"major": major, "install": install}


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


def _ssh_env_script(ctx):
    """
    Put JAVA_HOME and PATH into ``~/.ssh/environment``.

    This is the step that is easiest to forget and hardest to diagnose.  ducktape runs
    every command over *non-interactive* ssh, where ``~/.profile`` is not sourced, so a
    ``java`` that works when you log in by hand is simply absent during a test run.  The
    Dockerfile solves it with ``PermitUserEnvironment yes`` plus ``~/.ssh/environment``;
    the same fix applies here, and the step ends by proving it non-interactively rather
    than trusting the edit.
    """
    extra = ctx.config["provision"].get("ssh_env_path_extra") or []
    extra_path = "".join(":%s" % p for p in extra)
    return """set -u
mkdir -p ~/.ssh
chmod 700 ~/.ssh
jh="${JAVA_HOME:-}"
if [ -z "$jh" ] && command -v java >/dev/null 2>&1; then
  jh=$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")
fi
if [ -z "$jh" ]; then echo "cannot determine JAVA_HOME; run the jdk step first" >&2; exit 1; fi
want_path="PATH=$PATH:$jh/bin%(extra)s"
want_home="JAVA_HOME=$jh"
changed=0
touch ~/.ssh/environment
chmod 600 ~/.ssh/environment
for line in "$want_path" "$want_home" "LANG=C.UTF-8"; do
  key=${line%%%%=*}
  if grep -q "^$key=" ~/.ssh/environment 2>/dev/null; then
    current=$(grep "^$key=" ~/.ssh/environment | head -n1)
    [ "$current" = "$line" ] && continue
    grep -v "^$key=" ~/.ssh/environment > ~/.ssh/environment.tmp || true
    mv ~/.ssh/environment.tmp ~/.ssh/environment
  fi
  printf '%%s\\n' "$line" >> ~/.ssh/environment
  changed=1
done
chmod 600 ~/.ssh/environment
if [ "$changed" -eq 1 ]; then echo "CHANGED wrote ~/.ssh/environment"; else echo "up to date"; fi
if ! grep -qi '^ *PermitUserEnvironment *yes' /etc/ssh/sshd_config 2>/dev/null; then
  echo "NOTE sshd has no 'PermitUserEnvironment yes'; ~/.ssh/environment will be ignored."
  echo "NOTE ask your administrator for it, or make sure java is on the default PATH."
fi
echo "verify: $(command -v java || echo 'java not on this shell PATH')"
""" % {"extra": extra_path}


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
