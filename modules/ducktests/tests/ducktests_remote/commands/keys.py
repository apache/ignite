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

"""``keys push`` - install the runner's identity and authorise it on every worker."""

import argparse
import shlex
from pathlib import Path

from ducktests_remote.cli import EXIT_OK, EXIT_TRANSPORT, EXIT_USAGE
from ducktests_remote.config import ConfigError, expand_path
from ducktests_remote.fanout import (CHANGED, HostResult, OK, any_failed, fanout,
                                     render_table, summarise)
from ducktests_remote.transport import run_local


def register(subparsers, common):
    """Wire up the ``keys`` subcommand."""
    parser = subparsers.add_parser(
        "keys", parents=[common], help="manage the ssh key ducktape uses",
        description="""Install the private key on the runner and authorise the matching
public key on every worker.

Why this exists: ducktape connects from the runner to the workers using the identity
named in the cluster file, and a run launched with --detach outlives the coordinator's
ssh session. Agent forwarding dies with that session, so an agent-only setup produces a
run that authenticates for the first few minutes and then fails on every subsequent
connection. A real key file on the runner is the only arrangement that survives.""",
        formatter_class=_RawHelp)
    sub = parser.add_subparsers(dest="keys_command", metavar="<action>")
    push = sub.add_parser("push", parents=[common],
                          help="install the identity on the runner and authorise it on "
                               "the workers")
    push.add_argument("--identity", metavar="PATH",
                      help="coordinator-side private key (default: cluster.identity_file, "
                           "or a generated one)")
    push.add_argument("--generate", action="store_true",
                      help="generate a new keypair when the identity does not exist")
    push.add_argument("-n", "--num-nodes", type=int, default=None,
                      help="only authorise on the first N inventory hosts")
    push.set_defaults(handler=execute_push)
    parser.set_defaults(handler=_no_action)


class _RawHelp(argparse.RawDescriptionHelpFormatter):
    """Keeps the explanation in the help text readable."""


def _no_action(ctx):
    ctx.console.error("keys: expected an action, e.g. `ducktests-remote keys push`")
    return EXIT_USAGE


def execute_push(ctx):
    """Install and authorise the key. :return: the process exit code."""
    console = ctx.console
    identity = Path(expand_path(ctx.args.identity or ctx.identity_file or "~/.ssh/id_rsa"))
    public = Path(str(identity) + ".pub")

    if not identity.is_file():
        if not ctx.args.generate:
            raise ConfigError(
                "%s does not exist on this machine. Pass --generate to create a keypair, "
                "or --identity to point at an existing one." % identity)
        if ctx.dry_run:
            console.out("[dry-run] would generate a keypair at %s" % identity)
        else:
            identity.parent.mkdir(parents=True, exist_ok=True)
            run_local(["ssh-keygen", "-m", "PEM", "-q", "-t", "rsa", "-N", "",
                       "-f", str(identity)], check=True)
            console.out("generated %s" % identity)

    if not public.is_file() and not ctx.dry_run:
        raise ConfigError("public key %s not found next to the private key" % public)

    pubkey = public.read_text(encoding="utf-8").strip() if public.is_file() else "<public key>"

    runner_target = ctx.runner.expand(ctx.identity_file or "~/.ssh/id_rsa")
    console.heading("RUNNER %s" % ctx.runner_host)
    if ctx.dry_run:
        console.out("[dry-run] would install %s -> %s:%s (mode 0600)"
                    % (identity, ctx.runner_host, runner_target))
    else:
        ctx.runner.mkdirs(_parent(runner_target), mode=0o700)
        ctx.runner.upload(identity, runner_target, mode=0o600)
        ctx.runner.upload(public, runner_target + ".pub", mode=0o644)
        console.out("installed %s (mode 0600)" % runner_target)

    console.heading("WORKERS")
    results = fanout(ctx.all_nodes, lambda node: _authorize(ctx, node, pubkey),
                     jobs=ctx.jobs, fail_fast=getattr(ctx.args, "fail_fast", False))
    console.out(render_table(results, verbose=console.verbose))
    console.out(summarise(results))
    return EXIT_TRANSPORT if any_failed(results) else EXIT_OK


def _authorize(ctx, node, pubkey):
    if ctx.dry_run:
        return HostResult(node.host, OK, "would authorise the key for %s" % node.user)
    script = """set -eu
key=%s
mkdir -p ~/.ssh
chmod 700 ~/.ssh
touch ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
if grep -qxF "$key" ~/.ssh/authorized_keys; then
  echo present
else
  printf '%%s\\n' "$key" >> ~/.ssh/authorized_keys
  echo added
fi
""" % shlex.quote(pubkey)
    result = ctx.worker(node).run_script(script, check=False)
    if not result.ok:
        return HostResult(node.host, "failed", "could not write authorized_keys",
                          detail=result.stderr.strip())
    added = result.out == "added"
    return HostResult(node.host, CHANGED if added else OK,
                      "authorised" if added else "already authorised")


def _parent(path):
    return path.rsplit("/", 1)[0] or "/"
