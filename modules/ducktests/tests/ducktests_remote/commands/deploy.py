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
``deploy`` - push distributions from a coordinator-local directory to the install root.

Deliberately dumb.  Each subdirectory of ``--dist-dir`` is copied verbatim to
``<install_root>/<name>``; the name is never interpreted, rewritten, or checked against
version-parsing logic.  The operator names the directories to match what the tests
expect, which is also what makes fork layouts work without special cases.

The one filter is ``--exclude``, which exists because ``ignite-dev`` is normally a link
to a source checkout: the workers need the built jars under ``modules/*/target``, and
nothing else in the tree.  Excludes are opt-in and default to nothing, so a distribution
without them is still shipped byte for byte.

:func:`build_manifest`, :func:`prepare_script`, :func:`swap_script` and :func:`human` are
public because ``provision``'s ``jdk`` step delivers a JDK the same way and must not grow
a second copy of the staging-and-swap logic.
"""

import hashlib
import json
import os
import posixpath
import shlex
import tempfile
import uuid
from pathlib import Path

from ducktests_remote.cli import EXIT_OK, EXIT_TRANSPORT, EXIT_USAGE
from ducktests_remote.config import ConfigError, expand_path
from ducktests_remote.fanout import (CHANGED, FAILED, HostResult, SKIPPED, any_failed,
                                     fanout, render_table, summarise)
from ducktests_remote.transport import ProxiedTransport, is_excluded, make_tarball

MANIFEST_NAME = ".ducktests-deploy.json"

# Per-distribution exclude list, read from the root of the distribution itself.  It is
# deliberately NOT called .ducktestsignore: when ignite-dev links to a checkout, the
# distribution root and the source root are the same directory, and the two lists are
# opposites - the source sync drops `target`, deploy keeps only `target`.
IGNORE_NAME = ".ducktests-deploy.ignore"


def register(subparsers, common):
    """Wire up the ``deploy`` subcommand."""
    parser = subparsers.add_parser(
        "deploy", parents=[common], help="copy distributions to the workers",
        description="Copy each subdirectory of --dist-dir to <install_root>/<name> on "
                    "every worker, skipping hosts that already have identical content.")
    parser.add_argument("--dist-dir", metavar="PATH",
                        help="directory holding one subdirectory per distribution")
    parser.add_argument("--only", action="append", default=[], metavar="NAME",
                        help="restrict to this distribution; repeatable")
    parser.add_argument("--exclude", action="append", default=[], metavar="PATTERN",
                        help="rsync-style pattern to leave out of every distribution; "
                             "repeatable. Overrides %s and deploy.exclude" % IGNORE_NAME)
    parser.add_argument("--install-root", metavar="PATH", help="target root on the workers")
    parser.add_argument("--via", metavar="HOST",
                        help="upload once to HOST, then fan out from there")
    parser.add_argument("--sudo", action="store_true",
                        help="prefix remote commands with `sudo -n`")
    parser.add_argument("--owner", metavar="USER", help="chown -R the extracted tree")
    parser.add_argument("--force", action="store_true",
                        help="redeploy even when the manifest already matches")
    parser.add_argument("--checksum", action="store_true",
                        help="hash file contents for the manifest instead of size+mtime")
    parser.add_argument("-n", "--num-nodes", type=int, default=None,
                        help="only deploy to the first N inventory hosts")
    parser.add_argument("--json", action="store_true", help="machine-readable output")
    parser.set_defaults(handler=execute)


def execute(ctx):  # pylint: disable=too-many-locals
    """Deploy distributions. :return: the process exit code."""
    args = ctx.args
    console = ctx.console

    dist_dir = Path(expand_path(args.dist_dir or ctx.config["deploy"]["dist_dir"]))
    if not dist_dir.is_dir():
        raise ConfigError("--dist-dir %s does not exist" % dist_dir)

    install_root = (args.install_root or ctx.config["deploy"].get("install_root")
                    or ctx.cluster_cfg.get("install_root", "/opt"))
    nodes = ctx.all_nodes
    if not nodes:
        raise ConfigError("cluster.nodes is empty; there is nowhere to deploy")

    dists = _distributions(dist_dir, args.only)
    if not dists:
        console.error("no distributions found under %s" % dist_dir)
        return EXIT_USAGE

    use_checksum = args.checksum or ctx.config["deploy"].get("checksum", False)
    plans = []
    for name in dists:
        excludes = resolve_excludes(ctx, dist_dir / name)
        manifest = build_manifest(dist_dir / name, checksum=use_checksum, excludes=excludes)
        if excludes and manifest["excluded"]:
            console.info("%s: %d file(s) left out by %d pattern(s)"
                         % (name, manifest["excluded"], len(excludes)))
            console.detail("excludes: %s" % ", ".join(excludes))
        plans.append((name, manifest, excludes))

    _print_cost(ctx, plans, nodes)

    overall = []
    for name, manifest, excludes in plans:
        console.heading("%s -> %s/%s" % (name, install_root, name))
        results = _deploy_one(ctx, dist_dir, name, manifest, install_root, nodes, excludes)
        overall.extend(results)
        console.out(render_table(results, verbose=console.verbose))
        console.out(summarise(results))

    if args.json:
        console.out(json.dumps([{"host": r.host, "status": r.status, "message": r.message}
                                for r in overall], indent=2))

    return EXIT_TRANSPORT if any_failed(overall) else EXIT_OK


def _distributions(dist_dir, only):
    names = sorted(p.name for p in dist_dir.iterdir()
                   if p.is_dir() and not p.name.startswith("."))
    if only:
        missing = [n for n in only if n not in names]
        if missing:
            raise ConfigError("--only %s: not found under %s (available: %s)"
                              % (", ".join(missing), dist_dir, ", ".join(names) or "none"))
        return [n for n in names if n in only]
    return names


def resolve_excludes(ctx, dist_root):
    """
    :return: the exclude patterns for one distribution, most specific source winning.

    ``--exclude`` beats a :data:`IGNORE_NAME` file at the root of the distribution, which
    beats ``deploy.exclude`` in the configuration.  The list is never merged across
    sources: a pattern list is read as a whole, the way ``run``'s source-sync list is.
    """
    ignore_file = Path(dist_root) / IGNORE_NAME
    # The list file is bookkeeping, not part of the distribution; never ship it.
    tail = [IGNORE_NAME] if ignore_file.is_file() else []
    if ctx.args.exclude:
        return list(ctx.args.exclude) + tail
    if ignore_file.is_file():
        return [line.strip() for line in ignore_file.read_text(encoding="utf-8").splitlines()
                if line.strip() and not line.strip().startswith("#")] + tail
    return list(ctx.config["deploy"].get("exclude") or [])


def build_manifest(path, *, checksum=False, excludes=()):
    """
    :return: a manifest describing ``path``, used to skip hosts that already match.

    Sorted relative paths plus sizes and mtimes by default; ``--checksum`` adds a content
    sha256 per file, which is exact but reads gigabytes off disk every time.

    ``excludes`` must be the same list the tarball is built with, or a host would be
    called up to date while holding a different set of files.
    """
    entries = []
    total = 0
    excluded = 0
    root = Path(path)
    for entry in sorted(root.rglob("*")):
        if entry.is_dir() or entry.is_symlink():
            continue
        rel = entry.relative_to(root)
        if is_excluded(rel, excludes):
            excluded += 1
            continue
        stat = entry.stat()
        total += stat.st_size
        if checksum:
            entries.append("%s\0%d\0%s" % (rel.as_posix(), stat.st_size, _sha256(entry)))
        else:
            entries.append("%s\0%d\0%d" % (rel.as_posix(), stat.st_size, int(stat.st_mtime)))
    digest = hashlib.sha256("\n".join(entries).encode("utf-8")).hexdigest()
    return {"hash": digest, "files": len(entries), "bytes": total, "excluded": excluded,
            "mode": "checksum" if checksum else "size+mtime"}


def _sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _print_cost(ctx, plans, nodes):
    total = sum(m["bytes"] for _, m, _ in plans)
    per_host = human(total)
    console = ctx.console
    console.info("%d distribution(s), %s each, %d host(s) = %s total"
                 % (len(plans), per_host, len(nodes), human(total * len(nodes))))
    if not ctx.args.via and len(nodes) > 3 and total > 200 * 1024 * 1024:
        console.warn("that is %s over the wire from this machine. `--via <host-near-the-"
                     "cluster>` uploads it once and fans out from there."
                     % human(total * len(nodes)))


def human(size):
    value = float(size)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            return "%.0f %s" % (value, unit) if unit in ("B", "KB") else "%.1f %s" % (value, unit)
        value /= 1024
    return "%.1f TB" % value


def _deploy_one(ctx, dist_dir, name, manifest, install_root, nodes, excludes=()):
    args = ctx.args
    target = posixpath.join(install_root, name)
    manifest_body = json.dumps({
        **manifest,
        "name": name,
        "source": str((Path(dist_dir) / name).resolve()),
        "deployed_by": os.environ.get("USER") or os.environ.get("USERNAME") or "unknown",
        "deployed_at": _now(),
    }, indent=2, sort_keys=True)

    if ctx.dry_run:
        return [HostResult(node.host, SKIPPED,
                           "would send %s to %s" % (human(manifest["bytes"]), target))
                for node in nodes]

    with tempfile.TemporaryDirectory() as tmp:
        archive = Path(tmp) / ("%s.tar.gz" % name)
        make_tarball(Path(dist_dir) / name, archive, excludes=excludes)

        via_transport = None
        staged_on_via = None
        if args.via:
            via_transport = ctx.worker(_via_node(ctx, args.via))
            staged_dir = ctx.config["deploy"]["staging_dir"]
            via_transport.mkdirs(staged_dir)
            staged_on_via = posixpath.join(staged_dir, "%s-%s.tar.gz"
                                           % (name, uuid.uuid4().hex[:8]))
            ctx.console.info("staging %s on %s" % (name, args.via))
            via_transport.upload(archive, staged_on_via)

        def operation(node, _archive=archive, _staged=staged_on_via, _via=via_transport):
            return _deploy_to_host(ctx, node, name, target, manifest, manifest_body,
                                   _archive, _staged, _via)

        try:
            return fanout(nodes, operation, jobs=ctx.jobs,
                          fail_fast=getattr(ctx.args, "fail_fast", False))
        finally:
            if via_transport is not None and staged_on_via:
                via_transport.run(["rm", "-f", "--", staged_on_via], check=False)


def _via_node(ctx, host):
    for node in ctx.all_nodes:
        if node.host == host:
            return node
    from ducktests_remote.cluster import Node  # pylint: disable=import-outside-toplevel
    return Node(host=host, user=ctx.cluster_cfg.get("user"),
                port=ctx.cluster_cfg.get("port", 22),
                identity_file=ctx.cluster_cfg.get("identity_file"))


def _deploy_to_host(ctx, node, name, target, manifest, manifest_body, archive,
                    staged_on_via, via_transport):
    transport = ctx.worker(node)
    remote_manifest = posixpath.join(target, MANIFEST_NAME)

    if not ctx.args.force:
        existing = transport.read_file(remote_manifest)
        if existing:
            try:
                if json.loads(existing).get("hash") == manifest["hash"]:
                    return HostResult(node.host, SKIPPED, "already at %s"
                                      % manifest["hash"][:12])
            except ValueError:
                pass

    install_root = posixpath.dirname(target)
    writable = transport.run(["test", "-w", install_root], check=False).ok
    if not writable and not ctx.args.sudo:
        return HostResult(node.host, FAILED,
                          "%s is not writable by %s and --sudo was not passed"
                          % (install_root, node.user or "this account"))

    staging = "%s/.%s.tmp.%s" % (install_root, name, uuid.uuid4().hex[:8])

    if via_transport is not None:
        proxied = ProxiedTransport(name=node.host, via=via_transport, user=node.user,
                                   port=node.port,
                                   identity_file=node.identity_file,
                                   staging_dir=ctx.config["deploy"]["staging_dir"],
                                   dry_run=ctx.dry_run, verbose=ctx.console.verbose)
        proxied.run_script(prepare_script(staging, ctx.args.sudo)).check()
        proxied.push_archive(staged_on_via, staging)
    else:
        transport.run_script(prepare_script(staging, ctx.args.sudo)).check()
        remote_archive = "%s/.payload.tar.gz" % staging
        transport.upload(archive, remote_archive)
        transport.run_script(
            "set -eu\ntar -xzf %s -C %s\nrm -f -- %s\n"
            % (shlex.quote(remote_archive), shlex.quote(staging),
               shlex.quote(remote_archive))).check()

    transport.write_file(manifest_body, posixpath.join(staging, MANIFEST_NAME))
    transport.run_script(swap_script(staging, target, ctx.args.sudo, ctx.args.owner)).check()
    return HostResult(node.host, CHANGED, "%s files, %s"
                      % (manifest["files"], human(manifest["bytes"])))


def prepare_script(staging, use_sudo):
    sudo = "sudo -n " if use_sudo else ""
    return "set -eu\n%(sudo)srm -rf -- %(staging)s\n%(sudo)smkdir -p %(staging)s\n" % {
        "sudo": sudo, "staging": shlex.quote(staging)}


def swap_script(staging, target, use_sudo, owner):
    """
    Swap the freshly extracted tree into place, then delete the old one.

    A half-copied distribution that looks present is worse than an absent one: the tests
    start, fail somewhere inside the JVM, and nobody suspects the copy.
    """
    sudo = "sudo -n " if use_sudo else ""
    script = """set -eu
staging=%(staging)s
target=%(target)s
old="$target.old.$$"
if [ -e "$target" ]; then %(sudo)smv -- "$target" "$old"; fi
%(sudo)smv -- "$staging" "$target"
if [ -e "$old" ]; then %(sudo)srm -rf -- "$old"; fi
""" % {"staging": shlex.quote(staging), "target": shlex.quote(target), "sudo": sudo}
    if owner:
        script += "%schown -R %s -- %s\n" % (sudo, shlex.quote(owner), shlex.quote(target))
    return script


def _now():
    from datetime import datetime, timezone  # pylint: disable=import-outside-toplevel
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
