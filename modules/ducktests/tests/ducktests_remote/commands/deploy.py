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

Two ways to move the bytes.  The default is rsync into a staging directory hardlinked
against the previous deployment (``--link-dest``), so a rebuild of one module sends one
jar rather than the whole tree.  The fallback, used when either end has no rsync or when
``--via`` stages the payload on an intermediate host, is a tarball of the whole
distribution.  Both end in the same atomic swap, and both carry exactly the file list the
manifest was built from - rsync is handed that list as a file rather than its own
``--exclude`` patterns, whose matching rules differ subtly from :func:`is_excluded`'s.

Long transfers report progress per host; see :mod:`ducktests_remote.progress`.

:func:`build_manifest`, :func:`prepare_script`, :func:`swap_script` and :func:`human` are
public because ``provision``'s ``jdk`` step delivers a JDK the same way and must not grow
a second copy of the staging-and-swap logic.
"""

import hashlib
import json
import os
import posixpath
import re
import shlex
import shutil
import tempfile
import threading
import uuid
from pathlib import Path

from ducktests_remote.cli import EXIT_OK, EXIT_TRANSPORT, EXIT_USAGE
from ducktests_remote.config import ConfigError, expand_path
from ducktests_remote.fanout import (CHANGED, FAILED, HostResult, SKIPPED, any_failed,
                                     fanout, render_table, summarise)
from ducktests_remote.progress import NullProgress, build_progress
from ducktests_remote.transport import (ProxiedTransport, is_excluded, make_tarball,
                                        parse_rsync_progress, run_local,
                                        run_local_streaming)

MANIFEST_NAME = ".ducktests-deploy.json"

# Per-distribution exclude list, read from the root of the distribution itself.  It is
# deliberately NOT called .ducktestsignore: when ignite-dev links to a checkout, the
# distribution root and the source root are the same directory, and the two lists are
# opposites - the source sync drops `target`, deploy keeps only `target`.
IGNORE_NAME = ".ducktests-deploy.ignore"

# `Number of regular files transferred: 12` on rsync 3.x, `Number of files transferred`
# on 2.x.  Thousands separators are locale-dependent; the labels are not translated.
_RSYNC_TRANSFERRED = re.compile(r"Number of (?:regular )?files transferred:\s*([\d,.]+)")
_RSYNC_SENT = re.compile(r"Total transferred file size:\s*([\d,.]+)")


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
    parser.add_argument("--no-rsync", action="store_true",
                        help="send a full tarball instead of an incremental rsync")
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

    if (not rsync_enabled(ctx) and not args.via and not args.no_rsync
            and ctx.config["deploy"].get("rsync", True)):
        console.detail("no usable rsync on this machine; sending whole tarballs")

    _print_cost(ctx, plans, nodes)

    overall = []
    for name, manifest, excludes in plans:
        console.heading("%s -> %s/%s" % (name, install_root, name))
        progress = build_progress(ctx, nodes)
        with progress:
            results = _deploy_one(ctx, dist_dir, name, manifest, install_root, nodes,
                                  excludes, progress)
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
    included, excluded = _scan(path, excludes)
    entries = []
    total = 0
    for entry, rel in included:
        stat = entry.stat()
        total += stat.st_size
        if checksum:
            entries.append("%s\0%d\0%s" % (rel.as_posix(), stat.st_size, _sha256(entry)))
        else:
            entries.append("%s\0%d\0%d" % (rel.as_posix(), stat.st_size, int(stat.st_mtime)))
    digest = hashlib.sha256("\n".join(entries).encode("utf-8")).hexdigest()
    return {"hash": digest, "files": len(entries), "bytes": total, "excluded": excluded,
            "mode": "checksum" if checksum else "size+mtime"}


def included_files(path, excludes=()):
    """
    :return: the relative posix paths a deploy would carry, sorted.

    This is what rsync is fed on stdin.  It comes from the same walk as the manifest on
    purpose: rsync's own ``--exclude`` matching is close to :func:`is_excluded` but not
    identical, and a distribution that differs from the manifest describing it is exactly
    the bug the manifest exists to prevent.
    """
    return [rel.as_posix() for _, rel in _scan(path, excludes)[0]]


def _scan(path, excludes):
    """:return: ``([(file, relative_path)], excluded_count)`` for one distribution."""
    included = []
    excluded = 0
    root = Path(path)
    for entry in sorted(root.rglob("*")):
        if entry.is_dir() or entry.is_symlink():
            continue
        rel = entry.relative_to(root)
        if is_excluded(rel, excludes):
            excluded += 1
            continue
        included.append((entry, rel))
    return included, excluded


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
    if rsync_enabled(ctx):
        console.info("rsync: only what differs from each host is sent")
        return
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


def _deploy_one(ctx, dist_dir, name, manifest, install_root, nodes, excludes=(),
                progress=None):
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
        how = "rsync" if rsync_enabled(ctx) else "tarball"
        return [HostResult(node.host, SKIPPED, "would send %s to %s (%s)"
                           % (human(manifest["bytes"]), target, how))
                for node in nodes]

    with tempfile.TemporaryDirectory() as tmp:
        payload = _Payload(Path(dist_dir) / name, Path(tmp) / ("%s.tar.gz" % name), excludes)

        via_transport = None
        staged_on_via = None
        if args.via:
            via_transport = ctx.worker(_via_node(ctx, args.via))
            staged_dir = ctx.config["deploy"]["staging_dir"]
            via_transport.mkdirs(staged_dir)
            staged_on_via = posixpath.join(staged_dir, "%s-%s.tar.gz"
                                           % (name, uuid.uuid4().hex[:8]))
            ctx.console.info("staging %s on %s" % (name, args.via))
            via_transport.upload(payload.archive(), staged_on_via)

        file_list = None
        if rsync_enabled(ctx):
            file_list = Path(tmp) / ("%s.files" % name)
            file_list.write_text("\n".join(included_files(Path(dist_dir) / name, excludes))
                                 + "\n", encoding="utf-8")

        def operation(node, _payload=payload, _staged=staged_on_via, _via=via_transport,
                      _files=file_list):
            return _deploy_to_host(ctx, node, name, target, manifest, manifest_body,
                                   _payload, _staged, _via, _files, progress)

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


class _Payload:
    """
    The tarball for one distribution, built at most once and only if a host needs it.

    With every host on the rsync path, a distribution is never tarred at all - which for
    a linked checkout is a gigabyte of compression that would buy nothing.
    """

    def __init__(self, root, archive_path, excludes):
        self.root = root
        self._archive = archive_path
        self._excludes = excludes
        self._built = False
        self._lock = threading.Lock()

    def archive(self):
        """:return: the path to the tarball, building it on first use."""
        with self._lock:
            if not self._built:
                make_tarball(self.root, self._archive, excludes=self._excludes)
                self._built = True
        return self._archive


def rsync_enabled(ctx):
    """
    :return: True when the incremental path may be used for this invocation.

    ``--via`` is deliberately excluded: its whole point is that the payload crosses the
    slow link once, as one file, which is the opposite of a per-host rsync.
    """
    if getattr(ctx.args, "no_rsync", False) or getattr(ctx.args, "via", None):
        return False
    if not ctx.config["deploy"].get("rsync", True):
        return False
    if os.name == "nt":
        # rsync reads `C:/dist/ignite-dev` as host `C`, and a coordinator that hands it a
        # Windows path silently deploys nothing.  The tarball path has no such problem.
        return False
    return shutil.which("rsync") is not None


def rsync_argv(transport, local_root, staging, *, files_from, link_dest=None,
               checksum=False, sudo=False, progress=False):
    """
    :return: the rsync command line that fills ``staging`` on one worker.

    ``--link-dest`` is what makes a redeploy cheap on both sides: unchanged files become
    hardlinks to the deployment already on the host and are never sent, so a rebuild of
    one module costs one jar.  The staging directory is still built from scratch and
    still swapped in atomically, so an interrupted transfer cannot leave a live
    distribution half updated.

    ``files_from`` is a file holding the exact list to send, written once per
    distribution.  It is not stdin: the output side is read as it arrives for progress,
    and a process whose stdin and stdout are both being pumped by one thread deadlocks
    as soon as either pipe fills.
    """
    argv = ["rsync", "-a", "--stats", "--files-from=%s" % files_from,
            "-e", shlex.join(["ssh"] + transport.ssh_options())]
    if progress:
        argv.append("--info=progress2")
    if checksum:
        argv.append("--checksum")
    if link_dest:
        argv.append("--link-dest=%s" % link_dest)
    if sudo:
        argv.append("--rsync-path=sudo -n rsync")
    argv += ["%s/" % str(local_root).rstrip("/\\"),
             "%s:%s/" % (transport.target, staging)]
    return argv


def parse_rsync_stats(text):
    """:return: ``(files_transferred, bytes_transferred)`` from ``--stats``, or None."""
    transferred = _RSYNC_TRANSFERRED.search(text or "")
    sent = _RSYNC_SENT.search(text or "")
    if not transferred or not sent:
        return None
    try:
        return (int(re.sub(r"[,.]", "", transferred.group(1))),
                int(re.sub(r"[,.]", "", sent.group(1))))
    except ValueError:
        return None


def _deploy_to_host(ctx, node, name, target, manifest, manifest_body, payload,
                    staged_on_via, via_transport, file_list=None, progress=None):
    transport = ctx.worker(node)
    progress = progress or NullProgress()
    remote_manifest = posixpath.join(target, MANIFEST_NAME)

    if not ctx.args.force:
        existing = transport.read_file(remote_manifest)
        if existing:
            try:
                if json.loads(existing).get("hash") == manifest["hash"]:
                    progress.done(node.host, "already up to date")
                    return HostResult(node.host, SKIPPED, "already at %s"
                                      % manifest["hash"][:12])
            except ValueError:
                pass

    install_root = posixpath.dirname(target)
    writable = transport.run(["test", "-w", install_root], check=False).ok
    if not writable and not ctx.args.sudo:
        progress.done(node.host, "not writable")
        return HostResult(node.host, FAILED,
                          "%s is not writable by %s and --sudo was not passed"
                          % (install_root, node.user or "this account"))

    staging = "%s/.%s.tmp.%s" % (install_root, name, uuid.uuid4().hex[:8])
    used_rsync = False
    stats = None

    if via_transport is not None:
        progress.phase(node.host, "fanning out")
        proxied = ProxiedTransport(name=node.host, via=via_transport, user=node.user,
                                   port=node.port,
                                   identity_file=node.identity_file,
                                   staging_dir=ctx.config["deploy"]["staging_dir"],
                                   dry_run=ctx.dry_run, verbose=ctx.console.verbose)
        proxied.run_script(prepare_script(staging, ctx.args.sudo)).check()
        proxied.push_archive(staged_on_via, staging)
    elif file_list is not None and getattr(transport, "has_rsync", _no_rsync)():
        outcome = _rsync_to_host(ctx, node, transport, payload.root, staging, target,
                                 file_list, progress)
        if isinstance(outcome, HostResult):
            progress.done(node.host, "failed")
            return outcome
        used_rsync, stats = True, outcome
    else:
        transport.run_script(prepare_script(staging, ctx.args.sudo)).check()
        remote_archive = "%s/.payload.tar.gz" % staging
        archive = payload.archive()
        progress.phase(node.host, "sending")
        transport.upload_watched(archive, remote_archive,
                                 on_bytes=_watcher(progress, node.host,
                                                   os.path.getsize(archive)))
        progress.phase(node.host, "extracting")
        transport.run_script(
            "set -eu\ntar -xzf %s -C %s\nrm -f -- %s\n"
            % (shlex.quote(remote_archive), shlex.quote(staging),
               shlex.quote(remote_archive))).check()

    progress.phase(node.host, "swapping")
    transport.write_file(manifest_body, posixpath.join(staging, MANIFEST_NAME))
    transport.run_script(swap_script(staging, target, ctx.args.sudo, ctx.args.owner)).check()
    progress.done(node.host)
    if used_rsync:
        if stats:
            return HostResult(node.host, CHANGED, "rsync: %d of %s file(s) changed, %s sent"
                              % (stats[0], manifest["files"], human(stats[1])))
        return HostResult(node.host, CHANGED, "rsync: %s file(s)" % manifest["files"])
    return HostResult(node.host, CHANGED, "%s files, %s"
                      % (manifest["files"], human(manifest["bytes"])))


def _no_rsync():
    """``has_rsync`` stand-in for transports that have no such notion (local copies)."""
    return False


def _watcher(progress, host, total):
    """:return: an ``on_bytes`` callback that feeds one host's row."""
    return lambda sent: progress.sent(host, sent, total)


def _rsync_to_host(ctx, node, transport, local_root, staging, target, file_list,
                   progress=None):
    """
    :return: ``(files, bytes)`` transferred, ``None`` when ``--stats`` could not be read,
        or a failed :class:`HostResult`.

    The staging directory is created before rsync runs, and hardlinked against the live
    distribution when there is one to link against.
    """
    progress = progress or NullProgress()
    transport.run_script(prepare_script(staging, ctx.args.sudo)).check()
    # Both display modes need the byte counts; only the display itself differs.
    watched = progress.watching
    argv = rsync_argv(transport, local_root, staging, files_from=file_list,
                      link_dest=target if transport.exists(target) else None,
                      checksum=ctx.args.checksum or ctx.config["deploy"].get("checksum", False),
                      sudo=ctx.args.sudo, progress=watched)
    ctx.console.detail("%s: %s" % (node.host, shlex.join(argv)))
    progress.phase(node.host, "sending")
    if watched:
        result = run_local_streaming(argv, host=node.host,
                                     on_output=_rsync_watcher(progress, node.host))
    else:
        result = run_local(argv)
    if not result.ok:
        return HostResult(node.host, FAILED, "rsync failed",
                          detail=(result.stderr or result.stdout).strip())
    return parse_rsync_stats(result.stdout)


def _rsync_watcher(progress, host):
    """:return: a line callback that turns ``--info=progress2`` into one host's row."""
    def watch(line):
        reading = parse_rsync_progress(line)
        if reading:
            progress.sent(host, reading[0], fraction=reading[1])
    return watch


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
