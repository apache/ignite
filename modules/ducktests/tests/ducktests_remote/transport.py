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
Command transport.

Every command the CLI runs, locally or remotely, goes through a :class:`Transport`.
Nothing above this boundary may shell out to ``ssh`` directly, so ``--runner local`` and
``--runner some-host`` exercise identical code paths.
"""

import abc
import os
import posixpath
import re
import shlex
import shutil
import subprocess
import tarfile
import tempfile
import threading
import uuid
from dataclasses import dataclass, field
from fnmatch import fnmatch
from pathlib import Path
from typing import List, Optional, Sequence, Union

# OpenSSH on Windows has no ControlMaster support; multiplexing options make it fail hard.
_MULTIPLEXING_SUPPORTED = os.name != "nt"

DEFAULT_SSH_OPTIONS = (
    "-o", "BatchMode=yes",
    "-o", "StrictHostKeyChecking=accept-new",
    "-o", "ServerAliveInterval=30",
)


class TransportError(Exception):
    """Raised when a transport-level operation fails (exit code 5 territory)."""

    def __init__(self, message, result=None):
        super().__init__(message)
        self.result = result


@dataclass
class Result:
    """Outcome of a single command."""

    argv: List[str]
    returncode: int
    stdout: str = ""
    stderr: str = ""
    host: str = "local"

    @property
    def ok(self):
        """:return: True when the command exited successfully."""
        return self.returncode == 0

    @property
    def out(self):
        """:return: stdout with surrounding whitespace removed."""
        return self.stdout.strip()

    def check(self):
        """Raise :class:`TransportError` unless the command succeeded."""
        if not self.ok:
            raise TransportError(
                "command failed on %s (exit %d): %s\n%s"
                % (self.host, self.returncode, shlex.join(self.argv), self.stderr.strip()),
                self)
        return self


@dataclass
class Transport(abc.ABC):  # pylint: disable=too-many-instance-attributes
    """Runs commands and moves files on exactly one host."""

    name: str = "local"
    dry_run: bool = False
    verbose: bool = False
    printer: Optional[object] = None
    _home: Optional[str] = field(default=None, init=False, repr=False)

    def home(self):
        """:return: the home directory of the account this transport connects as."""
        if self._home is None:
            if self.dry_run:
                # Nothing is executed in a dry run, so the remote home is unknown; keep
                # the tilde so the preview stays readable rather than inventing a path.
                self._home = "~"
            else:
                result = self.run(["printenv", "HOME"], check=False)
                self._home = result.out or "/root"
        return self._home

    def expand(self, path):
        """
        Expand a leading ``~`` against the *remote* home directory.

        Paths are shell-quoted before they reach the remote side, so a literal tilde
        would never be expanded by the remote shell.  Resolving it here, once per
        connection, keeps ``~/.ducktests-remote`` working without giving up quoting.
        """
        text = str(path)
        if text == "~":
            return self.home()
        if text.startswith("~/"):
            return posixpath.join(self.home(), text[2:])
        return text

    def _echo(self, message):
        if self.printer is not None:
            self.printer(message)

    def _trace(self, argv):
        if self.dry_run:
            self._echo("[dry-run] %s$ %s" % (self.name, shlex.join(argv)))
        elif self.verbose:
            self._echo("+ %s$ %s" % (self.name, shlex.join(argv)))

    @abc.abstractmethod
    def run(self, argv, *, check=True, timeout=None, input=None):  # noqa: A002 - matches spec
        """Run ``argv`` (a list, never a string) and return a :class:`Result`."""

    def run_script(self, script, **kw):
        """
        Feed a shell script to ``bash -s`` over stdin.

        Long quoted one-liners are the single largest source of remote-execution bugs;
        anything longer than a couple of words belongs here instead.
        """
        return self.run(["bash", "-s"], input=script, **kw)

    @abc.abstractmethod
    def upload(self, local_path, remote_path, *, mode=None):
        """Copy a single local file to ``remote_path``."""

    def upload_watched(self, local_path, remote_path, *, mode=None, on_bytes=None):
        """
        Copy a file, reporting bytes as they go out.

        The default is :meth:`upload` with no reporting at all; only transports that can
        see the bytes leave override it.  Callers therefore never have to ask whether
        progress is available.
        """
        del on_bytes
        return self.upload(local_path, remote_path, mode=mode)

    @abc.abstractmethod
    def download(self, remote_path, local_path):
        """Copy a single remote file to ``local_path``."""

    @abc.abstractmethod
    def upload_dir(self, local_dir, remote_dir, *, excludes=(), delete=False,
                   on_progress=None):
        """
        Copy a directory tree; ``local_dir`` contents land inside ``remote_dir``.

        ``on_progress(sent_bytes, fraction)`` is called as the transfer goes, with
        ``fraction`` set only when the sender knows it.  Implementations that cannot see
        the bytes leave simply never call it.
        """

    def exists(self, remote_path):
        """:return: True when ``remote_path`` exists on this host."""
        if self.dry_run:
            return True
        return self.run(["test", "-e", remote_path], check=False).ok

    def mkdirs(self, remote_path, *, mode=None):
        """Create ``remote_path`` and any missing parents."""
        self.run(["mkdir", "-p", remote_path]).check()
        if mode is not None:
            self.run(["chmod", "%o" % mode, remote_path]).check()

    def write_file(self, content, remote_path, *, mode=None):
        """
        Write ``content`` to ``remote_path`` without going through a shell quote.

        Written as bytes with explicit LF endings: a coordinator on Windows would
        otherwise translate every newline to CRLF, and a shell script with carriage
        returns fails on the runner with a message that names the wrong line.
        """
        with tempfile.TemporaryDirectory() as tmp:
            local = Path(tmp) / "payload"
            local.write_bytes(content.replace("\r\n", "\n").encode("utf-8"))
            self.upload(local, remote_path, mode=mode)

    def read_file(self, remote_path, *, missing_ok=True):
        """:return: contents of ``remote_path``, or None when it does not exist."""
        result = self.run(["cat", remote_path], check=False)
        if result.ok:
            return result.stdout
        if missing_ok:
            return None
        return result.check().stdout


class LocalTransport(Transport):
    """Runs everything in the coordinator's own filesystem."""

    def __init__(self, **kw):
        super().__init__(name="local", **kw)

    def run(self, argv, *, check=True, timeout=None, input=None):  # noqa: A002 - matches spec
        argv = [str(a) for a in argv]
        self._trace(argv)
        if self.dry_run:
            return Result(argv, 0, host=self.name)
        return _spawn(argv, host=self.name, check=check, timeout=timeout, input=input)

    def upload(self, local_path, remote_path, *, mode=None):
        self._trace(["cp", str(local_path), remote_path])
        if self.dry_run:
            return
        Path(remote_path).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(str(local_path), remote_path)
        if mode is not None:
            os.chmod(remote_path, mode)

    def upload_watched(self, local_path, remote_path, *, mode=None, on_bytes=None):
        if on_bytes is None:
            return self.upload(local_path, remote_path, mode=mode)
        self._trace(["cp", str(local_path), remote_path])
        if self.dry_run:
            return None
        Path(remote_path).parent.mkdir(parents=True, exist_ok=True)
        sent = 0
        with open(local_path, "rb") as source, open(remote_path, "wb") as target:
            for block in iter(lambda: source.read(CHUNK), b""):
                target.write(block)
                sent += len(block)
                on_bytes(sent)
        if mode is not None:
            os.chmod(remote_path, mode)
        return None

    def download(self, remote_path, local_path):
        self._trace(["cp", remote_path, str(local_path)])
        if self.dry_run:
            return
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(remote_path, str(local_path))

    def upload_dir(self, local_dir, remote_dir, *, excludes=(), delete=False,
                   on_progress=None):
        del on_progress  # a local copy is not worth watching
        self._trace(["cp", "-r", str(local_dir), remote_dir])
        if self.dry_run:
            return
        if delete and Path(remote_dir).exists():
            shutil.rmtree(remote_dir)
        ignore = shutil.ignore_patterns(*[_exclude_to_glob(e) for e in excludes]) if excludes else None
        shutil.copytree(str(local_dir), remote_dir, dirs_exist_ok=True, ignore=ignore, symlinks=True)


@dataclass
class SshTransport(Transport):
    """
    Runs commands on a remote host through the *system* ``ssh``/``scp`` binaries.

    paramiko is deliberately not used: the system client brings ``~/.ssh/config``,
    ``ProxyJump``, agent and Kerberos support along for free, and it is the same client
    an engineer would use by hand when reproducing a failure.
    """

    user: Optional[str] = None
    port: int = 22
    identity_file: Optional[str] = None
    connect_timeout: int = 15
    control_dir: Optional[str] = None
    _rsync: Optional[bool] = field(default=None, init=False, repr=False)

    def __post_init__(self):
        if self.control_dir is None:
            self.control_dir = tempfile.gettempdir()

    @property
    def target(self):
        """:return: ``user@host`` or plain host when no user is configured."""
        return "%s@%s" % (self.user, self.name) if self.user else self.name

    def ssh_options(self, *, for_scp=False):
        """:return: the base ssh option list shared by every connection this class makes."""
        opts = list(DEFAULT_SSH_OPTIONS)
        opts += ["-o", "ConnectTimeout=%d" % self.connect_timeout]
        if _MULTIPLEXING_SUPPORTED:
            # doctor and deploy open many connections per host; multiplexing turns each
            # extra one into a no-cost channel on the first connection.
            opts += ["-o", "ControlMaster=auto",
                     "-o", "ControlPersist=60s",
                     "-o", "ControlPath=%s" % os.path.join(self.control_dir, "dtr-%r@%h:%p")]
        if self.identity_file:
            opts += ["-o", "IdentitiesOnly=yes", "-i", os.path.expanduser(self.identity_file)]
        if self.port and int(self.port) != 22:
            opts += ["-P" if for_scp else "-p", str(self.port)]
        return opts

    def run(self, argv, *, check=True, timeout=None, input=None):  # noqa: A002 - matches spec
        argv = [str(a) for a in argv]
        remote = shlex.join(argv)
        full = ["ssh"] + self.ssh_options() + ["-T", self.target, remote]
        self._trace(argv)
        if self.dry_run:
            return Result(argv, 0, host=self.name)
        result = _spawn(full, host=self.name, check=check, timeout=timeout, input=input)
        return Result(argv, result.returncode, result.stdout, result.stderr, self.name)

    def upload(self, local_path, remote_path, *, mode=None):
        self._trace(["scp", str(local_path), "%s:%s" % (self.target, remote_path)])
        if self.dry_run:
            return
        remote = "%s:%s" % (self.target, remote_path)
        argv = ["scp"] + self.ssh_options(for_scp=True) + [str(local_path), remote]
        _spawn(argv, host=self.name, check=True)
        if mode is not None:
            self.run(["chmod", "%o" % mode, remote_path]).check()

    def upload_watched(self, local_path, remote_path, *, mode=None, on_bytes=None):
        """
        Send a file through ``ssh 'cat > path'``, counting the bytes on the way in.

        scp is the better tool and stays the default, but its progress meter is written
        for a terminal and suppressed when stdout is a pipe - which it always is here.
        Feeding the file to a remote ``cat`` ourselves is the only way to know how far a
        300 MB payload has got, and the write blocks on a slow link exactly as scp does.
        """
        if on_bytes is None:
            return self.upload(local_path, remote_path, mode=mode)
        self._trace(["cat", str(local_path), ">", "%s:%s" % (self.target, remote_path)])
        if self.dry_run:
            return None
        command = "cat > %s" % shlex.quote(remote_path)
        argv = ["ssh"] + self.ssh_options() + ["-T", self.target, command]
        _stream_to_stdin(argv, local_path, on_bytes, host=self.name)
        if mode is not None:
            self.run(["chmod", "%o" % mode, remote_path]).check()
        return None

    def download(self, remote_path, local_path):
        self._trace(["scp", "%s:%s" % (self.target, remote_path), str(local_path)])
        if self.dry_run:
            return
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        remote = "%s:%s" % (self.target, remote_path)
        argv = ["scp"] + self.ssh_options(for_scp=True) + [remote, str(local_path)]
        _spawn(argv, host=self.name, check=True)

    def has_rsync(self):
        """:return: True when rsync is usable on both ends. Probed once, then cached."""
        if self._rsync is None:
            if self.dry_run:
                self._rsync = False
            else:
                local = shutil.which("rsync") is not None
                remote = local and self.run(["command", "-v", "rsync"], check=False).ok
                self._rsync = bool(remote)
        return self._rsync

    def upload_dir(self, local_dir, remote_dir, *, excludes=(), delete=False,
                   on_progress=None):
        local_dir = str(local_dir)
        self._trace(["rsync", local_dir, "%s:%s" % (self.target, remote_dir)])
        if self.dry_run:
            return
        self.mkdirs(remote_dir)
        if self.has_rsync():
            argv = ["rsync", "-az", "-e", shlex.join(["ssh"] + self.ssh_options())]
            if delete:
                argv.append("--delete")
            for pattern in excludes:
                argv += ["--exclude", pattern]
            argv += [local_dir.rstrip("/\\") + "/", "%s:%s/" % (self.target, remote_dir)]
            if on_progress is None:
                _spawn(argv, host=self.name, check=True)
                return
            argv.insert(1, "--info=progress2")
            run_local_streaming(argv, host=self.name,
                                on_output=rsync_reporter(on_progress)).check()
            return
        # rsync missing on one of the ends: fall back to a tar stream through scp.
        with tempfile.TemporaryDirectory() as tmp:
            archive = Path(tmp) / "payload.tar.gz"
            make_tarball(local_dir, archive, excludes=excludes)
            staged = "/tmp/dtr-upload-%s.tar.gz" % uuid.uuid4().hex[:8]
            if on_progress is None:
                self.upload(archive, staged)
            else:
                total = archive.stat().st_size
                self.upload_watched(archive, staged,
                                    on_bytes=lambda sent: on_progress(sent, sent / total))
            script = "set -eu\nmkdir -p %s\n" % shlex.quote(remote_dir)
            if delete:
                script += "rm -rf -- %s/*\n" % shlex.quote(remote_dir)
            script += "tar -xzf %s -C %s\nrm -f -- %s\n" % (
                shlex.quote(staged), shlex.quote(remote_dir), shlex.quote(staged))
            self.run_script(script).check()


@dataclass
class ProxiedTransport(Transport):
    """
    Runs commands on a host by hopping through another transport.

    Two uses: probing a worker over the connection *ducktape itself* will make (from the
    runner, with the runner-side identity file), and ``deploy --via HOST``, where a
    payload is uploaded once and then fanned out from a machine that is close to the
    workers instead of once per host from a laptop.
    """

    via: Optional[Transport] = None
    user: Optional[str] = None
    port: int = 22
    identity_file: Optional[str] = None
    connect_timeout: int = 15
    staging_dir: str = "/tmp"

    @property
    def target(self):
        """:return: ``user@host`` as seen from the intermediate host."""
        return "%s@%s" % (self.user, self.name) if self.user else self.name

    def ssh_options(self, *, for_scp=False):
        """:return: ssh options for the second hop, evaluated on the intermediate host."""
        opts = list(DEFAULT_SSH_OPTIONS)
        opts += ["-o", "ConnectTimeout=%d" % self.connect_timeout]
        if self.identity_file:
            opts += ["-o", "IdentitiesOnly=yes", "-i", self.identity_file]
        if self.port and int(self.port) != 22:
            opts += ["-P" if for_scp else "-p", str(self.port)]
        return opts

    def run(self, argv, *, check=True, timeout=None, input=None):  # noqa: A002 - matches spec
        argv = [str(a) for a in argv]
        hop = ["ssh"] + self.ssh_options() + ["-T", self.target, shlex.join(argv)]
        result = self.via.run(hop, check=False, timeout=timeout, input=input)
        result = Result(argv, result.returncode, result.stdout, result.stderr, self.name)
        return result.check() if check else result

    def _staged(self, name):
        return posixpath.join(self.staging_dir, "dtr-%s-%s" % (uuid.uuid4().hex[:8], name))

    def upload(self, local_path, remote_path, *, mode=None):
        staged = self._staged(Path(local_path).name)
        self.via.upload(local_path, staged)
        argv = ["scp"] + self.ssh_options(for_scp=True) + [staged,
                                                           "%s:%s" % (self.target, remote_path)]
        try:
            self.via.run(argv).check()
        finally:
            self.via.run(["rm", "-f", "--", staged], check=False)
        if mode is not None:
            self.run(["chmod", "%o" % mode, remote_path]).check()

    def download(self, remote_path, local_path):
        staged = self._staged(posixpath.basename(remote_path))
        argv = ["scp"] + self.ssh_options(for_scp=True) + ["%s:%s" % (self.target, remote_path),
                                                           staged]
        self.via.run(argv).check()
        try:
            self.via.download(staged, local_path)
        finally:
            self.via.run(["rm", "-f", "--", staged], check=False)

    def upload_dir(self, local_dir, remote_dir, *, excludes=(), delete=False,
                   on_progress=None):
        del on_progress  # the interesting hop is the second one, which scp cannot watch
        with tempfile.TemporaryDirectory() as tmp:
            archive = Path(tmp) / "payload.tar.gz"
            make_tarball(local_dir, archive, excludes=excludes)
            staged = self._staged("payload.tar.gz")
            self.via.upload(archive, staged)
            try:
                self.push_archive(staged, remote_dir, delete=delete)
            finally:
                self.via.run(["rm", "-f", "--", staged], check=False)

    def push_archive(self, staged_on_via, remote_dir, *, delete=False):
        """Send an archive that already sits on the intermediate host to this host."""
        remote_tmp = self._staged("payload.tar.gz")
        argv = ["scp"] + self.ssh_options(for_scp=True) + [
            staged_on_via, "%s:%s" % (self.target, remote_tmp)]
        self.via.run(argv).check()
        script = "set -eu\nmkdir -p %s\n" % shlex.quote(remote_dir)
        if delete:
            script += "rm -rf -- %s/*\n" % shlex.quote(remote_dir)
        script += "tar -xzf %s -C %s\nrm -f -- %s\n" % (
            shlex.quote(remote_tmp), shlex.quote(remote_dir), shlex.quote(remote_tmp))
        self.run_script(script).check()


def make_tarball(source_dir, archive_path, *, excludes=()):
    """Create a gzip tarball of the *contents* of ``source_dir``, honouring rsync-ish excludes."""
    source = Path(source_dir)
    with tarfile.open(archive_path, "w:gz") as tar:
        for entry in sorted(source.rglob("*")):
            if entry.is_dir():
                continue
            rel = entry.relative_to(source)
            if is_excluded(rel, excludes):
                continue
            tar.add(str(entry), arcname=rel.as_posix())


def is_excluded(rel_path, excludes):
    """
    :return: True when ``rel_path`` matches one of the rsync-style ``excludes``.

    A pattern matches a whole relative path, any prefix of it, or any single path
    component, so ``.git/``, ``*.pyc`` and ``target/`` all behave as expected.
    """
    rel = Path(rel_path)
    posix = rel.as_posix()
    for raw in excludes:
        pattern = raw.rstrip("/")
        if not pattern:
            continue
        if fnmatch(posix, pattern) or fnmatch(posix, pattern + "/*"):
            return True
        if any(fnmatch(part, pattern) for part in rel.parts):
            return True
    return False


def _exclude_to_glob(exclude):
    return exclude.rstrip("/") or exclude


def _spawn(argv, *, host, check=True, timeout=None, input=None):  # noqa: A002 - matches spec
    payload = input.encode("utf-8") if isinstance(input, str) else input
    try:
        completed = subprocess.run(  # noqa: S603 - argv is always a list, never a shell string
            argv, input=payload, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            timeout=timeout, check=False)
    except FileNotFoundError as ex:
        raise TransportError("%s: %s" % (argv[0], ex)) from ex
    except subprocess.TimeoutExpired as ex:
        raise TransportError("timed out after %ss on %s: %s" % (timeout, host, shlex.join(argv))) from ex
    result = Result(argv, completed.returncode,
                    _decode(completed.stdout), _decode(completed.stderr), host)
    if check:
        result.check()
    return result


def _decode(raw):
    if raw is None:
        return ""
    return raw.decode("utf-8", errors="replace")


def run_local(argv, *, check=False, timeout=None, input=None):  # noqa: A002 - matches spec
    """Run a command on the coordinator itself, outside any transport (probes only)."""
    return _spawn([str(a) for a in argv], host="local", check=check, timeout=timeout, input=input)


CHUNK = 1024 * 1024

# `--info=progress2`: `      1,234,567  35%   12.34MB/s    0:00:12`.  The separators are
# locale-dependent, the layout is not.
_RSYNC_PROGRESS = re.compile(r"^\s*([\d,.]+)\s+(\d+)%\s")


def parse_rsync_progress(line):
    """:return: ``(bytes_so_far, fraction)`` from one ``--info=progress2`` line, or None."""
    match = _RSYNC_PROGRESS.match(line or "")
    if not match:
        return None
    try:
        return int(re.sub(r"[,.]", "", match.group(1))), int(match.group(2)) / 100.0
    except ValueError:
        return None


def rsync_reporter(on_progress):
    """:return: an ``on_output`` callback that turns rsync's meter into ``on_progress``."""
    def watch(line):
        reading = parse_rsync_progress(line)
        if reading:
            on_progress(reading[0], reading[1])
    return watch


def run_local_streaming(argv, *, on_output, host="local"):
    """
    Run ``argv`` locally, handing each output line to ``on_output`` as it appears.

    :return: a :class:`Result` holding the whole output, so a caller can still parse a
        summary the command printed at the end.

    Progress meters separate their updates with carriage returns rather than newlines,
    so both count as line endings here.  stderr is drained by a thread: a command that
    fills the stderr pipe while nobody reads it deadlocks, and rsync is chatty on stderr
    when a host misbehaves.
    """
    argv = [str(a) for a in argv]
    try:
        # pylint: disable=consider-using-with
        process = subprocess.Popen(  # noqa: S603 - argv is always a list, never a string
            argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0)
    except FileNotFoundError as ex:
        raise TransportError("%s: %s" % (argv[0], ex)) from ex

    errors = []
    drain = threading.Thread(target=lambda: errors.append(_decode(process.stderr.read())),
                             daemon=True)
    drain.start()

    collected = []
    pending = ""
    while True:
        block = process.stdout.read(4096)
        if not block:
            break
        text = _decode(block)
        collected.append(text)
        pending += text
        pending = _emit_lines(pending, on_output)
    if pending.strip():
        on_output(pending.strip())

    process.wait()
    drain.join(timeout=5)
    return Result(argv, process.returncode, "".join(collected),
                  errors[0] if errors else "", host)


def _emit_lines(pending, on_output):
    """:return: the unterminated tail; every complete line goes to ``on_output``."""
    pending = pending.replace("\r\n", "\n")
    while True:
        cut = min((i for i in (pending.find("\n"), pending.find("\r")) if i >= 0), default=-1)
        if cut < 0:
            return pending
        line, pending = pending[:cut], pending[cut + 1:]
        if line.strip():
            on_output(line.strip())


def _stream_to_stdin(argv, local_path, on_bytes, *, host):
    """Feed a file into ``argv``'s stdin in chunks, reporting the running total."""
    try:
        # pylint: disable=consider-using-with
        process = subprocess.Popen(  # noqa: S603 - argv is always a list, never a string
            argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except FileNotFoundError as ex:
        raise TransportError("%s: %s" % (argv[0], ex)) from ex

    sent = 0
    try:
        with open(local_path, "rb") as handle:
            for block in iter(lambda: handle.read(CHUNK), b""):
                process.stdin.write(block)
                sent += len(block)
                on_bytes(sent)
        process.stdin.close()
    except (BrokenPipeError, OSError):
        # The far end died mid-transfer; its stderr says why, so fall through to wait().
        pass
    _, stderr = process.communicate()
    result = Result(argv, process.returncode, "", _decode(stderr), host)
    return result.check()


def build_transport(host: str, *, user=None, port=22, identity_file=None, connect_timeout=15,
                    dry_run=False, verbose=False, printer=None) -> Transport:
    """:return: a :class:`LocalTransport` for ``local``, otherwise an :class:`SshTransport`."""
    if host in (None, "", "local", "localhost"):
        return LocalTransport(dry_run=dry_run, verbose=verbose, printer=printer)
    return SshTransport(name=host, user=user, port=int(port or 22), identity_file=identity_file,
                        connect_timeout=connect_timeout, dry_run=dry_run, verbose=verbose,
                        printer=printer)


def quote_all(values: Sequence[Union[str, Path]]) -> str:
    """:return: the values shell-quoted and space joined."""
    return " ".join(shlex.quote(str(v)) for v in values)
