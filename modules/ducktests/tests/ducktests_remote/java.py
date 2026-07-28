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
Choosing the JVM the workers run the tests under.

**Why PATH matters more than JAVA_HOME here.**  ``ignitetest`` reaches a JVM four
different ways, and only one of them respects ``JAVA_HOME``:

===================================================  ==========================
consumer                                             mechanism
===================================================  ==========================
``ignite.sh``, via ``IgniteSpec.envs()``             honours ``JAVA_HOME``
``jvm_utils.java_version()`` -> ``java -version``    bare ``java``, so PATH
``services/kafka/kafka.py`` -> ``nohup java ...``    bare ``java``, so PATH
``jmx_utils`` -> ``java -jar jmxterm.jar``           bare ``java``, so PATH
===================================================  ==========================

All four run over *non-interactive* ssh, where ``~/.profile`` is never sourced.  Setting
``JAVA_HOME`` alone therefore changes what ``ignite.sh`` uses and nothing else; what the
rest of the suite gets is decided by the non-interactive PATH.  Both are written, and the
result is then verified over a fresh connection rather than assumed - see
:func:`env_script` and :func:`verify_script`.

The resolution ladder, in :func:`discovery_script` (rungs 1-3, pure discovery, safe to run
from ``doctor``) and in ``commands/provision.py`` (rungs 4-5, which mutate):

1. ``java.home`` is set - verify it, and fail naming the host when it is not there.
   Explicit means explicit; falling back would defeat the point of saying it.
2. the JVM the non-interactive shell already provides matches ``java.major`` - use it.
3. a JDK of the right major exists under one of ``java.search_paths`` - use it.
4. ``java.archive`` is set - deliver it (``commands/provision.py``).
5. otherwise fail, listing every JDK that *was* found.
"""

import posixpath
import shlex
import tarfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

from ducktests_remote.config import ConfigError, expand_path

BLOCK_BEGIN = "# BEGIN ducktests-remote"
BLOCK_END = "# END ducktests-remote"

# Sources, in the order the ladder tries them.
EXPLICIT = "explicit"
CURRENT = "current"
SEARCH = "search"
DELIVERED = "delivered"
NONE = "none"

_TAR_SUFFIXES = (".tar.gz", ".tgz", ".tar", ".tar.bz2", ".tbz2", ".tar.xz", ".txz")


@dataclass
class JavaConfig:
    """The ``java`` config section, with the cluster-level fallbacks already applied."""

    major: Optional[int] = None
    home: Optional[str] = None
    search_paths: List[str] = field(default_factory=list)
    archive: Optional[str] = None
    install_root: str = "/opt"
    name: Optional[str] = None
    ssh_environment: bool = True
    bashrc: bool = True


@dataclass
class Resolution:
    """What one worker reported, and what should be done about it."""

    host: str = "-"
    home: Optional[str] = None          # the JDK the ladder selected
    version: str = ""
    major: Optional[int] = None
    source: str = NONE
    path_java: Optional[str] = None     # what bare `java` resolves to right now
    path_real: Optional[str] = None     # ... with symlinks resolved
    path_version: str = ""
    path_major: Optional[int] = None
    env_home: Optional[str] = None      # $JAVA_HOME as the non-interactive shell sees it
    candidates: List[tuple] = field(default_factory=list)   # [(home, major, version)]

    @property
    def selected(self):
        """:return: True when the ladder found a JDK of the requested major."""
        return bool(self.home)

    def path_matches(self, requested_major):
        """
        :return: True when the JVM the tests will actually get has the requested major.

        This, not :attr:`selected`, is the question ``doctor`` has to answer: a perfect
        JDK sitting in ``/opt`` that the non-interactive PATH does not point at is not the
        JVM the suite will run under.
        """
        if self.path_major is None:
            return False
        return requested_major is None or self.path_major == requested_major

    @property
    def home_in_effect(self):
        """
        :return: True when the JDK on PATH is the one the ladder selected.

        Compared on the resolved path, so ``/usr/bin/java`` symlinked into the selected
        home counts as a match.  A same-major JDK from somewhere else does not, which is
        worth a warning but not a failure - the tests still get the version they asked
        for.
        """
        return _same_home(self.path_real or self.path_java, self.home)

    def summary(self):
        """:return: a short human description of the selected JDK."""
        if not self.selected:
            return "no JDK of the requested version"
        return "%s (%s, found by %s)" % (self.home, self.version or "unknown version",
                                         self.source)


def config_of(ctx):
    """:return: the :class:`JavaConfig` for this context, cluster fallbacks applied."""
    section = ctx.config.get("java") or {}
    major = section.get("major")
    if major is not None:
        try:
            major = int(major)
        except (TypeError, ValueError) as ex:
            raise ConfigError("java.major must be a whole number, found %r" % (major,)) from ex
    search = section.get("search_paths") or []
    if isinstance(search, str):
        search = [search]
    return JavaConfig(
        major=major,
        home=section.get("home") or None,
        search_paths=[str(p) for p in search],
        archive=section.get("archive") or None,
        install_root=(section.get("install_root")
                      or ctx.cluster_cfg.get("install_root") or "/opt"),
        name=section.get("name") or None,
        ssh_environment=bool(section.get("ssh_environment", True)),
        bashrc=bool(section.get("bashrc", True)))


def major_of(version):
    """
    :return: the major version of a Java version string, or None.

    Mirrors ``ignitetest.services.utils.jvm_utils.java_major_version``: ``1.8.0_292`` is
    8, ``11.0.19`` is 11, ``17.0.11+9`` is 17.
    """
    text = str(version or "").strip().strip('"')
    if not text:
        return None
    parts = text.split(".")
    chosen = parts[1] if parts[0] == "1" and len(parts) > 1 else parts[0]
    digits = ""
    for char in chosen:
        if not char.isdigit():
            break
        digits += char
    return int(digits) if digits else None


def discovery_script(cfg: JavaConfig):
    """
    :return: a POSIX shell script that reports the JDK situation on one host.

    It only ever *reads*, which is what makes it safe to run from ``doctor`` and from
    ``provision --only ssh-env`` as well as from the ``jdk`` step, and it always exits 0:
    the status travels in the fields, so one unusable host cannot abort a fan-out.
    """
    return _DISCOVERY % {
        "requested": shlex.quote(str(cfg.major) if cfg.major else ""),
        "explicit": shlex.quote(cfg.home or ""),
        "search": " ".join(shlex.quote(p) for p in cfg.search_paths),
    }


_DISCOVERY = r"""
set -u
say() { printf '%%s=%%s\n' "$1" "$2"; }
requested=%(requested)s
explicit=%(explicit)s

jv_version() { "$1" -version 2>&1 | head -n1 | sed -n 's/.*version "\([^"]*\)".*/\1/p'; }
jv_major() {
  case "$1" in
    "") printf '' ;;
    1.*) printf '%%s' "$1" | cut -d. -f2 | tr -cd '0-9' ;;
    *) printf '%%s' "$1" | cut -d. -f1 | tr -cd '0-9' ;;
  esac
}
home_of() {
  p=$(readlink -f "$1" 2>/dev/null || printf '%%s' "$1")
  dirname "$(dirname "$p")"
}

cands=""
add_cand() {
  [ -n "$1" ] || return 0
  [ -x "$1/bin/java" ] || return 0
  for seen in $cands; do [ "$seen" = "$1" ] && return 0; done
  cands="$cands $1"
}

say java_requested "$requested"

# What the tests will actually get: bare `java`, over non-interactive ssh.
cur=$(command -v java 2>/dev/null || true)
say java "$(java -version 2>&1 | head -n1 | tr -d '\r' || echo missing)"
if [ -n "$cur" ]; then
  cv=$(jv_version "$cur"); say java_path "$cur"; say java_path_version "$cv"
  say java_path_major "$(jv_major "$cv")"
  say java_path_real "$(readlink -f "$cur" 2>/dev/null || printf '%%s' "$cur")"
  add_cand "$(home_of "$cur")"
fi
say java_env_home "${JAVA_HOME:-}"
add_cand "${JAVA_HOME:-}"
add_cand "$explicit"

for root in %(search)s; do
  add_cand "$root"
  for entry in "$root"/*; do add_cand "$entry"; done
done

report=""
for c in $cands; do
  v=$(jv_version "$c/bin/java")
  m=$(jv_major "$v")
  [ -n "$m" ] || continue
  report="$report,$c:$m:$v"
done
say java_candidates "${report#,}"
exit 0
"""


def parse_probe(host, text, cfg: JavaConfig):
    """:return: :func:`parse_facts` applied to one host's raw discovery output."""
    return parse_facts(host, _parse_kv(text), cfg)


def parse_facts(host, facts, cfg: JavaConfig):
    """
    Turn one host's discovery facts into a :class:`Resolution`.

    The selection happens here rather than in the shell so that it is unit-testable and
    identical for every caller.  ``doctor`` already parses the worker probe into a dict,
    which is why this takes facts and :func:`parse_probe` takes text.
    """
    res = Resolution(host=host)
    res.path_java = facts.get("java_path") or None
    res.path_real = facts.get("java_path_real") or None
    res.path_version = facts.get("java_path_version") or ""
    res.path_major = _int_or_none(facts.get("java_path_major"))
    res.env_home = facts.get("java_env_home") or None
    res.candidates = _parse_candidates(facts.get("java_candidates"))

    versions = {home: version for home, _, version in res.candidates}
    majors = {home: major for home, major, _ in res.candidates}

    if cfg.home:
        if majors.get(cfg.home) is None:
            return res       # explicit home absent or unusable: the caller fails hard
        res.home, res.major, res.version = cfg.home, majors[cfg.home], versions[cfg.home]
        res.source = EXPLICIT
        return res

    wanted = cfg.major
    current_home = _home_of(res.path_real or res.path_java) if res.path_java else None
    if wanted is None:
        # No requested version: whatever the host already provides is the answer.
        if current_home and res.path_major is not None:
            res.home = current_home
            res.major, res.version, res.source = res.path_major, res.path_version, CURRENT
        return res

    if res.path_major == wanted and current_home:
        res.home = current_home
        res.major, res.version, res.source = wanted, res.path_version, CURRENT
        return res

    matching = [(home, version) for home, major, version in res.candidates if major == wanted]
    if matching:
        # Highest patch level wins, compared numerically: sorting the strings would put
        # 17.0.9 above 17.0.11.  The path breaks ties, so the choice is deterministic.
        res.home = max(matching, key=lambda item: (version_key(item[1]), item[0]))[0]
        res.major, res.version, res.source = wanted, versions[res.home], SEARCH
    return res


def version_key(version):
    """:return: a numeric tuple for ordering Java version strings (17.0.9 < 17.0.11)."""
    numbers = []
    current = ""
    for char in str(version or ""):
        if char.isdigit():
            current += char
        elif current:
            numbers.append(int(current))
            current = ""
    if current:
        numbers.append(int(current))
    return tuple(numbers)


def env_script(cfg: JavaConfig, java_home, path_extra=()):
    """
    :return: a script writing ``JAVA_HOME``/``PATH`` where non-interactive ssh will see it.

    Two mechanisms, written from the same resolved value in the same step so they cannot
    drift apart:

    ``~/.ssh/environment``
        what the Dockerfile does, and silently ignored unless sshd carries
        ``PermitUserEnvironment yes``, which no site guarantees.

    ``~/.bashrc``
        a marked block at the very *top* of the file, above the
        ``case $- in *i*) ;; *) return;; esac`` guard that the stock Ubuntu file opens
        with - that guard exists precisely because bash does source ``~/.bashrc`` for
        non-interactive ssh commands.  It does nothing when the login shell is not bash.

    Neither is trusted afterwards: :func:`verify_script` re-asks over a fresh connection.
    """
    if not (cfg.ssh_environment or cfg.bashrc):
        raise ConfigError("java.ssh_environment and java.bashrc are both false, so there "
                          "is no way to put the JDK on the workers' non-interactive PATH")
    fallback = ("echo \"NOTE the ~/.bashrc block below is what will carry JAVA_HOME.\""
                if cfg.bashrc else
                "echo \"NOTE java.bashrc is off, so nothing else will carry it: ask for\"\n"
                "    echo \"NOTE PermitUserEnvironment, or name a JDK already on PATH.\"")
    body = _ENV_HEAD % {"home": shlex.quote(str(java_home))}
    if cfg.ssh_environment:
        body += _ENV_SSH % {"extra": "".join(":%s" % p for p in (path_extra or [])),
                            "fallback": fallback}
    if cfg.bashrc:
        body += _ENV_BASHRC % {"begin": BLOCK_BEGIN, "end": BLOCK_END}
    return body + _ENV_TAIL


_ENV_HEAD = r"""
set -u
jh=%(home)s
[ -x "$jh/bin/java" ] || { echo "no java under $jh" >&2; exit 1; }
changed=0
"""

_ENV_SSH = r"""
mkdir -p ~/.ssh
chmod 700 ~/.ssh
touch ~/.ssh/environment
chmod 600 ~/.ssh/environment
# Drop the entries we are about to prepend before reading $PATH back, or a host that
# honours ~/.ssh/environment would grow one more copy of $jh/bin on every run and this
# step would report CHANGED for ever.
base_path=$(printf '%%s' "$PATH" | awk -v RS=: -v strip="$jh/bin%(extra)s" '
  BEGIN { ORS=""; n = split(strip, s, ":") }
  {
    keep = ($0 != "")
    for (i = 1; i <= n; i++) if ($0 == s[i]) keep = 0
    if (keep) { if (out++) printf ":"; printf "%%s", $0 }
  }')
want_path="PATH=$jh/bin:$base_path%(extra)s"
want_home="JAVA_HOME=$jh"
for line in "$want_path" "$want_home" "LANG=C.UTF-8"; do
  key=${line%%%%=*}
  if grep -q "^$key=" ~/.ssh/environment 2>/dev/null; then
    current=$(grep "^$key=" ~/.ssh/environment | head -n1)
    [ "$current" = "$line" ] && continue
    grep -v "^$key=" ~/.ssh/environment > ~/.ssh/environment.tmp || true
    mv ~/.ssh/environment.tmp ~/.ssh/environment
  fi
  printf '%%s\n' "$line" >> ~/.ssh/environment
  changed=1
done
chmod 600 ~/.ssh/environment
[ "$changed" -eq 1 ] && echo "CHANGED wrote ~/.ssh/environment"
if ! grep -qi '^ *PermitUserEnvironment *yes' /etc/ssh/sshd_config 2>/dev/null; then
    echo "NOTE sshd has no 'PermitUserEnvironment yes'; ~/.ssh/environment is ignored."
    %(fallback)s
fi
"""

_ENV_BASHRC = r"""
# The block goes at the TOP, above the `case $- in *i*)` early return that the stock
# ~/.bashrc opens with, or it would never run for a non-interactive ssh command.
tmp=$(mktemp)
{
  printf '%%s\n' "%(begin)s"
  printf '%%s\n' "export JAVA_HOME=$jh"
  printf '%%s\n' 'export PATH="$JAVA_HOME/bin:$PATH"'
  printf '%%s\n' "%(end)s"
} > "$tmp"
if [ -f ~/.bashrc ]; then
  awk 'BEGIN{skip=0}
       /^# BEGIN ducktests-remote$/{skip=1; next}
       /^# END ducktests-remote$/{skip=0; next}
       skip==0{print}' ~/.bashrc >> "$tmp"
fi
if cmp -s "$tmp" ~/.bashrc 2>/dev/null; then
  rm -f "$tmp"
else
  mv "$tmp" ~/.bashrc
  changed=1
  echo "CHANGED wrote the ducktests-remote block in ~/.bashrc"
fi
"""

_ENV_TAIL = r"""
if [ "$changed" -eq 0 ]; then echo "up to date"; fi
exit 0
"""


def verify_script():
    """
    :return: a script reporting what a *fresh* non-interactive session actually gets.

    This is the authority.  ``~/.ssh/environment`` and ``~/.bashrc`` are both best effort,
    and the failure they exist to prevent - ``java`` missing three hours into a run - is
    only really excluded by asking the way ducktape will ask.
    """
    return r"""
set -u
say() { printf '%s=%s\n' "$1" "$2"; }
jv_version() { "$1" -version 2>&1 | head -n1 | sed -n 's/.*version "\([^"]*\)".*/\1/p'; }
jv_major() {
  case "$1" in
    "") printf '' ;;
    1.*) printf '%s' "$1" | cut -d. -f2 | tr -cd '0-9' ;;
    *) printf '%s' "$1" | cut -d. -f1 | tr -cd '0-9' ;;
  esac
}
say java "$(java -version 2>&1 | head -n1 | tr -d '\r' || echo missing)"
say java_env_home "${JAVA_HOME:-}"
cur=$(command -v java 2>/dev/null || true)
if [ -n "$cur" ]; then
  cv=$(jv_version "$cur")
  say java_path "$cur"
  say java_path_version "$cv"
  say java_path_major "$(jv_major "$cv")"
  say java_path_real "$(readlink -f "$cur" 2>/dev/null || printf '%s' "$cur")"
fi
exit 0
"""


@dataclass
class ArchivePlan:
    """A coordinator-side JDK archive or directory, inspected before anything is sent."""

    path: Path
    kind: str                       # "tar" or "dir"
    top_level: Optional[str]        # single top-level directory inside a tarball
    strip: int                      # --strip-components for tar
    bytes: int
    name: str                       # default target directory name


def archive_plan(archive, name=None):
    """
    Inspect ``java.archive`` on the coordinator.

    Reading the member list with :mod:`tarfile` rather than guessing in the shell is what
    lets a bad archive fail *before* it is copied to every host: a Temurin tarball unpacks
    into a single ``jdk-17.0.11+9/`` directory, and an archive with no ``bin/java`` under
    it (a macOS build, with ``Contents/Home``) is worth catching here.
    """
    path = Path(expand_path(archive))
    if not path.exists():
        raise ConfigError("java.archive %s does not exist on this machine" % path)

    if path.is_dir():
        if not (path / "bin" / "java").exists():
            raise ConfigError("java.archive %s has no bin/java; point it at a JDK home"
                              % path)
        total = sum(f.stat().st_size for f in path.rglob("*") if f.is_file())
        return ArchivePlan(path=path, kind="dir", top_level=None, strip=0, bytes=total,
                           name=name or path.name)

    lowered = path.name.lower()
    if lowered.endswith(".zip"):
        raise ConfigError(
            "java.archive %s is a zip; ducktests-remote unpacks .tar.gz/.tgz/.tar only. "
            "Linux JDKs ship as tarballs, and an untested zip branch on every run is "
            "worse than this message." % path)
    if not lowered.endswith(_TAR_SUFFIXES):
        raise ConfigError("java.archive %s is neither a directory nor a tar archive"
                          % path)

    try:
        with tarfile.open(path) as tar:
            members = tar.getmembers()
    except (tarfile.TarError, OSError) as ex:
        raise ConfigError("java.archive %s could not be read: %s" % (path, ex)) from ex

    names = [m.name.lstrip("./") for m in members if m.name not in (".", "./")]
    if not names:
        raise ConfigError("java.archive %s is empty" % path)
    total = sum(m.size for m in members if m.isfile())
    tops = {n.split("/", 1)[0] for n in names if n}

    if len(tops) == 1:
        top = tops.pop()
        _require_java(names, "%s/bin/java" % top, path)
        return ArchivePlan(path=path, kind="tar", top_level=top, strip=1, bytes=total,
                           name=name or top)
    _require_java(names, "bin/java", path)
    return ArchivePlan(path=path, kind="tar", top_level=None, strip=0, bytes=total,
                       name=name or _strip_suffix(path.name))


def _require_java(names, expected, path):
    if expected not in names:
        raise ConfigError(
            "java.archive %s does not contain %s. A JDK for Linux unpacks with bin/java "
            "directly under its top-level directory; a macOS build has Contents/Home in "
            "between and cannot be used here." % (path, expected))


def target_dir(cfg: JavaConfig, plan: ArchivePlan):
    """:return: where ``plan`` is installed on a worker."""
    return posixpath.join(cfg.install_root, cfg.name or plan.name)


def _strip_suffix(filename):
    for suffix in _TAR_SUFFIXES:
        if filename.lower().endswith(suffix):
            return filename[: -len(suffix)]
    return filename


def _home_of(java_binary):
    return posixpath.dirname(posixpath.dirname(str(java_binary)))


def _same_home(java_binary, home):
    if not java_binary or not home:
        return False
    return _home_of(java_binary).rstrip("/") == str(home).rstrip("/")


def _parse_candidates(raw):
    out = []
    for item in (raw or "").split(","):
        if not item.strip():
            continue
        parts = item.split(":")
        if len(parts) < 2:
            continue
        major = _int_or_none(parts[1])
        if major is None:
            continue
        out.append((parts[0], major, parts[2] if len(parts) > 2 else ""))
    return out


def _parse_kv(text):
    fields = {}
    for line in (text or "").split("\n"):
        if "=" in line:
            key, value = line.split("=", 1)
            fields[key.strip()] = value.strip()
    return fields


def _int_or_none(value):
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None
