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
SSH failure classification and the "what to ask your administrator" block.

The common first-time experience is not a subtle bug, it is *"nobody has added me to
these machines yet"*.  A raw ``Permission denied (publickey)`` repeated twelve times
tells the operator nothing about what to ask for, so every SSH failure is classified and
mapped to a concrete next action, and the whole inventory is always probed before
anything is reported.

The patterns below are derived from OpenSSH's own message strings.  They have not been
replayed against every target distribution's build, so treat the table as data: adding a
distro-specific string is a one-line change and ``checks/check_remote_sshdiag.py`` is
table-driven over recorded samples.
"""

import re
import shutil
from dataclasses import dataclass
from typing import List, Optional

from ducktests_remote.transport import Result, TransportError, run_local

OK = "ok"
UNRESOLVED = "unresolved"
NO_SSHD = "no-sshd"
UNREACHABLE = "unreachable"
NO_ACCESS = "no-access"
NO_USER = "no-user"
HOSTKEY = "hostkey"
NO_SUDO = "no-sudo"
UNKNOWN = "unknown"

# Ordered: the first pattern that matches wins, so the specific ones come first.
PATTERNS = (
    (HOSTKEY, r"REMOTE HOST IDENTIFICATION HAS CHANGED|Host key verification failed|"
              r"host key .* has changed"),
    (UNRESOLVED, r"Could not resolve hostname|Name or service not known|"
                 r"nodename nor servname provided|Temporary failure in name resolution|"
                 r"Name does not resolve"),
    (NO_SSHD, r"Connection refused|port \d+: Connection refused"),
    (UNREACHABLE, r"Connection timed out|Operation timed out|No route to host|"
                  r"Network is unreachable|Host is unreachable|"
                  r"kex_exchange_identification: Connection closed"),
    (NO_USER, r"Invalid user|no such user|Please login as the user|"
              r"This account is currently not available"),
    (NO_ACCESS, r"Permission denied \(publickey|Permission denied \(.*publickey|"
                r"Too many authentication failures|no matching host key type found|"
                r"Authentication failed"),
    (NO_SUDO, r"sudo: a (password|terminal) is required|sudo: no tty present|"
              r"a terminal is required to read the password|"
              r"is not in the sudoers file"),
)

_ADVICE = {
    UNRESOLVED: "hostname does not resolve from here. Check VPN/DNS, or put the IP in "
                "cluster.nodes[].ip.",
    NO_SSHD: "host answers but nothing is listening on port {port}; sshd is down or on "
             "another port.",
    UNREACHABLE: "no network path to {host}:{port}. Firewall, routing or the host is down.",
    NO_ACCESS: "your key is not authorised for user {user!r} on this host.",
    NO_USER: "account {user!r} does not exist on this host.",
    HOSTKEY: "the host key changed. Inspect it, then remove the stale line yourself; this "
             "tool will never do it for you.",
    NO_SUDO: "passwordless sudo is missing for {user!r}.",
    UNKNOWN: "unrecognised ssh failure; rerun with -v for the full stderr.",
}


@dataclass
class SshDiagnosis:
    """One host's SSH state."""

    host: str
    classification: str = OK
    user: Optional[str] = None
    port: int = 22
    stderr: str = ""
    returncode: int = 0

    @property
    def ok(self):
        """:return: True when the connection succeeded."""
        return self.classification == OK

    @property
    def advice(self):
        """:return: a one-line, host-specific next action."""
        template = _ADVICE.get(self.classification, _ADVICE[UNKNOWN])
        return template.format(host=self.host, user=self.user, port=self.port)


def classify(returncode, stderr):
    """
    Map an ssh exit status plus stderr onto one of the classification constants.

    :param returncode: process exit status; 0 always means ``ok``.
    :param stderr: captured stderr, matched case-insensitively.
    """
    if returncode == 0:
        return OK
    text = stderr or ""
    for name, pattern in PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            return name
    return UNKNOWN


def diagnose(result: Result, *, host, user=None, port=22):
    """:return: an :class:`SshDiagnosis` built from a command :class:`Result`."""
    return SshDiagnosis(host=host,
                        classification=classify(result.returncode, result.stderr),
                        user=user, port=port,
                        stderr=(result.stderr or "").strip(),
                        returncode=result.returncode)


def diagnose_exception(ex, *, host, user=None, port=22):
    """:return: an :class:`SshDiagnosis` built from a raised transport error."""
    stderr = str(ex)
    result = getattr(ex, "result", None)
    if isinstance(result, Result):
        return diagnose(result, host=host, user=user, port=port)
    return SshDiagnosis(host=host, classification=classify(255, stderr), user=user, port=port,
                        stderr=stderr, returncode=255)


def key_fingerprint(identity_file):
    """
    :return: the fingerprint of the public key being offered, or None.

    The administrator needs this to confirm they are authorising the right key, and the
    operator usually cannot produce it from memory.
    """
    if not identity_file:
        return None
    keygen = shutil.which("ssh-keygen")
    if not keygen:
        return None
    for candidate in (str(identity_file) + ".pub", str(identity_file)):
        try:
            result = run_local([keygen, "-l", "-f", candidate])
        except TransportError:
            continue
        if result.ok and result.out:
            return result.out
    return None


def public_key(identity_file):
    """:return: the contents of ``<identity_file>.pub`` when readable, else None."""
    if not identity_file:
        return None
    try:
        with open(str(identity_file) + ".pub", "r", encoding="utf-8") as handle:
            return handle.read().strip()
    except OSError:
        return None


def summarise(diagnoses: List[SshDiagnosis]):
    """:return: ``9 ok, 2 no-access, 1 no-user``."""
    counts = {}
    for diagnosis in diagnoses:
        counts[diagnosis.classification] = counts.get(diagnosis.classification, 0) + 1
    ordered = sorted(counts.items(), key=lambda kv: (kv[0] != OK, kv[0]))
    return ", ".join("%d %s" % (count, name) for name, count in ordered)


def admin_request_block(diagnoses: List[SshDiagnosis], *, user, identity_file=None):
    """
    Render the copy-pasteable block the operator forwards to whoever owns the machines.

    This block is the point of the whole classification exercise; the table above is
    plumbing.  It names the hosts, the account, the key, and the exact line to append.
    """
    failures = [d for d in diagnoses if not d.ok]
    if not failures:
        return ""

    by_class = {}
    for diagnosis in failures:
        by_class.setdefault(diagnosis.classification, []).append(diagnosis.host)

    lines = ["", "=" * 72,
             "WHAT TO ASK YOUR ADMINISTRATOR",
             "=" * 72,
             "%d of %d hosts are not usable. Summary: %s"
             % (len(failures), len(diagnoses), summarise(diagnoses)),
             ""]

    fingerprint = key_fingerprint(identity_file)
    pubkey = public_key(identity_file)

    if NO_ACCESS in by_class:
        hosts = by_class[NO_ACCESS]
        lines += ["Please authorise my SSH key for the account %r on these %d host(s):"
                  % (user, len(hosts)),
                  _host_list(hosts), ""]
        if fingerprint:
            lines += ["  key fingerprint offered: %s" % fingerprint]
        lines += ["  append this line to ~%s/.ssh/authorized_keys on each host:" % user,
                  "    %s" % (pubkey or "<contents of %s.pub>" % (identity_file or "your key")),
                  ""]

    if NO_USER in by_class:
        hosts = by_class[NO_USER]
        working = [d.host for d in diagnoses if d.ok]
        lines += ["The account %r does not exist on these %d host(s):" % (user, len(hosts)),
                  _host_list(hosts)]
        if working:
            lines += ["  (it does exist on: %s)" % ", ".join(sorted(working)[:10])]
        lines += ["  please create it, or tell me which account to use instead.", ""]

    if NO_SSHD in by_class:
        lines += ["sshd is not listening on these host(s):",
                  _host_list(by_class[NO_SSHD]), ""]

    if UNREACHABLE in by_class:
        lines += ["No network path to these host(s) on port %d; please check the firewall:"
                  % (failures[0].port or 22),
                  _host_list(by_class[UNREACHABLE]), ""]

    if UNRESOLVED in by_class:
        lines += ["These hostnames do not resolve from my machine. Either grant DNS/VPN "
                  "access, or give me their IP addresses:",
                  _host_list(by_class[UNRESOLVED]), ""]

    if NO_SUDO in by_class:
        lines += ["Passwordless sudo is missing for %r on these host(s):" % user,
                  _host_list(by_class[NO_SUDO]),
                  "  It is needed only by the network-segmentation suites",
                  "  (ignitetest/tests/discovery_test.py, "
                  "ignitetest/tests/cellular_affinity_test.py), which call",
                  "  `sudo iptables` from IgniteAwareService.drop_network. Every other test "
                  "runs unprivileged.",
                  "  Required line: %s ALL=(ALL) NOPASSWD: /usr/sbin/iptables, "
                  "/usr/sbin/iptables-save, /usr/sbin/iptables-restore" % user,
                  ""]

    if HOSTKEY in by_class:
        lines += ["Host key mismatch on these host(s) - verify before removing anything:",
                  _host_list(by_class[HOSTKEY])]
        for host in by_class[HOSTKEY]:
            lines.append("    ssh-keygen -R %s" % host)
        lines.append("")

    if UNKNOWN in by_class:
        lines += ["Unclassified ssh failures (rerun with -v for the full output):",
                  _host_list(by_class[UNKNOWN]), ""]

    lines.append("=" * 72)
    return "\n".join(lines)


def _host_list(hosts):
    return "\n".join("    %s" % host for host in sorted(hosts))
