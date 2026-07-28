<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Troubleshooting

Start with `ducktests-remote doctor`. It probes everything in parallel, never stops at the
first failure, and changes nothing, so it is always safe to run — including on a cluster
someone else is using.

## The classic failures

### Stale Ignite JVMs

The single most common source of baffling failures on a shared cluster: a previous run was
killed and its JVMs still hold ports and `/mnt/service`. Tests fail in ways that have
nothing to do with the change you are testing.

`doctor` reports it as a FAIL with the host list.

```bash
ducktests-remote clean --dry-run    # prints every pid and command line it would kill
ducktests-remote clean
```

### `java: command not found`, or the wrong JDK, deep inside a test

Non-interactive SSH does not source `~/.profile`, so the `java` you get when you log in by
hand is not necessarily the one the tests get.

```bash
ducktests-remote provision --only jdk --only ssh-env
```

If it still reports the wrong JVM afterwards, sshd is ignoring `~/.ssh/environment` **and**
the login shell is not bash. Set `java.home` to a JDK the site already puts on the default
`PATH`. Full detail in [java.md](java.md).

### `identity_file` does not exist on the runner

`cluster.identity_file` is the path *ducktape* will open, **on the runner**. A file that
exists on your laptop proves nothing. `doctor` checks it there and reports its mode;
`keys push` installs it.

### Agent forwarding does not survive a detached run

`ssh -A` gives you an agent for the lifetime of your session; a run that lasts hours
outlives it, and every subsequent worker connection then fails — typically after the first
few minutes, which makes it look like an intermittent cluster problem. Use a real key file
on the runner (`keys push`).

### Discovery failures with no useful message

Workers that cannot resolve each other's hostnames fail inside Ignite discovery, far from
the cause. `doctor` runs an N-way resolution probe from one worker.

```bash
ducktests-remote provision --sudo --write-hosts   # escape hatch when cluster DNS cannot be fixed
```

### `deploy` wants to send a gigabyte of `ignite-dev`

The distribution is a link to a checkout and nothing is being filtered. The workers need
`modules/*/target/*.jar`, `modules/*/target/libs/*.jar`, `bin/` and
`modules/ducktests/tests/certs`; `.git` and every `src` tree are ballast. Set
`deploy.exclude` (or `--exclude`, or a `.ducktests-deploy.ignore` at the root of the
distribution) and confirm with `--dry-run`, which prints the payload size and how many
files the patterns dropped. The recipe is in
[commands.md](commands.md#ignite-dev-from-your-own-checkout).

Do not reach for `.ducktestsignore` here — that file is the *source sync* list and its
patterns are the opposite ones.

### "source payload is N MB, above the limit"

A build directory leaked into the sync. Distributions go through `deploy`, never through
the source sync. Adjust `--exclude`, add a `.ducktestsignore` at the source root, or raise
`run.max_payload_mb` if the payload really is that big.

### The venv cannot be prepared

The message names the index it tried. On a network without PyPI access, set `pip.index_url`
(see [configuration.md § pip](configuration.md#pip)) or point `runner.venv` at an
environment that already has ducktape. `doctor` prints the effective index on the runner
row, with credentials masked.

### Tests are skipped as un-runnable

Most `ignitetest` suites declare `@cluster(num_nodes=...)` above three. An inventory smaller
than the largest declaration means those tests are never scheduled. `run` warns when the
inventory is below three hosts; ducktape's own report lists what it skipped and why.

### No distribution for a version

`doctor` reports a WARN listing what it *did* find under `install_root`. Remember that
ignitetest normalises version strings: `ise--6` maps to `/opt/ise-6`. See
[commands.md § deploy](commands.md#where-the-directory-names-come-from).

## SSH failure classes

Every failed connection is classified and mapped to a concrete next action, because the
first-time experience is usually not a subtle bug but "nobody has added me to these
machines yet".

| Class | Meaning | Next action |
| --- | --- | --- |
| `unresolved` | the hostname does not resolve from here | check VPN/DNS, or put the address in `cluster.nodes[].ip` |
| `no-sshd` | the host answers but nothing listens on the port | sshd is down or on another port |
| `unreachable` | no network path | firewall, routing, or the host is down |
| `no-access` | your key is not authorised for that account | `keys push`, or the administrator block |
| `no-user` | the account does not exist there | ask for it, or use a per-host `user` override |
| `hostkey` | the host key changed | **verify first**, then run the printed `ssh-keygen -R` yourself; this tool will never remove a host key for you |
| `no-sudo` | passwordless sudo is missing | only the two network-segmentation suites need it |
| `unknown` | unrecognised | rerun with `-v` for the full stderr |

When any host is unusable, the report ends with a **"what to ask your administrator"**
block: the hosts, the account, the fingerprint of the key being offered, the exact line to
append to `authorized_keys`, and — for `no-sudo` — the precise sudoers line plus the two
suites that need it. It is meant to be forwarded verbatim.

The classification patterns come from OpenSSH's own message strings and have not been
replayed against every distribution's build. Adding one is a one-line change in
`sshdiag.py`; `checks/check_remote_sshdiag.py` is table-driven over recorded samples.

## Privileges the tests actually need

Grepped from the `ignitetest` sources, not assumed. Useful when an administrator offers you
a privileged account you do not need:

- **An ordinary unprivileged account** for everything except the two suites below. It needs
  write access to `persistent_root` (default `/mnt/service`) and read access to
  `install_root` (default `/opt`).
- **Passwordless `sudo` for `iptables` only**, and only for
  `ignitetest/tests/discovery_test.py` and `ignitetest/tests/cellular_affinity_test.py`,
  which reach `sudo iptables`, `iptables-save` and `iptables-restore` through
  `IgniteAwareService.drop_network`.
- **Write access to `install_root`** only if you use `deploy` without `--sudo`.

Nothing else in `ignitetest` needs root.

## When the message is not enough

```bash
ducktests-remote -v doctor              # every command as it is issued, full per-host output
ducktests-remote --dry-run <command>    # the exact scripts and files, executed nowhere
ducktests-remote status --json          # machine-readable run state
```

On the runner, the run directory holds the whole truth: `run.sh` is the exact command,
`ducktape.log` is the combined output, and `meta.json` records the configuration the run
was launched with. See [runs.md](runs.md).

If a failure looks like a bug in this CLI rather than in the cluster, the unit checks run
in under a second and touch no network:

```bash
cd modules/ducktests/tests
pytest ducktests_remote/checks
flake8 ducktests_remote
```
