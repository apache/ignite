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

# Concepts

## The three roles

| Role | What it is | What runs there |
| --- | --- | --- |
| **coordinator** | wherever you typed `ducktests-remote` — laptop, cluster VM, Jenkins agent | the CLI itself, and only the CLI |
| **runner** | the host the `ducktape` process lives on: `cluster.runner`, or `local` | ducktape, the Python venv, all run state |
| **workers** | `cluster.nodes` — the machines ducktape drives over SSH | Ignite JVMs, the Java applications, Kafka/ZooKeeper when a test needs them |

The coordinator can be any of the three at once. `cluster.runner: local` makes the
coordinator the runner; a worker listed in `extra_hosts` can be the runner too. The roles
are about *function*, not about hardware, and every command is written so that the local
and remote cases take the same code path — see [internals.md](internals.md#transport).

A worker never needs Python and never runs any of this code. ducktape drives it over plain
SSH, which is why `provision --only python` verifies rather than installs, and why the JDK
is the only runtime the workers must actually have.

## What lives where

**On the coordinator:** your config files (`~/.ducktests-remote/config.yaml`, profiles),
your source tree, and whatever `deploy --dist-dir` / `java.archive` point at. Nothing else.
Closing the laptop loses nothing.

**On the runner, under `cluster.state_root`** (default `~/.ducktests-remote`):

```
venv/                 the Python environment ducktape runs in
src/<run-id>/         the synced source tree for one run
runs/<run-id>/        one run directory; see runs.md
runs/latest           symlink to the newest run
```

**On the workers:** `cluster.install_root` (default `/opt`) holds the Ignite
distributions and, when delivered, the JDK. `/mnt/service` (ignitetest's
`persistent_root`) holds everything a test writes. `~/.ssh/environment` and `~/.bashrc`
carry `JAVA_HOME`/`PATH`.

## Why all run state lives on the runner

Because a run outlives the thing that started it. A three-hour suite launched from a
laptop must survive the laptop being closed, and a Jenkins job that starts a run must be
followable from a workstation afterwards.

So: `run` detaches the ducktape process from second zero, writes everything about the run
into a directory on the runner, and every other command (`status`, `logs`, `fetch`, `stop`)
reads that directory. Any coordinator with SSH access to the runner can inspect, follow or
stop a run that a different coordinator started. There is no local state to get out of sync
and nothing to clean up on the coordinator.

## Invariants

These hold everywhere in the codebase. Each has a unit check, and breaking one is a bug
even when the result looks correct.

**The CLI never imports ducktape.** It renders artifacts and drives the ducktape process on
the runner. That keeps it installable on a coordinator with no ducktape at all, and lets
the two versions move independently. `checks/check_remote_transport.py` runs a subprocess
to prove the import boundary holds.

**Everything remote goes through a `Transport`.** No module above `transport.py` shells out
to `ssh` or `scp`. `--runner local` and `--runner build-vm-01` therefore exercise identical
code paths, and the unit checks can substitute a recording fake.

**`--dry-run` executes nothing.** Not even a read-only probe. Commands that need facts from
a host to decide report `not probed (--dry-run)` rather than guessing, and commands that
would transfer print the size instead.

**`doctor` never mutates.** It is safe on a cluster someone else is using. The JDK
discovery script it runs is checked to contain no `mkdir`, `rm`, `mv`, `chmod` or
redirection (`checks/check_remote_java.py`).

**A fan-out never stops at the first failure** unless `--fail-fast` says so. When the
outcome of a command is a request to whoever administers the machines, a partial list of
broken hosts is worse than useless.

**Secrets are redacted by value, not by key name.** Anything resolved from `${env:}` or
`${file:}` is registered with a `Redactor` and replaced with `***` in everything the CLI
prints — including `--dry-run` output, streamed logs and error messages. A password that
leaks into an unrelated field is still caught. See
[configuration.md § Secrets](configuration.md#secrets-and-redaction).

## The two flows this replaces

**`docker/run_tests.sh`** — the local Docker flow. Every node is a container, `ducker01` is
the runner, and the image guarantees the node state. That flow is unchanged and remains the
right one for developing tests.

**A Jenkins one-liner** — a very long `ducktape --globals '<a kilobyte of JSON>' ...`
command that only existed inside a job definition. `ducktests-remote` takes that apart:
the inventory becomes `cluster.nodes`, the JSON blob becomes a profile, and the secrets
become `${env:}` placeholders resolved at launch and written to a `0600` file on the
runner instead of appearing on a command line in build logs. `--globals-json` and
`--cluster-file` exist so the blob can be moved over verbatim first and split up
afterwards; see [commands.md § run](commands.md#run).

## What this tool deliberately does not do

- **It is not configuration management.** `provision` covers the specific package /
  directory / ssh-environment set the Dockerfile installs, and must not grow into Ansible.
- **It does not manage OS repositories.** `pip.*` configures pip; which apt/dnf mirrors a
  VM talks to is set when the image is built.
- **It does not download a JDK from the internet.** It moves one you already have.
- **It takes no cluster lease.** One runner, one operator; queueing belongs to whatever
  schedules the runs. Exit code 3 is reserved should that ever change.
- **It never removes a host key.** A changed host key is reported with the exact
  `ssh-keygen -R` line for you to run after you have verified it.
