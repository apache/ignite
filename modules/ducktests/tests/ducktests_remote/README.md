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

# ducktests-remote

Run Apache Ignite ducktests against a real VM cluster, from wherever you happen to be.

`tests/docker/run_tests.sh` covers the local Docker flow. This covers the other one: a
cluster of real machines, driven by a long `ducktape ...` command line that until now
only existed inside a Jenkins job.

## The model, in three sentences

The **coordinator** is the machine where this CLI runs — your laptop, a VM inside the
cluster, or a Jenkins agent. The **runner** is the host where the `ducktape` process
itself lives; it may be the coordinator (`--runner local`) or an SSH host. The
**workers** are the cluster hosts that ducktape drives over SSH and that actually run
Ignite nodes.

All run state lives on the runner, so any coordinator can inspect, follow or stop a run
that a different coordinator started. Everything the CLI executes goes through a single
`Transport` abstraction, so `--runner local` and `--runner build-vm-01` take identical
code paths.

## Install

```bash
cd modules/ducktests/tests
pip install -e .            # provides the `ducktests-remote` console script
```

Runtime dependencies: the standard library and `PyYAML`. The CLI deliberately **never
imports ducktape** — it drives ducktape on the runner, so it stays installable on a
coordinator that has none. There is a unit check that fails if that ever changes.

## Quickstart

Every command accepts `--dry-run`, and `--dry-run` is genuinely side-effect free: it
prints the commands it would run and the files it would generate, including the rendered
`run.sh`, the `cluster.json`, and a redacted `globals.json`. Start there.

### 1. Describe your cluster

Copy `examples/cluster.yaml` to `~/.ducktests-remote/config.yaml` and edit it. The
minimum:

```yaml
cluster:
  name: lab
  user: max                     # your account; there is no `ducker` on a real VM
  identity_file: ~/.ssh/id_rsa  # path AS THE RUNNER SEES IT
  runner: build-vm-01           # or "local"
  nodes:
    - host: node[01-12].dc.local
```

### 2. Find out what is missing

```bash
ducktests-remote doctor
```

`doctor` probes the coordinator, the runner and every worker in parallel, and never
stops at the first failure. When hosts are unusable it ends with a copy-pasteable
**"what to ask your administrator"** block naming the hosts, the account, and the exact
line to append to `authorized_keys`.

### 3. Run

```bash
ducktests-remote run ./modules/ducktests/tests/ignitetest/tests/smoke_test.py
```

## The three coordinators

**Laptop.** The runner is remote, so the key ducktape uses has to live on the runner.
Agent forwarding will not do: a detached run outlives your SSH session, and the agent
dies with it.

```bash
ducktests-remote keys push          # installs the identity on the runner, authorises it on the workers
ducktests-remote doctor
ducktests-remote run -t ./modules/ducktests/tests/ignitetest/tests/smoke_test.py --detach
ducktests-remote logs -f            # reattach later, from anywhere
```

**A VM inside the cluster.** Set `cluster.runner: local`. The venv is created under
`state_root` on first run and populated from `docker/requirements.txt`.

```bash
ducktests-remote --runner local doctor
ducktests-remote --runner local run ./modules/ducktests/tests/ignitetest/
```

**Jenkins agent.** Use `--detach` plus `status --json`, and read the exit code.

```bash
ducktests-remote --profile ise-perf run -t "$TC_PATHS" --detach
ducktests-remote status --json > status.json
```

## Migrating from the Jenkins one-liner

Do it in two steps, and keep a working run at each one.

**Step 1 — paste the blob verbatim.** Whatever JSON the Jenkins job passes to
`--globals`, hand it over unchanged:

```bash
ducktests-remote run \
  --globals-json '{"project":"ise","ignite_versions":["ise-0-32"],"ssl":{"enabled":true}}' \
  --cluster-file ./49_cluster.json \
  -t ./isetest/perftests/
```

`--cluster-file` is uploaded byte for byte, so an existing hand-written cluster file
keeps working. `--globals-file` reads the same JSON from a file.

**Step 2 — split it into a profile.** Move the keys into
`~/.ducktests-remote/profiles/ise-perf.yaml`, replacing every secret with a placeholder:

```yaml
globals:
  project: ise
  ignite_versions: ["ise-0-32"]
  ssl: {enabled: true}
  authentication:
    enabled: true
    username: ${env:ISE_USER}
    password: ${env:ISE_PASSWORD}
```

Then `ducktests-remote --profile ise-perf run -t ./isetest/perftests/`. Compare the two
with `--dry-run` until the rendered `globals.json` matches, and delete the blob.

`${env:NAME}` and `${file:PATH}` are resolved on the coordinator at launch. A missing
variable is a hard error naming the variable and the file it came from — never an empty
string, never a run that fails on authentication three hours later.

Layering, later winning: built-in defaults → `~/.ducktests-remote/config.yaml` →
`--config` files → `--profile` files → `DTR_*` environment → command-line flags →
`-g KEY=VALUE`. Dicts deep-merge; **lists replace**, so a later layer can shrink one.
`-g` values are parsed as JSON when they parse, so `-g ssl.enabled=true` is a boolean and
`-g project=ise` is a string.

## Secrets

- The composed `globals.json` is written to the run directory with mode `0600`.
- Any value resolved from `${env:}` or `${file:}` is registered with a redactor and
  replaced with `***` in everything the CLI prints — including `--dry-run` output, log
  streaming and error messages. Redaction is keyed on the *value*, so a password that
  leaks into an unrelated field is still caught. Key-name matching is only a fallback.
- `fetch` always excludes `globals.json`.
- The example profiles in `examples/` contain no real hostnames, addresses, accounts or
  passwords. Keep it that way: this directory is in a public Apache repository.

## Deploying distributions

`deploy` is deliberately dumb. Each subdirectory of `--dist-dir` is copied verbatim to
`<install_root>/<name>`; the name is never interpreted or checked against version
parsing, so you name the directories to match what the tests expect.

```
dist/
├── ignite-dev/       -> /opt/ignite-dev
├── ignite-2.17.0/    -> /opt/ignite-2.17.0
└── ise-0-32/         -> /opt/ise-0-32
```

```bash
ducktests-remote deploy --dry-run                  # plan and total bytes, transfers nothing
ducktests-remote deploy --only ignite-dev
ducktests-remote deploy --via build-vm-01          # upload once, fan out from there
ducktests-remote deploy --sudo --owner max         # when /opt is root-owned
```

Each host gets a `.ducktests-deploy.json` manifest (sorted paths + sizes + mtimes;
`--checksum` hashes contents instead). Hosts whose manifest already matches are skipped
unless `--force`. Extraction goes to a temporary directory and is then swapped into
place, because a half-copied distribution that looks present is worse than an absent one.

On a twelve-host cluster a 300 MB distribution is 3.7 GB over the wire from a laptop.
`deploy` prints that total before it starts, and suggests `--via`.

### Where the directory names come from

`ignitetest` resolves a distribution home as `<install_root>/<product>`, where `product`
is `str(IgniteVersion(version))` (`services/utils/path.py`, `services/utils/ignite_aware.py`).
`IgniteVersion.__str__` **normalises**, so:

| `ignite_versions` entry | directory under `/opt` |
| --- | --- |
| `dev` | `ignite-dev` |
| `2.17.0` | `ignite-2.17.0` |
| `ise-0-32` | `ise-0-32` |
| `ise--6` | `ise-6` — note the collapsed dash |

A fork can override `product`, so `doctor` reports a missing directory as a WARN listing
what it *did* find under the install root rather than failing on a guessed mapping.

## Provisioning

`modules/ducktests/tests/docker/Dockerfile` is the source of truth for what a prepared
node looks like; the package list in `config.py` is derived from it and carries a comment
saying so. `provision` is not Ansible and must not become it.

```bash
ducktests-remote provision --dry-run          # recommended first invocation
ducktests-remote provision --sudo --only packages --only dirs
ducktests-remote provision --only ssh-env
ducktests-remote provision --sudo --write-hosts
```

| Step | What it does |
| --- | --- |
| `packages` | Installs the Dockerfile's system utilities. Detects apt/dnf/yum; an unknown package manager is a clear failure, not a guess. Needs `--sudo`. |
| `jdk` | Verifies `java -version` matches the expected major. Installing is opt-in (`--install-jdk`) because where a JDK comes from is site-specific. |
| `python` | Verifies only. Workers do not need Python — ducktape drives them over plain SSH. The runner's venv is created by `run`. |
| `user` | `--create-user NAME` plus `--authorize-key`. Not run by default; most operators use their own account. Needs `--sudo`. |
| `ssh-env` | Writes `PATH`/`JAVA_HOME` into `~/.ssh/environment`. **The one that is easiest to forget.** |
| `dirs` | Creates and chowns `/mnt/service` and the install root. Needs `--sudo`. |
| `hosts` | `--write-hosts` rewrites only the block between `# BEGIN ducktests-remote` and `# END ducktests-remote` in `/etc/hosts`. Needs `--sudo`. |

Anything needing root goes through `sudo -n`. If that fails, the step is reported as
`no-sudo`, skipped, and the remaining steps still run — a partial provision with an
honest report beats an all-or-nothing failure. `provision` always finishes by running
the `doctor` checks, so it ends with evidence rather than an assumption.

### Why `ssh-env` matters

ducktape runs every command over **non-interactive** SSH, where `~/.profile` is not
sourced. A `java` that works fine when you log in by hand is simply absent during a test
run, and the failure surfaces as an unrelated timeout. The Dockerfile solves this with
`PermitUserEnvironment yes` plus `~/.ssh/environment`; this step does the same and then
proves it by running `java -version` non-interactively.

## Privileges the tests actually need

Grepped from the `ignitetest` sources, not assumed:

- **Ordinary unprivileged account** for everything except the two suites below. It needs
  write access to `persistent_root` (default `/mnt/service`) and read access to
  `install_root` (default `/opt`).
- **Passwordless `sudo` for `iptables`** only, and only for the network-segmentation
  suites: `ignitetest/tests/discovery_test.py` and
  `ignitetest/tests/cellular_affinity_test.py`. They reach `sudo iptables`,
  `iptables-save` and `iptables-restore` through `IgniteAwareService.drop_network`
  (`services/utils/ignite_aware.py`).
- **Write access to `install_root`** only if you use `deploy`; otherwise `deploy --sudo`.

Nothing else in `ignitetest` needs root. If your administrator is offering you a
privileged account you do not need, this is the list to show them.

## Cleaning up

```bash
ducktests-remote clean --dry-run     # prints exactly what it would kill and remove
ducktests-remote clean
```

Kills processes matching `clean.process_pattern` (default `org.apache.ignite`, which
covers `CommandLineStartup`, `CdcCommandLineStartup`, `IgniteAwareApplicationService` and
`KafkaToIgniteCommandLineStartup`), then removes `clean.paths` (default `/mnt/service`).

Every path is checked against `clean.allowed_roots` before it is sent anywhere, and the
roots themselves are not removable. A bug here would delete distributions across every
machine at once, so the rule is deliberately blunt.

`stop` runs `clean` afterwards unless you pass `--no-clean`.

## Ctrl-C during `--follow`

`run` launches detached from second zero and then attaches to the log. Therefore:

- **Ctrl-C detaches. It does not stop the run.** The CLI prints how to reattach and how
  to stop.
- A **second Ctrl-C within 3 seconds** offers to stop the run (and, in a non-interactive
  shell, simply detaches).
- `--detach` skips the following entirely.

Reattach with `ducktests-remote logs <run-id> -f`; stop with
`ducktests-remote stop <run-id>`.

## Run directory

On the runner, under `<state_root>/runs/<run-id>/`:

```
meta.json      run id, coordinator, start time, test paths, redacted config summary
cluster.json   what ducktape was given
globals.json   composed globals, mode 0600
run.sh         the exact command; ssh in and rerun it to reproduce by hand
launch.sh      wrapper that waits on run.sh and records exit_code
pid, pgid      for stop
exit_code      written when the process ends
ducktape.log   combined stdout and stderr
results/       ducktape --results-root, with ducktape's own `latest` symlink inside
```

`<state_root>/runs/latest` points at the newest run. Run ids look like
`max-20260727-141233-9f2a`.

## Exit codes

| Code | Meaning |
| --- | --- |
| 0 | success |
| 1 | usage or configuration error |
| 2 | preflight failed |
| 3 | reserved (this deployment has a single runner and takes no cluster lease) |
| 4 | ducktape ran and reported test failures |
| 5 | transport or infrastructure error |
| 130 | interrupted by the operator |

4 and 5 are deliberately distinct: Jenkins needs "tests failed" separate from "the
cluster is broken". Note that ducktape itself exits `1` both for test failures and for
its own startup errors, so a `4` means "ducktape ran and exited non-zero" — the log
distinguishes the two.

There is no cluster lease. This deployment has one runner and one operator; queueing is
the job of whatever schedules these runs.

## Troubleshooting

**Stale Ignite JVMs.** The single most common source of baffling failures on a shared
cluster: a previous run was killed and its JVMs are still holding ports and
`/mnt/service`. `doctor` reports this as a FAIL with the host list. Fix with
`ducktests-remote clean --dry-run` and then `clean`.

**`identity_file` is a runner-side path.** It is the path *ducktape* will open, on the
runner. If the runner is not the coordinator, a file that exists on your laptop proves
nothing. `doctor` checks it on the runner and reports mode; `keys push` installs it.

**Agent forwarding does not survive a detached run.** `ssh -A` gives you an agent for the
lifetime of your session. A run that lasts hours outlives it, and every subsequent
worker connection then fails. Use a real key file on the runner.

**`java: command not found` deep inside a test.** Non-interactive SSH does not source
`~/.profile`. Run `provision --only ssh-env`.

**Discovery failures with no useful message.** Workers that cannot resolve each other's
hostnames fail inside discovery. `doctor` runs an N-way resolution probe; `provision
--write-hosts` is the escape hatch when cluster DNS cannot be fixed.

**"source payload is N MB, above the limit".** A build directory leaked into the sync.
Distributions go through `deploy`, never through the source sync. Adjust `--exclude` or
add a `.ducktestsignore` file at the source root.

## Development

```bash
cd modules/ducktests/tests
pytest ducktests_remote/checks      # unit only: no network, no Docker, no ducktape
flake8 ducktests_remote
```

The checks live in `ducktests_remote/checks/` and are named `check_*.py` with `Check`
classes and `check_*` methods, because that is what `[pytest]` in `tox.ini` collects.
`checks/fake_transport.py` provides a recording transport; nothing in the unit checks
touches a network or a real process except the deliberate subprocess in the import guard.
