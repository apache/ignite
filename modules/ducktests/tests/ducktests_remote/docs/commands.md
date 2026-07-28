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

# Command reference

Ten commands: `run`, `status`, `logs`, `fetch`, `stop`, `provision`, `deploy`, `clean`,
`doctor`, `keys`.

## Flags every command accepts

| Flag | Effect |
| --- | --- |
| `--config FILE` | extra config document; repeatable, applied in order |
| `--profile NAME` | named profile; repeatable, applied in order |
| `--runner HOST` | override `cluster.runner`; `local` runs ducktape here |
| `--jobs N` | fan-out parallelism (default 16) |
| `-v, --verbose` | print every command as `+ host$ ...`, and full per-host output |
| `-q, --quiet` | only results and errors |
| `--dry-run` | print what would happen; execute nothing |
| `--no-color` | disable ANSI colour |
| `--fail-fast` | stop scheduling further hosts after the first failure |

`--version` prints the CLI version. Everything after a bare `--` is passed straight to
ducktape by `run`.

## What each command transfers

The question that matters most on a slow link, answered in one table. "Coordinator → X"
means bytes leave your machine.

| Command | Transfers | To whom | When |
| --- | --- | --- | --- |
| `run` | source tree, `cluster.json`, `globals.json`, `parameters.json`, `run.sh`, `launch.sh`, `meta.json` | **runner only** | every run, unless `--no-sync` (which skips the source tree only) |
| `provision --only jdk` | the `java.archive` tarball | **workers**, and only those with no matching JDK | see [java.md](java.md) |
| `deploy` | one tarball per distribution | **workers**, skipping hosts whose manifest already matches | every invocation, unless skipped or `--dry-run` |
| `keys push` | private key + `.pub` to the runner, public key text to the workers | runner and workers | every invocation |
| `fetch` | *downloads* a results archive | runner → coordinator | every invocation |
| `doctor`, `status`, `logs`, `clean`, `stop` | nothing | — | never |

Nothing is transferred implicitly by any other command, and `--dry-run` transfers nothing
anywhere.

---

## `run`

```
run [TEST_PATH...] [-t PATH]... [-g KEY=VALUE]... [-p KEY=VALUE]...
    [--globals-json JSON | --globals-file PATH] [--params-json JSON]
    [--cluster-file PATH] [-n N] [--source-root PATH] [--no-sync]
    [--exclude PATTERN]... [--work-dir PATH] [--install-sources]
    [--pip-index-url URL] [--pip-extra-index-url URL]... [--pip-trusted-host HOST]...
    [--pip-timeout SECONDS] [--pip-cert PATH]
    [--repeat N] [--max-parallel N] [--test-runner-timeout MS] [--results-root PATH]
    [--skip-preflight] [--follow | --detach] [-- EXTRA DUCKTAPE ARGS]
```

At least one test path is required, positionally or with `-t`.

### Where you run it from, and what a test path means

The whole Ignite checkout is synced to the runner, and ducktape is started in its
`modules/ducktests/tests` directory — the same working directory `docker/run_tests.sh`
uses. So a test path is written exactly as it is locally:

```
ducktests-remote run ./ignitetest/tests/smoke_test.py::SmokeServicesTest.test_ignite_start_stop
```

The checkout is found by walking up from the current directory, so the command works from
`modules/ducktests/tests`, from the repository root, or from anywhere in between.
`--source-root` / `run.source_root` overrides the search. A directory that is not a
checkout — no `modules/ducktests/tests/docker/requirements.txt` under it — is refused
before anything is uploaded.

Test paths are accepted in any form that resolves on the coordinator (relative to the
current directory, to the tests directory, or to the checkout root) and are rewritten
into the tests-relative form ducktape sees. A path inside the checkout but outside the
tests directory becomes a runner-side absolute path; one that resolves nowhere is passed
to ducktape unchanged, with a warning.

### Order of operations

1. **Compose `globals`**: `--globals-json`/`--globals-file` (the raw layer), then config
   `globals`, then `-g` overrides. `parameters` likewise from `--params-json`, config
   `parameters`, `-p`. Placeholders resolve here; a missing one aborts now.
2. **Select nodes** (`-n` takes the first N) and build the cluster payload — generated from
   the inventory, or `--cluster-file` read and validated but uploaded byte for byte.
3. **Topology warnings**: the runner also appearing in `cluster.nodes`, and an inventory
   below three hosts (most suites declare `@cluster(num_nodes=...)` well above that and
   would be skipped as un-runnable). Warnings only.
4. **Preflight**: the full `doctor` check set, unless `--skip-preflight` or `--dry-run`.
   Any FAIL stops here with exit 2, before anything is created.
5. **Allocate the run id** — `max-20260727-141233-9f2a` — and derive the run directory,
   work directory and results root, and normalise the test paths against the checkout.
6. **Render `run.sh`** and, with `--dry-run` or `-v`, print it along with `cluster.json`
   and a redacted `globals.json`. **`--dry-run` returns here.**
7. **Create** the run and results directories on the runner.
8. **Sync the source tree** to `<state_root>/src/<run-id>` unless `--no-sync`. The payload
   is measured first and refused above `run.max_payload_mb` (200 MB): that almost always
   means a build directory leaked in. rsync when both ends have it, otherwise a tar stream
   over scp.
9. **Ensure the venv**: create `<state_root>/venv` when missing, and install
   `docker/requirements.txt` into it when `import ducktape` fails. This is where `pip.*`
   applies. Runs after the sync because the requirements file comes from the synced tree.
   A missing requirements file is reported as such, separately from an unreachable index.
10. **`--install-sources`** (opt-in): `pip install -e <work_dir>/modules/ducktests/tests`,
    with the same `pip.*` flags.
11. **Write the artifacts**: `cluster.json`, `globals.json` (`0600`), `parameters.json`
    (`0600`, when non-empty), `run.sh` (`0755`), `launch.sh` (`0755`), `meta.json`, and
    update the `runs/latest` symlink.
12. **Launch detached** — `setsid nohup bash launch.sh`, falling back to `nohup` plus
    `disown` — and record the pid.
13. **Follow** the log, or print the reattach commands and exit with `--detach`.

### Following, and Ctrl-C

The run is detached from second zero, so following is just streaming a file:

- **Ctrl-C detaches. It does not stop the run.** Losing a three-hour run to a reflex is
  not recoverable.
- A **second Ctrl-C within 3 seconds** offers to stop it; in a non-interactive shell it
  simply detaches.
- Reattach with `logs <run-id> -f` from any coordinator.

### Exit codes

`0` success · `2` preflight failed · `4` ducktape exited non-zero · `1` configuration
error · `5` transport error · `130` interrupted. See [runs.md](runs.md#exit-codes) for why
4 and 5 are distinct.

---

## `doctor`

```
doctor [--json] [-n N]
```

Probes the coordinator, the runner and every worker **in parallel**, never stopping at the
first failure, and **changes nothing anywhere**. Also runs implicitly before `run`.

### What it checks, in order

1. **Coordinator** — with a remote runner: `ssh` and `scp` on `PATH`. With
   `--runner local`: ducktape importable here, and its version against the pin in
   `docker/requirements.txt`.
2. **Runner** — `bash`, `python3`, `setsid` present (a missing `setsid` is a WARN: there is
   a `nohup` fallback); ducktape importable through the venv python; `identity_file` exists
   with sane permissions; free space at `state_root`; the effective pip index.
3. **Workers, reachability** — one connection each from the coordinator, every failure
   classified (see [troubleshooting.md](troubleshooting.md#ssh-failure-classes)).
4. **Workers, substance** — one script per reachable host returning: clock skew, free space
   at `install_root` and at `/mnt/service`, writability of both, stale JVMs matching
   `clean.process_pattern`, passwordless sudo, the distributions present under
   `install_root`, and the full JDK probe.
5. **Name resolution** — from the first reachable worker, `getent hosts` for every peer.
   Workers that cannot resolve each other fail deep inside discovery with no message that
   points here.
6. **Runner → workers** — the connection *ducktape itself* will make, from the runner, with
   the runner-side identity. A coordinator that can reach a worker proves nothing about the
   runner being able to.

### The administrator block

When hosts are unusable, the report ends with a copy-pasteable block naming the hosts, the
account, the key fingerprint being offered, and the exact line to append to
`authorized_keys` — plus, for missing sudo, the precise sudoers line and the two test
suites that need it. Forward it as is.

### Verdicts

FAIL stops a run at preflight; WARN never does. `--json` prints every check with its scope,
host, name, status and message, plus an `ok` boolean. Exit `0` when nothing failed, `2`
otherwise.

---

## `provision`

```
provision [--only STEP]... [--skip STEP]... [--sudo] [--install-jdk]
          [--java-home PATH] [--java-major N] [--java-archive PATH] [--force]
          [--create-user NAME] [--authorize-key PATH] [--write-hosts] [-n N] [--json]
```

Brings unconfigured VMs up to the state `docker/Dockerfile` guarantees. Steps run in this
order, each idempotent and independently selectable:

| Step | Does | Needs root | In the default set |
| --- | --- | --- | --- |
| `packages` | installs `provision.packages`; detects apt/dnf/yum, an unknown manager is a clear failure rather than a guess | yes | yes |
| `jdk` | resolves a JDK per host, delivers `java.archive` to hosts that need one — [java.md](java.md) | only with `--install-jdk`, or to write into an unwritable `install_root` | yes |
| `python` | reports whether `python3` exists. **Verifies only**: workers do not need Python, and nothing here will install it | no | yes |
| `user` | `--create-user NAME` plus `--authorize-key` | yes | only with `--create-user` or `--only user` |
| `ssh-env` | points the non-interactive `PATH`/`JAVA_HOME` at the resolved JDK, then verifies over a fresh connection | no | yes |
| `dirs` | creates and chowns `provision.dirs` plus `install_root` | yes | yes |
| `hosts` | rewrites only the block between `# BEGIN ducktests-remote` and `# END ducktests-remote` in `/etc/hosts` | yes | only with `--write-hosts` or `--only hosts` |

Anything needing root goes through `sudo -n`. If `--sudo` was not given, the step is
reported as skipped and **the remaining steps still run** — a partial provision with an
honest report beats an all-or-nothing failure. Unless `--dry-run`, the command always
finishes by running the `doctor` checks, so it ends with evidence rather than an
assumption; a doctor FAIL makes it exit 2.

`--dry-run` prints the exact script per host and probes nothing.

---

## `deploy`

```
deploy [--dist-dir PATH] [--only NAME]... [--exclude PATTERN]... [--install-root PATH]
       [--via HOST] [--sudo] [--owner USER] [--force] [--checksum] [-n N] [--json]
```

Each subdirectory of `--dist-dir` is copied verbatim to `<install_root>/<name>`. The name
is never interpreted or checked against version parsing, which is what makes fork layouts
work without special cases — you name the directories to match what the tests expect.

Per distribution, per host:

1. Build a manifest on the coordinator: sorted relative paths plus sizes and mtimes, or
   content hashes with `--checksum`.
2. Read `.ducktests-deploy.json` from the target; if its hash matches, **skip this host**
   (unless `--force`).
3. Refuse if `install_root` is not writable and `--sudo` was not passed.
4. Extract into a staging directory beside the target, then **swap it into place** and
   delete the old tree. A half-copied distribution that looks present is worse than an
   absent one.

`--via HOST` uploads the payload once to an intermediate host and fans out from there.
`deploy` prints the total bytes before it starts — on a twelve-host cluster a 300 MB
distribution is 3.7 GB from a laptop — and suggests `--via` when that total is large.

### Leaving files out

Excludes are rsync-style patterns, matched against paths relative to the distribution
root: a pattern matches the whole relative path, a path prefix, or any single path
component. So `src` drops every `modules/*/src`, `*.jar` drops jars anywhere, and
`modules/indexing` drops that one subtree — but `target/libs` matches only a `target/libs`
directly at the root, not `modules/core/target/libs`. They default to nothing: a
distribution without them is shipped byte for byte. Three sources, most specific winning:

| Source | Scope |
| --- | --- |
| `--exclude PATTERN` (repeatable) | every distribution in this invocation |
| `.ducktests-deploy.ignore` at the root of a distribution | that distribution |
| `deploy.exclude` in the configuration | every distribution |

The list is read from one source as a whole; sources are never merged. The ignore file is
one pattern per line, `#` comments allowed, and is itself never shipped.

It is **not** called `.ducktestsignore`: when `ignite-dev` links to a checkout, the
distribution root and the source root are the same directory, and the two lists are
opposites — the source sync drops `target`, `deploy` keeps almost nothing else.

The manifest is built from the same filtered file list, so a host reported as up to date
holds exactly the files the tarball carried. Change the excludes and every host is
redeployed, as it should be.

### `ignite-dev` from your own checkout

`ignite-dev` is the distribution the tests resolve `DEV_BRANCH` to, and on a worker it
must have the layout of a *built source tree*, not of a release: `IgniteSpec` puts
`modules/<module>/target` and `modules/<module>/target/libs` on the classpath for every
module a test asks for (`ignitetest/services/utils/ignite_spec.py`), `path.py` runs
`bin/ignite.sh` from the same home and reads certificates from
`modules/ducktests/tests/certs`. Everything else in a checkout is ballast for a worker.

So link the distribution to your checkout and let the excludes do the trimming:

```bash
mkdir -p ~/dist
ln -sfn ~/Development/vanilla/ignite ~/dist/ignite-dev     # relink any time
```

`deploy` follows that link: `is_dir()` accepts it as a distribution, the tree is walked
through it, and the workers receive ordinary files. Symlinks *inside* a distribution are
a different matter — they are stored as links and arrive dangling, so keep real files
below the top level.

Then, in your configuration:

```yaml
deploy:
  dist_dir: ~/dist
  exclude: [.git, .idea, src, docs, assembly, classes, test-classes,
            generated-sources, generated-test-sources, maven-status, maven-archiver,
            surefire-reports, javadoc, "*.tar.gz", "*.zip", __pycache__, "*.pyc"]
```

`src` as a pattern drops every `modules/*/src`, `classes` drops the exploded output Java
never reads off a classpath directory, and `target/*.jar` plus `target/libs/*.jar`
survive. The daily loop is then two commands:

```bash
mvn package -pl :ignite-ducktests -am -DskipTests   # in the checkout, however you build
ducktests-remote deploy --only ignite-dev
```

Check the damage before the first real transfer — `--dry-run` prints the payload size and
how many files the patterns dropped, and transfers nothing:

```bash
ducktests-remote deploy --only ignite-dev --dry-run
```

Note that a distribution is all-or-nothing: rebuild one module and the whole distribution
is re-tarred and re-uploaded, because the manifest hash covers the tree.

### Where the directory names come from

ignitetest resolves a distribution home as `<install_root>/<product>`, where `product` is
`str(IgniteVersion(version))`, and `IgniteVersion.__str__` **normalises**:

| `ignite_versions` entry | directory |
| --- | --- |
| `dev` | `ignite-dev` |
| `2.17.0` | `ignite-2.17.0` |
| `ise-0-32` | `ise-0-32` |
| `ise--6` | `ise-6` — note the collapsed dash |

A fork can override `product`, so `doctor` reports a missing directory as a WARN listing
what it *did* find rather than failing on a guessed mapping.

---

## `clean`

```
clean [-n N] [--keep-paths]
```

Kills processes matching `clean.process_pattern` (SIGTERM, five seconds, then SIGKILL for
survivors) and removes `clean.paths`. `--keep-paths` kills without deleting.

Every path is validated **on the coordinator, before it is sent anywhere**: it must be
absolute, must sit under one of `clean.allowed_roots`, and must not *be* one of those roots.
A bug here would delete distributions across every machine at once, so the rule is
deliberately blunt.

`--dry-run` prints the exact process list (pid plus command line) and the exact paths with
their sizes, and kills and deletes nothing. Use it first, always.

---

## `keys push`

```
keys push [--identity PATH] [--generate] [-n N]
```

Installs the private key on the **runner** (mode `0600`, `.pub` alongside at `0644`) and
appends the public key to `authorized_keys` on every worker and extra host.

Why it exists: ducktape connects from the runner to the workers with the identity named in
the cluster file, and a detached run outlives your SSH session. Agent forwarding dies with
that session, producing a run that authenticates for the first few minutes and then fails
on every subsequent connection. A real key file on the runner is the only arrangement that
survives.

`--generate` creates an RSA keypair when the identity does not exist. Already-authorised
hosts report `ok` rather than appending a duplicate.

---

## `status`

```
status [RUN_ID] [--all] [--json] [-n LINES]
```

With no run id, the most recent run on the runner. Prints state, pid, start time, elapsed,
exit code, test paths, cluster and node count, run directory, results path, and the last
15 log lines (`-n`). `--all` prints a table of every run, newest first. `--json` is the
Jenkins-friendly form.

Because all state lives on the runner, this works from any coordinator, including one that
did not start the run.

---

## `logs`

```
logs [RUN_ID] [-f] [-n LINES]
```

Prints the last 200 lines (`-n`) of `ducktape.log`, then streams with `-f` until the run
ends. Ctrl-C stops following and never touches the run. Output passes through the redactor,
so a secret that reached the log is masked on the way to your terminal.

---

## `fetch`

```
fetch [RUN_ID] [--dest DIR] [--full]
```

Archives the results **on the runner**, downloads one file, and extracts it into
`<dest>/<run-id>` (default `./ducktests-results/<run-id>`), then downloads `ducktape.log`
alongside. By default only ducktape's reports — `report.html`, `report.txt`, `report.json`,
`test_log.info`, `session.log`; `--full` takes the whole results tree.

`globals.json` is **always** excluded, at both ends. Extraction refuses any member that
would escape the destination directory.

---

## `stop`

```
stop [RUN_ID] [--kill] [--timeout SECONDS] [--no-clean] [-n N]
```

1. Touches `stopped` in the run directory, so the final state is reported as `stopped`
   rather than `failed`.
2. SIGTERMs the run's **process group**, waits up to `--timeout` (default 60s), and with
   `--kill` SIGKILLs whatever survives. Without `--kill` it says so and leaves it.
3. Writes exit code `143` if the process never wrote one itself.
4. Runs `clean` across the workers, unless `--no-clean`.

Stopping a run that already ended is safe: it says so, and still cleans.
