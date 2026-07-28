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

# Choosing the workers' JVM

## Why PATH decides, not JAVA_HOME

`ignitetest` reaches a JVM four different ways, and only one of them respects `JAVA_HOME`:

| Consumer | Mechanism |
| --- | --- |
| `ignite.sh`, via `IgniteSpec.envs()` → `export …;` | honours `JAVA_HOME` |
| `jvm_utils.java_version()` → `java -version` over ssh | bare `java`, so **PATH** |
| `services/kafka/kafka.py` → `nohup java …` | bare `java`, so **PATH** |
| `jmx_utils` → `java -jar jmxterm.jar` | bare `java`, so **PATH** |

All four run over **non-interactive** SSH, where `~/.profile` is never sourced. So setting
`JAVA_HOME` alone changes what `ignite.sh` uses and nothing else, and a `java` that works
perfectly when you log in by hand can be absent or wrong during a test run. Both are set,
and — because neither mechanism is guaranteed to work at a given site — what a fresh
non-interactive session actually gets is then *measured*.

This is also why `doctor` judges the JVM on `PATH` rather than the JDK it can find: a
perfect Java 17 sitting in `/opt` that the non-interactive `PATH` does not point at is not
the JVM the suite will run under.

The runner needs no JVM at all. ducktape is pure Python. Java is purely a worker concern.

## The resolution ladder

Run by `provision --only jdk`, per host. Rungs 1–3 are pure discovery — one round trip,
no writes — which is why `doctor` and `provision --only ssh-env` can run the identical
script.

**1. `java.home` is set.** Verify `$home/bin/java`. If it is missing or unusable on a host,
that host **fails, by name**. There is no fallback: an explicit `java.home` that silently
resolves to a different JVM would defeat the point of saying it. This is checked before
anything else, so an explicit home is never overridden by a lucky match elsewhere.

**2. The JVM already on the non-interactive PATH matches `java.major`.** Use it as is,
report `ok`. A correctly prepared VM never gets touched.

**3. A JDK under `java.search_paths` matches.** Each search path is probed both as a
directory *of* JDK homes (`/usr/lib/jvm/*/bin/java`) and as a JDK home itself
(`/opt/bin/java`), so both layouts work. Every candidate's `java -version` is parsed the
same way `jvm_utils.java_major_version` does: `1.8.0_292` → 8, `11.0.19` → 11,
`17.0.11+9` → 17. Among candidates of the requested major, the **highest patch level**
wins, compared numerically — string sorting would put `17.0.9` above `17.0.11` — and the
path breaks ties, so the choice is deterministic.

**4. `java.archive` is delivered.** Only now, and only to the hosts that reached this rung.
Details below.

**5. Nothing worked.** The host fails with the list of every JDK that *was* found and the
config keys that would fix it. With `--install-jdk` **and** `--sudo`, the distribution's
own `openjdk-N-jdk` / `java-N-openjdk-devel` package is attempted first as a last rung.

## When the archive is actually sent

The single question this section exists to answer.

**Only `provision`, step `jdk`, ever sends a JDK.** Not `doctor` (it never mutates
anything), not `ssh-env` (it only points `PATH` at a JDK that is already there), not `run`,
not `deploy`.

```bash
ducktests-remote provision --only jdk        # just this step
ducktests-remote provision --sudo            # all steps; jdk runs before ssh-env
```

Within that step:

| Phase | Where | What happens |
| --- | --- | --- |
| 1 | coordinator, once | `java.archive` is opened with `tarfile`, searched for `bin/java`, and its size printed. **A bad archive fails here**, before a byte moves. A directory is packed here too, once for the whole run |
| 2 | each host, in parallel | the discovery script runs rungs 1–3 |
| 3 | each host that found nothing | delivery, below |

A host reaches delivery only when **all** of these hold:

- rungs 1–3 found no JDK of `java.major` on it, **and**
- `java.home` is unset — if it is set and missing, that is a failure, never a delivery, **and**
- `java.archive` is set — otherwise the host fails with the "set `java.archive` or
  `java.home`" message.

So on a twelve-host cluster where eleven already carry Java 17, exactly one upload happens.

### What delivery does on the host

1. Read `.ducktests-java.json` in the target directory. If its hash matches the archive,
   **skip** — nothing is uploaded. `--force` overrides.
2. Check `install_root` is writable; if not and `--sudo` was not passed, fail naming the
   directory and the account.
3. Create a staging directory beside the target, upload the archive into it, and unpack —
   with `--strip-components` set to whatever wraps the JDK home (1 for a stock Temurin
   tarball) and the decompression flag taken from the file's suffix.
4. Verify `bin/java` exists in the staging tree; if not, remove the staging tree and fail.
5. Write the manifest, then **swap** the staging tree into place and delete the old one.

The staging-and-swap is `deploy`'s, reused rather than reimplemented: a half-extracted JDK
that looks present is exactly as bad as a half-extracted distribution.

### Archive formats

`.tar.gz`, `.tgz`, `.tar`, `.tar.bz2`/`.tbz2` and `.tar.xz`/`.txz` are all accepted; the
`tar` flag on the worker follows the suffix.

The JDK home is *located* rather than assumed: the shallowest `bin/java` in the archive
wins, and everything above it is stripped. A second, deeper `bin/java` (a bundled JRE)
cannot pull the depth with it.

| Given | Result |
| --- | --- |
| one top-level dir, `jdk-17.0.11+9/bin/java` | stripped by 1; target defaults to that dir's name, e.g. `/opt/jdk-17.0.11+9` |
| flat, `bin/java` at the root | not stripped; target defaults to the file name without its suffix |
| wrapped deeper, `openjdk-17/jdk-17.0.11+9/bin/java` | stripped by 2; target defaults to `jdk-17.0.11+9` |
| extra entries beside the JDK (a stray `LICENSE`, AppleDouble `._*` files) | ignored; they do not change where `bin/java` is |
| an unpacked directory containing `bin/java` | packed once on the coordinator and sent to every host that needs it |
| no `bin/java` anywhere | **refused on the coordinator**, naming the archive |
| a macOS build (`Contents/Home` in the way) | **refused on the coordinator** — it unpacks fine and then fails on every worker |
| `.zip` | **refused**, naming `.tar.gz`. Linux JDKs ship as tarballs, and a zip branch would be untested code on every real run |

`java.name` overrides the target directory name; `java.install_root` overrides where it
goes (defaulting to `cluster.install_root`).

### Cost

The archive is uploaded from the coordinator to each host that needs it. `provision` prints
the archive size up front and, above 200 MB with more than three hosts, warns with the
worst-case total. `provision` has no `--via`; if that total is painful, a good pattern is
to deliver to one machine, or to place the JDK in your `deploy` dist directory once and
point `java.home` at the result.

## Making the choice stick

The `ssh-env` step writes **both** files, from one resolved value, in one step, so they
cannot drift apart:

**`~/.ssh/environment`** — what the Dockerfile does. Contains `JAVA_HOME`, a `PATH` with
`$JAVA_HOME/bin` **first** (plus `provision.ssh_env_path_extra`), and `LANG=C.UTF-8`.
Silently ignored unless sshd carries `PermitUserEnvironment yes`; the step says so when it
is absent. The existing `PATH` is filtered of the entries about to be prepended, so running
the step repeatedly neither grows the variable nor reports a change for ever.

**`~/.bashrc`** — a block between `# BEGIN ducktests-remote` and `# END ducktests-remote`
at the **top** of the file, above the `case $- in *i*) ;; *) return;; esac` guard the stock
Ubuntu file opens with. That guard exists precisely because bash *does* source `~/.bashrc`
for non-interactive ssh commands, so appending would be writing to `/dev/null`. Only the
block between the markers is ever rewritten. This does nothing when the account's login
shell is not bash.

Either can be switched off with `java.ssh_environment: false` / `java.bashrc: false`;
switching off both is an error.

**Then it verifies.** A fresh non-interactive connection runs `java -version` and reads
`$JAVA_HOME`. That result is the outcome of the step: if the session still gets the wrong
JVM, the step fails there — with the advice that sshd may be ignoring `~/.ssh/environment`
*and* the login shell may not be bash — rather than reporting success and letting a test
fail three hours later.

## What `doctor` says

| Situation | Verdict | Message |
| --- | --- | --- |
| no `java` on the non-interactive PATH | **FAIL** | run `provision --only jdk --only ssh-env` |
| PATH java's major ≠ `java.major`, but a matching JDK is on the host | **FAIL** | names the JDK it found and the command that would point PATH at it |
| PATH java's major ≠ `java.major`, nothing matching installed | **FAIL** | lists what was found; names `java.archive` and `java.home` |
| major matches, but an explicit `java.home` is not the JDK in effect | **WARN** | the tests will run on the right version; the pin is simply not active |
| major matches | **OK** | version and resolved path |
| hosts disagree on version | **WARN** on a `java-consistency` row | majority version and the outliers |

A FAIL blocks `run` at preflight with exit 2. `--skip-preflight` remains the escape hatch,
and `java.major: null` disables the version requirement entirely (whatever is on the host
is then accepted).

## Worked scenarios

**VMs already have the right JDK.** `java.major: 17`, nothing else. Rung 2 matches
everywhere, `ssh-env` makes sure non-interactive sessions see it, no transfers at all.

**A JDK sits in `/opt/jdk-17.0.11` but `java` is 11.** Rung 3 finds it.
`provision --only jdk --only ssh-env` selects it and points `PATH` at it. No transfers.

**Fresh VMs, JDK pre-downloaded on your machine.**

```yaml
java:
  major: 17
  archive: ~/jdk/OpenJDK17U-jdk_x64_linux_hotspot.tar.gz
```

```bash
ducktests-remote provision --dry-run --only jdk    # validates the archive, prints the size
ducktests-remote provision --only jdk --only ssh-env
```

Rung 4 delivers to every host, unpacks to `/opt/jdk-17.0.11+9`, and `ssh-env` points at it.
Re-running transfers nothing: the manifest matches.

**A vendor JDK you must use exactly.** `java.home: /opt/corp-jdk-17`. Rungs 2–4 are skipped
entirely; a host without that directory fails by name.

**Mixed cluster, some hosts short.** Set both `java.major` and `java.archive`. Hosts that
already have a match are untouched; the rest are delivered to. One command, one honest
per-host table.

## Configuration recap

See [configuration.md § java](configuration.md#java) for the full table. The short version:
`major` is what the tests need, `home` is an exact pin, `search_paths` is where to look,
`archive` is what to fall back on, and `ssh_environment`/`bashrc` control how the answer is
made to stick.
