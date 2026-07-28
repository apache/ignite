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

# Configuration reference

Every default in this document comes from `DEFAULTS` in `config.py`. When the two
disagree, `config.py` is right and this file is a bug.

## Layering

Later layers win. Dicts deep-merge; **every other type, lists included, replaces
outright** — a later layer must be able to *shrink* a list, which is exactly what an
override is for.

1. built-in defaults (`config.py`)
2. `~/.ducktests-remote/config.yaml`
3. `--config FILE`, repeatable, applied in order
4. `--profile NAME`, repeatable, applied in order
5. `DTR_*` environment variables
6. explicit command-line flags
7. `-g KEY=VALUE` / `-p KEY=VALUE` — `globals` and `parameters` only, applied by
   `globals_builder`

After merging, `${env:}` / `${file:}` placeholders are resolved everywhere except
`globals` and `parameters`, which `globals_builder` resolves per layer so its errors can
name the profile a placeholder came from.

**Unknown keys are a hard error**, with a "did you mean" suggestion from `difflib`:

```
ERROR profile.yaml: unknown config key 'cluster.instal_root'; did you mean 'install_root'?
```

A typo silently ignored in a config that drives a three-hour run is expensive. The two
free-form sections, `globals` and `parameters`, are passed through unvalidated by design —
they belong to ducktape and to the tests, not to this CLI.

Files are parsed as **YAML or JSON by content, not by extension**: a document starting with
`{` or `[` is parsed as JSON, anything else as YAML.

### Profiles

`--profile NAME` searches, in order:

1. `<profiles_dir>/NAME.yaml`, `.yml`, `.json`, or `NAME` exactly
2. the same four under the current directory
3. `examples/profile-NAME.*` and `examples/NAME.*` shipped in the package

A profile is a normal config document; it is not restricted to `globals`.

### Environment variables

- `DTR_CLUSTER__RUNNER=build-vm-01` → `cluster.runner`. A **double** underscore separates
  path segments, so single underscores stay usable inside key names
  (`DTR_RUN__MAX_PAYLOAD_MB` → `run.max_payload_mb`).
- Short aliases: `DTR_RUNNER` → `cluster.runner`, `DTR_JOBS` → `jobs`, `DTR_CLUSTER` →
  `cluster.name`, `DTR_STATE_ROOT` → `cluster.state_root`, `DTR_INSTALL_ROOT` →
  `cluster.install_root`, `DTR_USER` → `cluster.user`.
- `DTR_CONFIG` and `DTR_PROFILE` are ignored here (they select files, not values).
- **Any other single-segment `DTR_*` variable is ignored.** Profiles interpolate secrets
  with `${env:DTR_SOMETHING}`, and treating every such variable as a config path would turn
  a password into a config error. The `__` form is still validated, so a typo there is
  caught.
- Values are parsed as JSON when they parse: `DTR_JOBS=8` is an integer, `DTR_RUNNER=vm` is
  a string.

## Sections

### `cluster`

| Key | Default | Meaning |
| --- | --- | --- |
| `name` | `default` | label only; appears in `meta.json` and `status` |
| `user` | your `$USER` | SSH account on the workers. Never defaults to `ducker` — that account exists only in the Docker image |
| `identity_file` | `~/.ssh/id_rsa` | private key **as the runner sees it**; this is the path written into `cluster.json` and opened by ducktape |
| `port` | `22` | default SSH port, per-host overridable |
| `install_root` | `/opt` | where distributions live on the workers; `<install_root>/<product>` is what ignitetest resolves a version to |
| `runner` | `local` | host running ducktape, or `local` |
| `state_root` | `~/.ducktests-remote` | runner-side root for the venv, sources and run directories |
| `nodes` | `[]` | the inventory; see below |
| `extra_hosts` | `[]` | hosts that `provision`/`deploy`/`clean`/`doctor` should also target but that ducktape must **not** schedule Ignite nodes onto — typically the runner itself |

`identity_file` is the single most misread key. It is a **runner-side** path. If the runner
is not the coordinator, a file that exists on your laptop proves nothing; `doctor` checks
it on the runner and `keys push` installs it there.

#### The inventory

An entry is either a bare string or a mapping with `host` and optional `ip`, `user`,
`port`, `identity_file`:

```yaml
nodes:
  - host: worker[01-12].dc.local     # expands to twelve hosts
  - host: worker13.dc.local
    ip: 10.0.0.13                    # -> externally_routable_ip in cluster.json
  - host: worker14.dc.local
    user: someone-else               # per-host override, for mixed clusters
  - 10.0.0.15                        # bare string is shorthand for {host: ...}
```

- **Ranges**: `[01-12]` zero-pads to the width of the lower bound, so `worker01`…`worker12`;
  `[1-12]` gives `worker1`…`worker12`. Several ranges in one pattern expand as a cartesian
  product, left to right. `ip` cannot be combined with a range.
- Duplicate hosts are an error. An unknown key inside an entry is an error.
- `-n/--num-nodes N` takes the **first N** entries, the analogue of
  `IGNITE_NUM_CONTAINERS` in `docker/run_tests.sh`. Asking for more than the inventory
  holds is an error naming both numbers.

The generated `cluster.json` is exactly what `ducktape.cluster.json.JsonCluster` reads:
a `nodes` list of `{externally_routable_ip, ssh_config}`, where `ssh_config` is passed
into `RemoteAccountSSHConfig(host, hostname, user, port, password, identityfile)`.
`--cluster-file` bypasses generation entirely and uploads your file byte for byte.

### `runner`

| Key | Default | Meaning |
| --- | --- | --- |
| `venv` | `null` | explicit venv path on the runner. Unset means `<state_root>/venv` |
| `python` | `python3` | interpreter used to create the venv and to probe for ducktape |
| `create_venv` | `true` | when false and `venv` is unset, no venv is used and ducktape is expected on `PATH` |
| `requirements` | `null` | requirements file; unset means `<work_dir>/modules/ducktests/tests/docker/requirements.txt`, so the ducktape pin has one source of truth |

### `pip`

Read only on the runner, by `run`: the venv install and `--install-sources`. Workers never
run pip. Full treatment in [commands.md § run](commands.md#run).

| Key | Default | pip flag |
| --- | --- | --- |
| `index_url` | `null` | `--index-url` |
| `extra_index_url` | `[]` | `--extra-index-url`, repeated; a bare string is accepted |
| `trusted_host` | `[]` | `--trusted-host`, repeated; a bare string is accepted |
| `timeout` | `null` | `--timeout`, seconds |
| `retries` | `null` | `--retries` |
| `cert` | `null` | `--cert`; a **runner-side** path to a CA bundle |

With nothing set, no flags are added at all and the rendered commands are byte for byte
what they were before this section existed.

### `java`

Read by `provision` (`jdk`, `ssh-env`) and by `doctor`. Full treatment in
[java.md](java.md).

| Key | Default | Meaning |
| --- | --- | --- |
| `major` | `17` | Java major version the tests need. Derived from the Dockerfile's `ARG jdk_version="eclipse-temurin:17"`; the Dockerfile wins if they ever disagree |
| `home` | `null` | an explicit JDK home on the workers. Set means *exactly this*: no search, and a host without it fails |
| `search_paths` | `[/opt, /usr/lib/jvm, /usr/java]` | where to look for an existing JDK. Each entry may be a directory *of* JDK homes or a JDK home itself |
| `archive` | `null` | coordinator-side JDK tarball (`.tar.gz`/`.tgz`/`.tar`/`.tar.bz2`/`.tar.xz`) or an unpacked directory, delivered to hosts that have no matching JDK |
| `install_root` | `null` | where a delivered JDK is unpacked; unset means `cluster.install_root` |
| `name` | `null` | target directory name; unset means the name of the JDK home found inside the archive |
| `ssh_environment` | `true` | write `~/.ssh/environment` |
| `bashrc` | `true` | write a marked block at the top of `~/.bashrc` |

Setting both `ssh_environment` and `bashrc` to false is an error: there would be no way to
put the JDK on the workers' non-interactive `PATH`.

### `run`

| Key | Default | Meaning |
| --- | --- | --- |
| `source_root` | `null` | Ignite checkout synced to the runner; unset means the checkout containing the current directory, found by walking up |
| `work_dir` | `null` | runner-side Ignite checkout to run from; ducktape itself runs in its `modules/ducktests/tests`; unset means the synced source directory |
| `exclude` | `[]` | extra sync exclusions, appended to the built-in list |
| `max_payload_mb` | `200` | refuse to sync more than this. A build directory leaking into the payload is the usual cause |
| `install_sources` | `false` | `pip install -e` the synced sources. Not needed for discovery: ducktape's loader walks up from each test file while `__init__.py` exists and puts the resulting top-level directory on `sys.path` |

Built-in sync exclusions: `.git`, `target`, `results`, `__pycache__`, `*.pyc`, `.idea`,
`venv`, `.venv`, `*.egg-info`, `.tox`, `.pytest_cache`. A `.ducktestsignore` file at the
source root replaces the built-in list; `--exclude` replaces both.

### `deploy`

| Key | Default | Meaning |
| --- | --- | --- |
| `dist_dir` | `./dist` | one subdirectory per distribution |
| `install_root` | `null` | target root; unset means `cluster.install_root` |
| `sudo` | `false` | use `sudo -n` for the remote side |
| `owner` | `null` | `chown -R` the extracted tree |
| `staging_dir` | `/tmp/ducktests-remote-staging` | where `--via` parks the payload |
| `checksum` | `false` | hash file contents for the manifest instead of size+mtime |

### `provision`

| Key | Default | Meaning |
| --- | --- | --- |
| `packages` | see below | system utilities, derived from `docker/Dockerfile` |
| `install_jdk` | `false` | allow the distribution's JDK package as the last rung |
| `ssh_env_path_extra` | `[]` | extra `PATH` entries appended in `~/.ssh/environment` |
| `dirs` | `[/mnt/service]` | directories to create and chown; `install_root` is always added |

Default packages: `sudo`, `netcat-traditional`, `iptables`, `rsync`, `unzip`, `wget`,
`curl`, `jq`, `coreutils`, `net-tools`. The Dockerfile is the source of truth; image-only
entries (`openssh-server`, `vim`, `mc`, build toolchain) are deliberately not replicated —
a real VM already has an sshd, and no compiler is needed to *run* tests.

### `clean`

| Key | Default | Meaning |
| --- | --- | --- |
| `process_pattern` | `org.apache.ignite` | `pgrep -f` pattern |
| `paths` | `[/mnt/service]` | directories to remove |
| `allowed_roots` | `[/mnt, /tmp, /var/tmp]` | every path in `paths` must sit under one of these |

The default pattern covers all four main classes ignitetest launches:
`CommandLineStartup`, `CdcCommandLineStartup`, `IgniteAwareApplicationService`,
`KafkaToIgniteCommandLineStartup`.

### `ssh`, `jobs`, `profiles_dir`

| Key | Default | Meaning |
| --- | --- | --- |
| `ssh.connect_timeout` | `15` | seconds, passed as `ConnectTimeout` to every connection |
| `jobs` | `16` | fan-out parallelism |
| `profiles_dir` | `~/.ducktests-remote/profiles` | where `--profile` looks first |

### `globals` and `parameters`

Free-form. `globals` becomes ducktape's `--globals` payload; `parameters` becomes
`--parameters`. Neither is validated by this CLI, both are deep-merged like everything
else, and both are written to `0600` files on the runner rather than onto a command line.

`-g` / `-p` take dotted overrides whose values are parsed as JSON when they parse, so
`-g ssl.enabled=true` is a boolean and `-g project=ise` is a string. This mirrors
`_extend_json` in `docker/run_tests.sh`, with nesting added.

## Placeholders

`${env:NAME}` and `${file:PATH}` are resolved **on the coordinator, at launch**, in every
section:

```yaml
cluster:
  user: ${env:DTR_SSH_USER}
pip:
  index_url: ${env:NEXUS_URL}
globals:
  authentication:
    password: ${file:~/.secrets/ise-password}
```

A missing variable or an unreadable file is a hard error naming both the placeholder and
where it came from — never an empty string, never a run that fails on authentication three
hours later. `${file:}` strips surrounding whitespace, which is what you want for a file
holding one token.

## Secrets and redaction

- Every value resolved from `${env:}` or `${file:}` is registered with a `Redactor` and
  replaced by `***` in everything the CLI prints: normal output, `--dry-run`, streamed
  logs, warnings, error messages, and the config summary inside `meta.json`.
- Redaction is keyed on the **value**, so the same secret is caught in an unrelated field
  or in a rendered command line. Key-name matching (`password`, `passwd`, `secret`,
  `token`, `keystore_pass`, `truststore_pass`) is only a fallback for values the CLI never
  resolved itself.
- Values shorter than three characters are not registered — masking `a` everywhere would
  destroy the output.
- `pip` index URLs get a second, independent treatment: any `user:password@` in a URL is
  masked wherever the CLI prints it, even when the URL was typed straight into a config
  file and the redactor has never seen it.
- The composed `globals.json` is written `0600` on the runner, and `fetch` always excludes
  it.
- The example profiles in `examples/` contain no real hostnames, addresses, accounts or
  passwords, and must stay that way: this directory is in a public Apache repository.

## Seeing the result

```bash
ducktests-remote run --dry-run -t ./ignitetest/tests/smoke_test.py
```

prints the composed `cluster.json`, the redacted `globals.json`, and the rendered `run.sh`
without creating, uploading or starting anything. `doctor` additionally prints the
effective pip index and the resolved identity path. When two configurations disagree, diff
their `--dry-run` output rather than reasoning about the layering.
