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

# Internals

For changing the CLI rather than using it.

## Module map

| Module | Responsibility |
| --- | --- |
| `cli.py` | argument parsing, the shared `Context`, `Console`, exit-code mapping |
| `config.py` | defaults, layering, validation, `${env:}`/`${file:}` interpolation |
| `globals_builder.py` | the `--globals` payload and the `Redactor` |
| `cluster.py` | inventory → `Node` list → ducktape's `cluster.json` |
| `transport.py` | the only place that shells out to `ssh`/`scp` |
| `fanout.py` | bounded parallel per-host execution and the result table |
| `runs.py` | run ids, run directory layout, state derivation, script rendering |
| `sshdiag.py` | SSH failure classification and the administrator block |
| `java.py` | JDK discovery, environment files, archive inspection |
| `pipconf.py` | pip index configuration → command-line flags |
| `commands/*.py` | one module per subcommand, each with `register()` and `execute(ctx)` |
| `templates/run.sh.tmpl` | the generated ducktape command |
| `checks/` | unit checks; no network, no Docker, no ducktape |

`pipconf` is not called `pip` so that it can never shadow the real `pip` package for
anything that ends up with this directory on `sys.path`.

## Context

`Context` (in `cli.py`) is what every command receives. It holds the composed config, the
parsed args and the console, and lazily builds transports:

- `ctx.nodes` — inventory, truncated by `-n`
- `ctx.all_nodes` — inventory plus `extra_hosts`; used by `provision`, `deploy`, `clean`,
  `doctor`, `keys`
- `ctx.runner` — a transport to the runner, created once
- `ctx.worker(node)` — a transport per worker, cached by host
- `ctx.state_root_resolved()` — the state root with `~` expanded against the *runner's* home

A subtlety worth knowing: `cluster.identity_file` is a runner-side path, so the coordinator
uses it only when a file of that name happens to exist locally; otherwise the system ssh
client falls back to `~/.ssh/config` and the agent.

## Transport

Three implementations behind one interface:

| Class | Used for |
| --- | --- |
| `LocalTransport` | `--runner local`, and anything else on this machine |
| `SshTransport` | a remote runner or a worker, through the **system** `ssh`/`scp` |
| `ProxiedTransport` | a second hop: `deploy --via`, and probing a worker over the connection ducktape itself will make |

The interface is `run(argv)`, `run_script(script)`, `upload`, `download`, `upload_dir`,
`exists`, `mkdirs`, `write_file`, `read_file`, `home`, `expand`.

Design decisions that are load-bearing:

- **`argv` is always a list, never a shell string.** Quoting is done once, in the transport.
- **`run_script` feeds a script to `bash -s` over stdin.** Long quoted one-liners are the
  single largest source of remote-execution bugs; anything longer than a couple of words
  belongs in a script.
- **The system ssh client, not paramiko.** It brings `~/.ssh/config`, `ProxyJump`, agent and
  Kerberos support for free, and it is the same client an engineer uses by hand when
  reproducing a failure.
- **Connection multiplexing** (`ControlMaster`) is enabled everywhere except Windows, where
  OpenSSH has no support for it and the options fail hard. `doctor` and `deploy` open many
  connections per host; multiplexing makes every extra one a free channel.
- **`write_file` writes bytes with explicit LF endings.** A coordinator on Windows would
  otherwise translate every newline to CRLF, and a shell script with carriage returns fails
  on the runner with a message naming the wrong line.
- **`expand`** resolves a leading `~` against the *remote* home, once per connection, because
  paths are shell-quoted before they reach the remote side and a literal tilde would never
  be expanded there.
- **`has_rsync()` is probed once per transport and cached.** Both the source sync and
  `deploy`'s incremental path ask for it; a mixed cluster where one host lacks rsync falls
  back per host rather than for the whole run. `deploy` does not route rsync through the
  transport, though — it runs rsync on the coordinator with the transport's own
  `ssh_options()` in `-e`, because the payload never passes through a shell.

## Fan-out

`fanout(hosts, operation, jobs=…, fail_fast=…)` runs `operation(host)` in a bounded thread
pool and returns results **in inventory order**. An exception inside one operation becomes
that host's `FAILED` result rather than killing the batch — per-host isolation is the whole
point. Statuses: `ok`, `changed`, `skipped`, `warn`, `failed`.

`render_table` prints failures' detail by default and everything's detail with `-v`;
`summarise` prints `9 ok, 2 failed`, ordered worst-first.

## Redaction

`Redactor` keys on resolved **values**, not on key names. Anything coming out of `${env:}`
or `${file:}` is registered at config load, and `Console` runs every line through it, so
redaction cannot be bypassed by printing from an unusual place. `redact_structure` also
masks a small set of sensitive key names as a fallback for values the CLI never resolved
itself. Independently, `pipconf.mask_credentials` masks `user:password@` in URLs.

If you add a new output path, print through `Console` — not `print`.

## Adding a command

1. Create `commands/<name>.py` with `register(subparsers, common)` and `execute(ctx)`.
2. Add it to the import and the loop in `cli.build_parser`. The order there is the order in
   `--help`.
3. Return one of the `EXIT_*` constants from `cli.py`.
4. Support `--dry-run` honestly: print what would happen, execute nothing, probe nothing.
5. Route every remote action through `ctx.runner` / `ctx.worker(node)`, and every host loop
   through `fanout`.
6. Add checks under `checks/check_remote_<name>.py` using `FakeTransport`.

## Conventions

- ASF licence header on every file, Python and Markdown alike.
- flake8, max line length **120**; config in `tox.ini`.
- Docstrings in the `:param:` / `:return:` style used throughout.
- Comments explain *why*, not *what*. The reason a decision was made is the part that
  cannot be recovered from the code later.
- No new runtime dependency. The package uses the standard library plus `PyYAML`, and
  **never imports ducktape**.

## Tests

```bash
cd modules/ducktests/tests
pytest ducktests_remote/checks      # unit only: no network, no Docker, no ducktape
flake8 ducktests_remote
```

`[pytest]` in `tox.ini` collects `check_*.py` files, `Check` classes and `check_*`
functions, which is why the files are named that way. `checks/fake_transport.py` provides a
recording transport that simulates a small filesystem and returns canned output for
commands matching a needle. The only subprocess in the suite is the deliberate one that
proves the ducktape import boundary still holds.

Checks are named as sentences — `check_lists_replace_and_do_not_concatenate` — because the
name is the specification and shows up in the failure output.

## Where the ignitetest facts come from

Several behaviours are pinned to things in `ignitetest` rather than invented here. When
those move, these move:

| Fact | Source |
| --- | --- |
| distribution home is `<install_root>/<product>` | `services/utils/path.py`, `services/utils/ignite_aware.py` |
| version strings normalise (`ise--6` → `ise-6`) | `utils/version.py` |
| `persistent_root` defaults to `/mnt/service` | `services/utils/path.py` |
| `sudo iptables` is used by exactly two suites | `IgniteAwareService.drop_network` |
| the four Ignite main classes `clean` must match | `services/ignite.py`, `services/ignite_app.py`, `services/utils/cdc/*` |
| Java major parsing | `services/utils/jvm_utils.py` |
| which Java consumers use `PATH` vs `JAVA_HOME` | `services/utils/ignite_spec.py`, `jvm_utils.py`, `kafka/kafka.py`, `jmx_utils.py` |
| the package list and the JDK version | `docker/Dockerfile` |
| the ducktape pin | `docker/requirements.txt` |

`docker/Dockerfile` is the source of truth for what a prepared node looks like. Where it
and this CLI disagree, the Dockerfile wins — and the derived lists in `config.py` carry a
comment saying so, to keep the drift visible at the next review.
