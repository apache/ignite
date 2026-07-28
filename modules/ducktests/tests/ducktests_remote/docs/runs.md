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

# Runs

## The run directory

Everything about a run lives on the runner, under
`<state_root>/runs/<run-id>/`:

| File | Written by | Contents |
| --- | --- | --- |
| `meta.json` | `run`, at launch | run id, CLI version, coordinator user/host/platform, runner, cluster name, node list, test paths, work dir, results root, start time, and a redacted config summary |
| `cluster.json` | `run` | exactly what ducktape was given |
| `globals.json` | `run`, mode `0600` | the composed globals, secrets resolved |
| `parameters.json` | `run`, mode `0600` | present only when `parameters` is non-empty |
| `run.sh` | `run`, mode `0755` | the exact ducktape command. SSH in and execute it to reproduce the run by hand |
| `launch.sh` | `run`, mode `0755` | wrapper that waits on `run.sh` and records the exit code |
| `pid`, `pgid` | `launch.sh` / the detach script | what `stop` signals |
| `exit_code` | `launch.sh`, last | written after ducktape exits |
| `stopped` | `stop` | marker that distinguishes "stopped" from "failed" |
| `ducktape.log` | `launch.sh` | combined stdout and stderr |
| `results/` | ducktape | `--results-root`, with ducktape's own `latest` symlink inside |

Alongside: `<state_root>/runs/latest` points at the newest run, and
`<state_root>/src/<run-id>/` holds that run's synced sources.

Run ids look like `max-20260727-141233-9f2a`: the account and timestamp make a directory
listing readable, and four hex characters keep two runs started in the same second apart.

## Reproducing a run by hand

```bash
ssh build-vm-01
cd ~/.ducktests-remote/runs/max-20260727-141233-9f2a
cat run.sh          # every path is shell-quoted; nothing is hidden
bash run.sh
```

`run.sh` activates the venv and execs ducktape with `--results-root`, `--cluster-file`,
`--globals` and the test paths. `--globals` takes a *file path*: ducktape 0.13 checks
`os.path.isfile` before parsing the argument as JSON, so the composed blob never crosses a
command line and never lands in shell history or a process listing.

## Run states

`status` derives the state from three observable facts — is the pid alive, is there an
`exit_code`, is there a `stopped` marker:

| State | Meaning |
| --- | --- |
| `running` | pid alive, no exit code yet |
| `finished` | exit code 0 |
| `failed` | non-zero exit code, no `stopped` marker |
| `stopped` | exit code present *and* a `stopped` marker, or the marker with no live pid |
| `unknown` | no pid, no exit code, no marker — usually a run that died before `launch.sh` got going |

`exit_code` wins over liveness because the file is written last: a process that has already
exited is never reported as running just because its pid got reused.

## Detach, follow, reattach

`run` detaches from second zero — `setsid nohup bash launch.sh`, with `nohup` plus `disown`
where `setsid` is missing — and then attaches to the log. `setsid` matters: it puts
ducktape in its own session so a dropped SSH connection cannot SIGHUP it and leave Ignite
JVMs alive on every worker.

Consequences:

- **Ctrl-C detaches; it does not stop the run.** A second Ctrl-C within 3 seconds offers to
  stop it (and simply detaches in a non-interactive shell).
- `--detach` skips following entirely and prints the reattach commands.
- `logs <run-id> -f` reattaches from **any** coordinator, including one that did not start
  the run.
- Following is just streaming a file by byte offset, so it costs the runner nothing and can
  be interrupted freely.

## Stopping

`stop` touches `stopped`, SIGTERMs the process **group**, waits `--timeout` seconds
(default 60), and with `--kill` SIGKILLs survivors. If the process never wrote an exit code,
`143` is recorded. Then it cleans the workers, unless `--no-clean`.

The follow-up clean is the important half: killing ducktape does not kill the Ignite JVMs
it started on twelve machines, and those are what break the *next* run. See
[commands.md § clean](commands.md#clean).

## Exit codes

| Code | Meaning |
| --- | --- |
| `0` | success |
| `1` | usage or configuration error — a bad key, a missing file, an unset `${env:}` |
| `2` | preflight failed; `run` stopped before creating anything |
| `3` | reserved: this deployment has a single runner and takes no cluster lease |
| `4` | ducktape ran and reported test failures |
| `5` | transport or infrastructure error |
| `130` | interrupted by the operator |

`4` and `5` are deliberately distinct: Jenkins needs "tests failed" separate from "the
cluster is broken". Note that ducktape itself exits `1` both for test failures and for its
own startup errors, so a `4` strictly means "ducktape ran and exited non-zero" — the log
distinguishes the two.

## Getting results back

```bash
ducktests-remote fetch                 # newest run, reports only
ducktests-remote fetch <run-id> --full # the whole results tree
```

Results land in `./ducktests-results/<run-id>/`, with `ducktape.log` beside them.
`globals.json` is always excluded.

The results also stay on the runner indefinitely — nothing is pruned automatically. On a
long-lived runner, `<state_root>/runs` and `<state_root>/src` grow one directory per run;
`status --all` lists them, and removing old ones is an ordinary `rm -rf` on the runner.

## Jenkins

```bash
ducktests-remote --profile ise-perf run -t "$TC_PATHS" --detach
ducktests-remote status --json > status.json
```

`--detach` returns immediately with the run id; `status --json` is machine-readable; the
exit code separates test failures from infrastructure failures. Because run state lives on
the runner, a job that times out and is retried can pick the same run back up rather than
starting a second one on top of it.
