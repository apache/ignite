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

# ducktests-remote manual

`../README.md` is the tour: install it, describe a cluster, get a run going. This is the
reference: every command, every configuration key, and — the part a README cannot carry —
**exactly what happens in what order, and what crosses the network when**.

The organising rule of these documents: when the answer is "it depends", the condition is
written down. If you find yourself guessing, that is a documentation bug worth fixing.

## The documents

| Document | What it answers |
| --- | --- |
| [concepts.md](concepts.md) | Which machine is which, what lives where, and the invariants the whole tool rests on |
| [configuration.md](configuration.md) | Every config key, its default, who reads it; layering, `${env:}`, secrets |
| [commands.md](commands.md) | Every command and flag, and the exact order of operations inside each |
| [java.md](java.md) | How the workers' JVM is chosen, and precisely when a JDK is copied to them |
| [runs.md](runs.md) | The run directory, run states, detach/follow/stop, exit codes |
| [troubleshooting.md](troubleshooting.md) | Symptom → cause → fix, for the failures this cluster actually produces |
| [internals.md](internals.md) | Module map, the transport contract, fan-out, redaction; where to change things |

## Find an answer fast

**"When does X get sent to the workers?"**
Nothing is ever sent implicitly. Three commands transfer files, and nothing else does:
`deploy` (distributions), `provision --only jdk` (a JDK, and only to hosts that lack one),
`keys push` (the public key). `run` uploads only to the *runner*. See
[commands.md § What each command transfers](commands.md#what-each-command-transfers).

**"Why did my test get the wrong Java?"** → [java.md](java.md), and in particular
[§ Why PATH decides, not JAVA_HOME](java.md#why-path-decides-not-java_home).

**"Where does this setting come from?"** → [configuration.md § Layering](configuration.md#layering)
then `--dry-run`, which prints the composed result of every layer.

**"What does this command actually do?"** → [commands.md](commands.md). Each entry lists
its steps in execution order, what it reads, what it writes, and what `--dry-run` skips.

**"The run ended — where is everything?"** → [runs.md § The run directory](runs.md#the-run-directory).

**"Is it safe to run twice?"** Yes, for every command; the per-command entry says how
idempotence is achieved. `clean` and `stop` are the two that destroy things, and both have
a `--dry-run` that prints the exact kill list and path list.

**"What does this exit code mean?"** → [runs.md § Exit codes](runs.md#exit-codes).

## Two habits worth having

**`--dry-run` first.** It is genuinely side-effect free on every command: no probes, no
uploads, no processes. It prints the commands it would run and the files it would generate,
including the rendered `run.sh`, `cluster.json` and a redacted `globals.json`. Most
questions about "what would this do" are answered faster by running it than by reading.

**`-v` when a message is not enough.** Verbose prints every command as it is issued
(`+ host$ ...`) and the full per-host output rather than only the failures.

## Conventions in these documents

- **coordinator**, **runner**, **worker** are used precisely; see
  [concepts.md](concepts.md#the-three-roles). A sentence that does not name one of them is
  about all three.
- Paths like `services/utils/path.py` are relative to `modules/ducktests/tests/ignitetest/`;
  paths like `commands/run.py` are relative to `modules/ducktests/tests/ducktests_remote/`.
- Code references name a file and a function rather than a line number, so they survive
  edits: "`_deliver_jdk` in `commands/provision.py`".
