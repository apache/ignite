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

# Compatibility Module — Developer Notes

## Prerequisites

- [Docker](https://www.docker.com/get-started) (Docker Desktop or Docker Engine) must be installed and running.

## Running IgniteRebalanceOnUpgradeTest

This test verifies that data rebalancing works correctly when upgrading Ignite from a specific version to the current codebase.
It supports two upgrade modes:

- **DOCKER** (default) — all nodes stay in Docker containers; each node is upgraded in-place by swapping its `libs/` directory and restarting.
- **LOCAL** — the source cluster runs in Docker containers, then nodes are upgraded to local host-JVM instances.

### Step 1. Build a local Docker image for the source (old) version

Run the following script from the **project root**, passing the commit hash of the version you want to test against:

```bash
./modules/compatibility/src/test/resources/docker/build_docker_image.sh <commit_hash>
```

> **Note:** If you omit `<commit_hash>`, the script will use the hash of the latest commit in the current branch.

The script will:
1. Checkout the specified commit.
2. Build the project (`./mvnw clean install -T1C -Pall-java,licenses -DskipTests`).
3. Initialize the release (`./mvnw initialize -Prelease`).
4. Build a Docker image tagged as `apacheignite/ignite:<commit_hash>`.
5. Restore the original git state.

> **Note:** If a distribution archive already exists in `target/bin/`, the build steps will be skipped.

### Step 2. Run the test

Run `IgniteRebalanceOnUpgradeTest` from your IDE or via Maven:

```bash
./mvnw test -pl modules/compatibility -Dtest=IgniteRebalanceOnUpgradeTest -Psurefire-fork-count-1
```

---

## Upgrade Modes

### DOCKER mode (default)

All nodes stay in Docker containers throughout the test. Each node is upgraded in-place:
1. The container is gracefully stopped (`docker stop`).
2. Source jars in `/opt/ignite/apache-ignite/libs/` are replaced by target jars from the host.
3. The container is restarted (`docker start`).

The Docker image for the source cluster is the same as in LOCAL mode — only one image is needed.
The target-version jars are provided from the host filesystem.

**Option A: Automatic (recommended)** — use the `compatibility-docker` profile:

```bash
./mvnw test -pl modules/compatibility -Dtest=IgniteRebalanceOnUpgradeTest \
    -Dru.source.commit.hash=0ad4656eef09acda288cbad96f80f0138732d94a \
    -Psurefire-fork-count-1,compatibility-docker
```

The profile will automatically:
1. Check if `project/target/ignite-target-libs` symlink exists.
2. If not, check for a distribution ZIP in `project/target/bin/`.
3. If the ZIP is missing, build the project and distribution (`mvn install` + `mvn initialize -Prelease`).
4. Extract the ZIP into `project/target/bin/` (the distribution lands in `project/target/bin/apache-ignite-*-bin/`).
5. Create a symlink `project/target/ignite-target-libs` → `project/target/bin/apache-ignite-*-bin/libs/`.

> **Note:** Subsequent runs will skip the build if the symlink or the distribution ZIP already exists.

**Option B: Manual** — build, extract, and specify the libs directory:

```bash
./mvnw clean install -T1C -Pall-java -DskipTests
./mvnw initialize -Prelease
cd target/bin && unzip apache-ignite-*-bin.zip && cd ../..

./mvnw test -pl modules/compatibility -Dtest=IgniteRebalanceOnUpgradeTest \
    -Dru.source.commit.hash=0ad4656eef09acda288cbad96f80f0138732d94a \
    -Dru.target.libs.dir=target/bin/apache-ignite-<version>-bin/libs \
    -Psurefire-fork-count-1
```

### LOCAL mode

The source (old-version) cluster starts in Docker containers. During rolling upgrade each container is stopped and replaced by a local host-JVM node with the same `consistentId` and persistence directory.

- Controlled by `-Dru.upgrade.mode=LOCAL`.

```bash
./mvnw test -pl modules/compatibility -Dtest=IgniteRebalanceOnUpgradeTest \
    -Dru.upgrade.mode=LOCAL \
    -Dru.source.commit.hash=0ad4656eef09acda288cbad96f80f0138732d94a \
    -Psurefire-fork-count-1
```

---

## System Properties

| Property | Default                                   | Class | Description |
|----------|-------------------------------------------|-------|-------------|
| `ru.upgrade.mode` | `DOCKER`                                  | `IgniteRebalanceOnUpgradeTest` | Upgrade mode: `LOCAL` or `DOCKER` |
| `ru.source.commit.hash` | `0ad4656eef09acda288cbad96f80f0138732d94a` | `IgniteRebalanceOnUpgradeTest` | Commit hash for the source (old-version) Docker image `apacheignite/ignite:<commit>` |
| `ru.target.libs.dir` | `<project.dir>/target/ignite-target-libs` | `IgniteContainer` | Host directory with target-version jars (DOCKER mode only) |
| `ru.local.work.dir` | `<project.dir>/target/test-ignite-work`   | `IgniteContainer` | Local directory bind-mounted as Ignite work directory (persists across container restarts) |
