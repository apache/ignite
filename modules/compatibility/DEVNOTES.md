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

This test verifies that data rebalancing works correctly when upgrading Ignite from a specific version to the current codebase. It uses Docker containers running the "old" Ignite version.

### Step 1. Build a local Docker image for the source (old) version

Run the following script from the **project root**, passing the commit hash of the version you want to test against:

```bash
./modules/compatibility/src/test/resources/docker/build_docker_image.sh <commit_hash>
```

> **Note:** If you omit `<commit_hash>`, the script will use the hash of the latest commit in the current branch.

> **Note:** If your corporate network uses SSL inspection and you get `TLS: server certificate not trusted`, the script will automatically patch Dockerfiles to use HTTP repositories and restore them after the build.

The script will:
1. Checkout the specified commit.
2. Build the project (`./mvnw clean install -T1C -Pall-java,licenses -DskipTests`).
3. Initialize the release (`./mvnw initialize -Prelease`).
4. Build a Docker image tagged as `apacheignite/ignite:<commit_hash>`.
5. Restore the original git state.

> **Note:** If a distribution archive already exists in `target/bin/`, the build steps will be skipped.

### Step 2. Set the commit hash in the test

Open `org.apache.ignite.compatibility.ru.IgniteRebalanceOnUpgradeTest` and set `SOURCE_COMMIT_HASH` to the **same commit hash** used in Step 1:

```java
private static final String SOURCE_COMMIT_HASH = "<commit_hash>";
```

### Step 3. Run the test

Run `IgniteRebalanceOnUpgradeTest` from your IDE or via Maven:

```bash
./mvnw test -pl modules/compatibility -Dtest=IgniteRebalanceOnUpgradeTest
```
