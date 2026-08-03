---
name: run-test
description: Run a single Apache Ignite 2.x JUnit test (or test class/method/suite) locally via ./mvnw with the exact flags Ignite needs. Use whenever asked to run, reproduce, or debug a specific test in this repo.
---

# Running a single Ignite test

Apache Ignite 2.x tests are heavyweight integration tests (they start real grids). Running one
correctly requires several non-obvious flags. Get any of them wrong and you get either
"No tests matching pattern", a reactor abort, or an `InaccessibleObjectException` at JVM startup.

## The command

```bash
./mvnw test -o -pl :ignite-<module> -am \
  -Plgpl,examples,-clean-libs,-release \
  -DFORK_COUNT_SET_TO_1 \
  -Dmaven.test.failure.ignore=true \
  -DfailIfNoTests=false \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest='<TestClass>#<method>'
```

Module artifact ids are `ignite-<dir>`: `modules/core` → `:ignite-core`, `modules/indexing` →
`:ignite-indexing`, etc.

Read the result from `modules/<module>/target/surefire-reports/*<TestClass>.txt`, not just the
console — the `.txt` has the full stack trace.

## Why each flag matters (all confirmed the hard way)

- **`-DFORK_COUNT_SET_TO_1`** — THE critical one. The parent pom sets `<forkCount>0</forkCount>`,
  so tests run inside Maven's own JVM and the surefire `<argLine>` (`--add-opens ...`) is IGNORED.
  Without forking, tests die at startup with
  `InaccessibleObjectException: ... module java.base does not "opens java.nio"`.
  This property activates the `surefire-fork-count-1` profile in `parent/pom.xml`.
  (Setting `MAVEN_OPTS` does NOT reliably fix this — use this flag.)
- **`-pl :ignite-<module>`** — scope to the test's module. Without it Maven runs surefire across
  every reactor module and aborts on the first one with no match (e.g. `ignite-tools`).
- **`-am`** — builds upstream modules from source in-reactor. Needed when the dependency jars
  aren't installed in `~/.m2` (e.g. after a `package`-only build). Once `:ignite-core` etc. are
  installed you can drop `-am` for speed.
- **`-Dsurefire.failIfNoSpecifiedTests=false`** — suppresses "No tests matching pattern" on the
  other reactor modules. NOTE: `-DfailIfNoTests=false` is a DIFFERENT property and does NOT
  suppress it; keep both.
- **`-Plgpl,examples,-clean-libs,-release`** — the documented profile set (see `DEVNOTES.txt`).
  The leading `-` disables `clean-libs` and `release`.
- **`-Dmaven.test.failure.ignore=true`** — keeps the build green so you can read the report even
  when the test fails (useful when looping).
- **`-o`** — offline; faster and avoids network. Drop `-o` only if Maven complains an artifact was
  never downloaded (e.g. the first time it needs `surefire-junit4`; fetch with
  `./mvnw dependency:get -Dartifact=org.apache.maven.surefire:surefire-junit4:<ver>`).

## Parameterized tests (`@RunWith(Parameterized.class)`)

A bare `#method` matches 0 invocations. Use a wildcard so it matches the per-parameter names
(`method[persist=true]`):

```bash
-Dtest='StatisticsConfigurationTest#updateStatisticsOnChangeTopology*'
```

## Whole class or a suite

```bash
-Dtest='DiscoveryUnmarshalVulnerabilityTest'          # whole class (all methods / params)
-Dtest='org.apache.ignite.testsuites.IgniteBasicTestSuite'   # a suite (no method gymnastics)
```

## Prerequisites

The target module + its deps must be built. Either rely on `-am` (above), or pre-build once:
`./mvnw package -pl :ignite-<module> -am -DskipTests -Pall-java -Dcheckstyle.skip=true -Drat.skip=true -Dmaven.javadoc.skip=true`.

## Reviewing/reproducing someone else's PR

Fetch it and run inside a disposable worktree so `master` stays clean:

```bash
git fetch us pull/<N>/head:pr-<N>          # 'us' = apache/ignite remote
git worktree add -f ../ignite-pr-<N> pr-<N>
# build + run the test inside ../ignite-pr-<N> with the command above
git worktree remove --force ../ignite-pr-<N>
```
