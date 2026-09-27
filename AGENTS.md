# AGENTS.md

## What this repo is

- Apache Ignite 2 (Java 17, Maven multi-module) forked as `junphine/ignite` (`origin`), with `upstream` = `apache/ignite`.
  The fork regularly merges upstream via commits named `upgrade from upstream` (often with Chinese commit messages/README text).
  Keep fork changes localized; avoid opportunistic refactors of upstream code — periodic upstream merges are expected.
- Version is `2.16.999-SNAPSHOT`, set once via `<revision>` in `parent/pom.xml`.
- Fork-specific features (not in upstream):
  - **Lucene full-text / vector / hybrid search** — `modules/indexing`
    (`org.apache.ignite.cache.FullTextLucene`, `VectorQueryIndex`, `LuceneConfiguration`, `HybridTextQuery`, `GridLuceneIndex`),
    annotation `@QueryVectorField` in `modules/commons`. Runtime config is Spring beans in `$IGNITE_HOME/config/lucene.xml`
    (bean `default`, or a bean named after the cache). Working example: `examples/src/main/java/org/apache/ignite/examples/datagrid/HybridSearchExample.java`.
    Fork commits also touch `modules/commons` (`QueryEntity`) and core query code (`GridCacheQueryManager`, `QueryUtils`).
  - **Vert.x REST + MCP server** — `modules/vertx-rest`: plugin `IgniteVertxPluginProvider` (registered via
    `META-INF/services`, only starts when the Ignite instance name starts with `vertx-`), spring-mvc-style annotations
    under `io.vertx.webmvc`, MCP endpoint `POST /mcp` (`io.vertx.webmvc.mcp`). Has its own tests in `src/test`.
  - **`web-console/`** — a *separate* Maven reactor (its own `pom.xml`, not listed in the root `pom.xml`): Spring Boot 3
    server, `web-agent`, and `quercus-webapps` (PHP apps such as mongoAdmin). It consumes ignite `2.16.999-SNAPSHOT`
    artifacts, so install the root reactor first, then build with `mvn -f web-console/pom.xml ...`.
- `README.md` describes Mongo/Gremlin/Redis/ES protocol features. `MongoPluginConfiguration` / `GremlinPluginConfiguration`
  mentioned there exist **nowhere in this repo** — don't go hunting for them; only the admin/client pieces live here.
- `DEVNOTES.txt`, `CONTRIBUTING.md` and `web-console/DEVNOTES.txt` are partly stale: profiles `check-licenses`, `platforms`,
  `clean-libs`, `web-console` referenced there no longer exist in any `pom.xml` (Maven only prints a warning). Trust the poms and `.github/workflows/`.

## Build and verify (order matters)

Always use the wrapper `./mvnw` (Maven 3.9.6), JDK 17.

1. Compile everything: `./mvnw clean install -Pall-java,licenses -DskipTests`
   - `-Pall-java` adds `examples`, `benchmarks`, `ducktests`, `numa-allocator`, `schedule`, `yardstick` to the reactor.
   - `modules/numa-allocator` needs `libnuma-dev` installed on Linux (CI does `apt-get install libnuma-dev`).
2. Style/compile gate (what CI runs, `.github/workflows/commit-check.yml`):
   `./mvnw test-compile -Pall-java,licenses,lgpl,checkstyle,examples -B -T 1C`
   - Checkstyle is skipped by default and only runs with `-Pcheckstyle` (bound to `compile`, fails the build on any
     violation, and also checks **test** sources). Config: `checkstyle/checkstyle.xml` + `checkstyle-suppressions.xml`,
     rules supplied by the `modules/checkstyle` artifact → run it from the repo root; with `-pl <module>` it needs
     `ignite-checkstyle` installed first.
3. Optional: javadoc `./mvnw initialize -Pjavadoc`; docs snippets `./mvnw compile -Pdocs -pl :code-snippets -am`.

Shortcuts:

- `./scripts/build-module.sh <name>` → builds `:ignite-<name>` (+ `-am`), skipping tests (uses bare `mvn`).
- Single test, one module: `./mvnw test -pl :ignite-core -Dtest=SomeTest#method -DfailIfNoTests=false` (add `-am` the first time).
- Full test command from DEVNOTES: `./mvnw clean test -U -Plgpl,examples,-clean-libs,-release -Dmaven.test.failure.ignore=true -DfailIfNoTests=false -Dtest=<suite-or-class>` (`-clean-libs` no longer exists; it only produces a warning).

**JVM flags for tests:** surefire is configured with `forkCount=0` (tests run inside the Maven JVM), so surefire's
`argLine` `--add-opens` list in `parent/pom.xml` does *not* apply to tests. CI exports a large `MAVEN_OPTS` block of
`--add-exports`/`--add-opens` flags before every Maven step — copy it from `.github/workflows/commit-check.yml` into
`MAVEN_OPTS` for local `./mvnw test` runs.

## Testing rules

- **Every test class must be referenced from a `*TestSuite`** under `src/test/.../testsuites/` of the same module.
  Enforced by `./mvnw test -Pcheck-test-suites` (custom surefire provider in `modules/tools` + `AssertOnOrphanedTests`
  in the root pom). For one module: `./mvnw install -Pcheck-test-suites`, add `ignite-tools` as a test dependency, then `./mvnw test` in that module.
- Fork features (Lucene/vector search, `vertx-rest`, `web-console`) have little or no test coverage — upstream suites
  cover core/indexing behavior only. `modules/ducktests/tests` is Python (run via `tox`), unrelated to surefire.
- Runtime/test data lands in `work/` at the repo root (gitignored, like `target/`, `pom-installed.xml`, `.idea/`) — never commit it.

## Conventions and gotchas

- **Rolling upgrade / protected classes:** classes under `org.apache.ignite.internal` carrying `@Order`
  (`org.apache.ignite.internal.Order`) are compatibility-protected. `.github/workflows/check-protected-classes.yml`
  flags any PR touching them with a `compatibility` label — changes there can break rolling upgrades.
- **Generated code:** annotation processors generate `*Walker` / `*Serializer` / `*Factory` classes (codegen, `internal.systemview`).
  On a compile failure, the flood of `cannot find symbol` for those generated names is a cascade — fix the first real
  error instead (CI filters these out explicitly).
- Code style: `idea/ignite_codeStyle.xml` + IDEA inspection profile `.idea/inspectionProfiles/Project_Default.xml` (see `CONTRIBUTING.md`);
  automated check is the `checkstyle` profile above.
- Only `commit-check` and `check-protected-classes` workflows run on this fork; `sonar-*`, `publish-snapshot`, website
  workflows are gated to `apache/ignite` and won't run here.
- .NET: `dotnet build modules/platforms/dotnet/Apache.Ignite.DotNetCore.sln` (CI, .NET 6). C++: `modules/platforms/cpp/DEVNOTES.txt`.
  Neither `modules/platforms/*` nor `web-console/` is part of the root Maven reactor.
- Docs are AsciiDoc in `docs/` (Jekyll preview: `docs/run.sh`, needs `bundle install`).
