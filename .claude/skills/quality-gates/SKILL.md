---
name: quality-gates
description: Run the Apache Ignite 2.x pre-merge quality gates (checkstyle, RAT license headers, test-suite membership) before considering a change done. Use when finishing a change, preparing a PR, or asked to check style/licenses/suites.
---

# Ignite quality gates

CI enforces these. Run them locally before calling a change done — a failure here blocks the PR.
Run from the repo root (or scope to a module with `-pl :ignite-<module>`).

## 1. Checkstyle

```bash
./mvnw checkstyle:check -Pcheckstyle
```

Config: `checkstyle/checkstyle.xml`; suppressions: `checkstyle/checkstyle-suppressions.xml`.
Notable enforced rules:
- No star imports.
- Import order: `STANDARD_JAVA → javax.* → THIRD_PARTY → STATIC`, alphabetical within each group.
- Tabs forbidden — spaces only.
- `org.mockito.internal.*` is banned.
- Javadoc expected on classes/methods; many fields carry a bare `/** */` to satisfy the check —
  follow the local pattern.

## 2. Apache RAT — license headers

```bash
./mvnw clean validate -Pcheck-licenses
```

Every new source file needs the ASF license header. Exclude list lives in the `check-licenses`
profile in `parent/pom.xml`.

## 3. Test-suite membership

```bash
./mvnw test -Pcheck-test-suites
```

**Every new test MUST belong to a JUnit test suite** under `src/test/.../testsuites/`
(e.g. `IgniteBasicTestSuite`). A test not in any suite fails this gate AND silently never runs in
CI. When you add a test class, add it to the appropriate suite in the same change.

## Notes

- `clean validate -Pcheck-licenses` triggers a `clean`, which wipes `target/`. Run the license
  gate first (or separately) if you want to keep compiled classes for a test run afterwards.
- These gates are independent — run whichever the change touches, but run all three before a PR.
- Coding conventions beyond checkstyle (Ignite abbreviation dictionary `cache→cctx`, `context→ctx`,
  `message→msg`, `request→req`, ...; `Grid*` internal / `Ignite*` public prefixes) are not caught
  by these gates — match surrounding code.
