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

# Public behavior changes

The check shows what a pull request to `master` changes for Ignite users and where, and whether the
[RFC process](../../rfcs) requires an RFC for it. The [public contract](../../rfcs/IGNITE-29219-public-api-and-compatibility.md)
defines what counts.

The check builds the target branch and the PR merged into it. For each build it lists the declarations that users can
call, pass or read, and then compares the two lists. A change is reported only when a declaration is added, removed or
has a different value. Descriptions are not compared, and moving a declaration within the sources is not a change.

## Results

- **No change.** The check passes.
- **Removed deprecated declarations only.** The check passes with a review warning. The RFC process lets authors remove
  APIs deprecated before it without an RFC. The reviewer decides whether the removal needs an RFC.
- **RFC in the PR.** Every Markdown file in `rfcs/` that the PR adds or updates, except `rfcs/TEMPLATE.md`, must be named
  `rfcs/IGNITE-<ticket>-<short-title>.md`. Any other name fails the check. With valid names, the check passes with a review
  warning that links to the RFC.
- **RFC linked from the PR description.** Without an RFC in the PR, the check looks for a direct link to an RFC file on the
  target branch, such as `https://github.com/apache/ignite/blob/master/rfcs/IGNITE-12345-cache-timeout.md`. A plain URL or a
  Markdown link works. A link to another PR does not. The branch, tag or commit in the link does not matter: the report
  links the file on the target branch. With such a link, the check passes with a review warning.
- **RFC needed.** Without an RFC in the PR or a valid link, the check fails and lists the changes.
- **Changes to the check only.** The check passes with a review warning and lists what the new version of the check
  describes differently. See [Changing the check](#changing-the-check).
- **Check could not finish.** The check fails and says whether to fix the PR, update the check or rerun it.

The job summary of the check shows the result and the changes with their locations. The reviewer checks that the RFC
covers the changes, reviews compatibility and verifies any required vote on the dev list.

## What the check compares

The check compares the system properties that `ignite.sh -systemProps` prints: their names, types, default values and
deprecation. Their descriptions are not compared. The check builds all modules of the tree, so it lists the properties
that `ignite.sh -systemProps` prints when every module is on the class path.

The other parts of the public contract are not compared yet. [IGNITE-29326](https://issues.apache.org/jira/browse/IGNITE-29326)
plans them. Until then, the reviewer checks them.

## How the check works

The check has two parts, connected by the description format below.

- `describe/` finds the public declarations of a built source tree. `describe.sh` builds the tree, compiles the sources in
  `src/` and runs `PublicDeclarations.java`. It runs one collector for each kind of declarations from the package
  `collectors`, such as `SystemProperties`. The collectors use the package `framework` only through its public API.
  There, `BuiltTree` gives them the source location of a declaration, `Declarations` merges declarations with the same key
  and prints them, and `Collector` is the contract of a collector.
- `check/` decides what the descriptions mean for the PR. `compare.js` finds what the PR adds, removes or changes.
  `rfc.js` applies the RFC rules to the files of the PR and to the links in its description. `outcome.js` picks the result.
  `render.js` writes the report, and `report.js` runs them in the PR job.

Each build is described by its own version of the check: the PR merge by the version of the PR, the target branch by its
own version. A version of the check can describe only the code it is written for. If the target branch has no check or its
check fails, the version of the PR describes it, and Checks show a warning.

### Description format

Each line is one declaration, with five columns separated by tabs:

1. **Area**: the heading in the report, such as `Configuration`.
2. **Key**: identifies the declaration within its area. The same key in both descriptions is the same declaration.
3. **Value**: what users rely on. The check reports a declaration whose value changed. Values of declarations with the same
   key are joined with ` | `, such as two constants that name the same system property.
4. **Flags**: `deprecated`, or empty. A removed declaration with this flag on the target branch is left to the reviewer. A
   collector sets the flag only for a mark that users see, and the value shows that mark too: marking a declaration
   deprecated is a change.
5. **Location**: `path:line` of the declaration, or the path alone. The report shows it. A removed declaration shows its
   location on the target branch.

Only the key and the value are compared. They keep every character, so two declarations that differ in a space stay
different. A backslash, a tab, a line feed, a carriage return, another control character and a space at either end are
written as `\\`, `\t`, `\n`, `\r`, `\uXXXX` and `\s`, and `|` in a value as `\|`. The report shows them this way. A
collector that knows that some whitespace does not matter normalizes it itself.

New columns go at the end: the check reads a description with fewer columns as if the missing ones were empty.

## Changing the check

The PR job runs the check from the PR, so a PR could change the verdict about itself. The reviewer of any PR that changes
the check reviews that change.

- **A PR changes Ignite code that the check reads.** The description of the PR merge fails, and the check log names the file
  in `describe/src/`. Each collector names the Ignite classes and files it reads at its start. Update the collector in the same
  PR. The target branch is still described by its own version, so a PR that changes nothing public shows no changes.
- **An RFC changes what the check compares.** Add or change a collector and the section
  [What the check compares](#what-the-check-compares). A collector that reads files outside `modules`, `bin`, `parent` and the
  root `pom.xml` adds them to the path filter of the check workflow: otherwise a PR that changes only those files skips the
  builds and passes. Declarations that the check starts to compare are reported as added, and the ones it stops comparing as
  removed. The reviewer checks them against the RFC.
- **A PR changes only the check.** If the PR changes nothing outside `.github/public-behavior/` and the workflow of the
  check, the check passes with a review warning and lists what the new version describes differently. If the new version
  describes the code as the old one did, the check passes. Keep such changes apart from changes of Ignite code, so that the
  list shows only the effect of the check.
- **The check fails on the target branch.** A PR that does not change the check gets a check that could not finish. A PR
  that fixes the check passes, because its version describes the target branch.
- **A PR changes `check/` or the workflow.** The PR job runs `check/` and its tests from the PR.

## Running locally

To describe the public declarations of a source tree, run from its root with JDK 17:

    bash .github/public-behavior/describe/describe.sh > declarations.txt

To check a change of `describe/`, describe the same tree with the old and the new version and compare the two files.

To test `check/`, run:

    node --test .github/public-behavior/check/*.test.js
