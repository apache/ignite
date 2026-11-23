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
has a different shape. Help texts, descriptions and examples are not compared, and moving a declaration within the
sources is not a change. Renaming an internal constant or the class of a system view row is not a change either, because
users see only its value or the view columns. A code is identified by its group, such as thin client operations, and its
name, not by the class that declares it, so moving codes to another class is not a change. If two codes swap values under
the same names, the check reports it. A rename is matched only within the same group, so a removed operation code is not
taken for a new status code with the same value.

## Results

- **No change.** The check passes. A clean PR gets no comment, including a PR that changes only an RFC.
- **Removed deprecated or changed experimental API only.** The check passes with a review warning. The RFC process lets
  authors remove APIs deprecated before it without an RFC, and experimental APIs may change in any release. The reviewer
  decides whether such a change needs an RFC. For a removed deprecated declaration the report shows the commit and the first
  release where its deprecation was last changed, if the check finds the annotation that deprecates it in the sources: its
  own, or that of its type or parent command.
- **RFC in the PR.** Every Markdown file in `rfcs/` that the PR adds or updates, except `rfcs/TEMPLATE.md`, must be named
  `rfcs/IGNITE-<ticket>-<short-title>.md`. Any other name fails the check. With valid names, the check passes with a review
  warning that links to the RFC.
- **RFC linked from the PR description.** Without an RFC in the PR, the check looks for a direct link to an RFC file on the
  target branch, such as `https://github.com/apache/ignite/blob/master/rfcs/IGNITE-12345-cache-timeout.md`. A plain URL or a
  Markdown link works. A link to another PR does not. The branch, tag or commit in the link does not matter: the comment
  links the file on the target branch. With such a link, the check passes with a review warning.
- **RFC needed.** Without an RFC in the PR or a valid link, the check fails and lists the changes.
- **Changes to the check only.** The check passes with a review warning and lists what the new version of the check
  describes differently. See [Changing the check](#changing-the-check).
- **Check could not finish.** The check fails and says whether to fix the PR or to rerun the check.

The result is shown in Checks and in one PR comment. The comment tells the author what to change and the reviewer what to
check. When the result changes, the bot deletes its previous comment and posts a new one. The reviewer checks that the RFC
covers the changes, reviews compatibility and verifies any required vote on the dev list.

## What the check compares

| Public contract | The check compares | Not compared, and why |
| --- | --- | --- |
| Programming APIs, plugins and SPIs | Public and protected types and members of the packages published in javadoc, in the modules listed in `ignite-bom`: signatures, supertypes, constant values, enum order, annotation defaults, where an annotation may be used and how long it is kept, `@Deprecated` and `@IgniteExperimental`. Members inherited from internal classes and interfaces count as members of the public type, except static methods of interfaces, which Java does not inherit. Names of type parameters are not compared. | The .NET and C++ APIs: the check builds only Java. |
| SQL | Keywords of the Calcite parser and of the native DDL parser. Operators and functions of the Calcite engine with their allowed signatures. | Functions of the H2 engine: they come from the H2 library, and Ignite does not list them. |
| REST | Command names accepted in the `cmd` parameter. | Parameters and response fields of each command: the command handlers read and write them by name in code, and no declaration lists them. |
| CLI | `control.sh` commands and subcommands, including whether a command is experimental (its own mark, which `control.sh` checks) or deprecated (its own or a parent's mark) and whether it asks for confirmation, named and positional parameters with their types, accepted enum values and whether they are required, groups of parameters, common options and exit codes. Options of `ignite.sh` that `bin/include/parseargs.sh` handles. | Options of `ignite.bat` and the help option of the launcher: `CommandLineTransformer` and `CommandLineStartup` parse them in code, with no declaration. |
| Configuration | Configuration classes through the Java API, including default value constants. System properties printed by `ignite.sh -systemProps`: name, type, default value and deprecation. Connection properties of the JDBC thin driver: name, whether required, default and accepted values. | Distributed properties: Ignite creates them when a node starts. Environment variables of the scripts: the scripts read them among their internal variables, with no list of their own. |
| Monitoring | Name, type and position of each system view column. JMX interfaces and event types through the Java API. | Metric names and types and system view names: Ignite registers them when a node starts, often under computed names. |
| Client protocols and external formats | Thin client operation codes, status codes, protocol versions and feature IDs. JDBC and ODBC request types and protocol versions. Binary format constants. | Layout of thin client, JDBC and ODBC messages: the code that reads and writes the fields sets it, and no declaration describes it. Attributes of the ODBC connection string: the ODBC driver is written in C++. |
| Stored data | WAL record type indices, page IO types, the latest version of each page type and the serialization version of snapshot metadata. | Layout of WAL records and pages: the code that reads and writes them sets it. The serialized form of stored cache configurations: Ignite writes them with Java serialization, and the classes it stores are yet to be identified. Versions of pages that Ignite registers when a node starts, such as H2 index pages. |
| Distribution and runtime | Entries of `ignite-bom` as Maven publishes it, including the ones it takes from the parent POM, with their type and classifier, the Java release that the build targets, the minimum Java version in the launch scripts, the launch scripts in `bin`, NuGet package IDs and their target frameworks. | Docker images and DEB and RPM packages: separate scripts in `deliveries` and `packaging` build them, and the check does not read them yet. |
| Errors | SQLSTATE values, query error codes and the SQLSTATE of each code, thin client status codes and `control.sh` exit codes. | Nothing. |
| Results, effects and operations | Public modes and defaults of transactions, retries and cancellation through the Java API. | Nothing beyond behavior, see below. |

Behavior is outside what the check compares, and the reviewer checks it: returned values and side effects, ordering,
timeouts and when callbacks and events run; custom validation of `control.sh` arguments and the command output; which
queries succeed and what they return; defaults computed at runtime; source and binary compatibility beyond signatures, and
whether internal types used in public signatures are part of the public API; version negotiation and supported
client/server combinations; reading old data, migration and recovery; supported JDK and OS combinations and upgrade paths;
the conditions that cause each error.

Protocol, storage and error codes come from a list of internal classes and the groups of their codes in
`describe/src/collectors/Codes.java`. A new class with such codes needs an entry in that list.

## How the check works

The check has two parts, connected by the description format below.

- `describe/` finds the public declarations of a built source tree. `describe.sh` builds the tree, compiles the sources in
  `src/` and runs `PublicDeclarations.java`. It runs one collector for each kind of declarations from the package
  `collectors`: `JavaApi`, `SystemProperties`, `SystemViews`, `Cli`, `Codes`, `Sql` and `Distribution`. The collectors use
  the package `framework` only through its public API. There, `BuiltTree` gives them the published modules, their classes,
  the packages that javadoc publishes and the source location of a declaration. `Declarations` merges declarations with
  the same key and prints them. `Collector` is the contract of a collector.
- `check/` decides what the descriptions mean for the PR. `compare.js` finds what the PR adds, removes or changes.
  `rfc.js` applies the RFC rules to the files of the PR and to the links in its description. `outcome.js` picks the result
  and checks `result.json`, which the PR job passes to the comment job. `render.js` writes the report. `report.js` runs in
  the PR job. `publish.js` runs in the comment job, with the code of the target branch.

Each build is described by its own version of the check: the PR merge by the version of the PR, the target branch by its
own version. A version of the check can describe only the code it is written for. If the target branch has no check or its
check fails, the version of the PR describes it, and Checks show a warning.

### Description format

Each line is one declaration, with seven columns separated by tabs:

1. **Area**: the heading in the report, such as `Java API`.
2. **Key**: identifies the declaration within its area. The same key in both descriptions is the same declaration.
3. **Value**: what users rely on. The check reports a declaration whose value changed. Values of declarations with the same
   key are joined with ` | `.
4. **Flags**: `deprecated`, `experimental` or both, separated by a comma. They decide whether a change needs an RFC. A
   collector sets a flag only for a mark that users see, and the value of the declaration, or of the type or command
   that contains it, shows that mark too: marking a declaration deprecated is a change, and the flag lets the reviewer
   decide on its later removal. `@Deprecated` on an internal constant is not such a mark.
5. **Location**: `path:line` of the declaration, or the path alone. The report shows it.
6. **Rename group**: an internal name in the key that users do not see, such as the name of a protocol constant, or empty.
   Declarations whose group only changed its name are not a change. The group must be a part of the key, and its first
   occurrence counts.
7. **Deprecation**: `path:line` of the annotation that deprecates the declaration, or empty. The report uses it to show when
   a removed declaration was deprecated.

Only the key and the value are compared. They keep every character, so two declarations that differ in a space stay
different. A backslash, a tab, a line feed, a carriage return, another control character and a space at either end are
written as `\\`, `\t`, `\n`, `\r`, `\uXXXX` and `\s`, and `|` in a value as `\|`. The report shows them this way. A
collector that knows that some whitespace does not matter, such as in an SQL signature, normalizes it itself.

New columns go at the end: the check reads a description with fewer columns as if the missing ones were empty.

## Changing the check

The PR job runs the check from the PR, so a PR could change the verdict about itself. The reviewer of any PR that changes
the check reviews that change.

- **A PR changes Ignite code that the check reads.** The description of the PR merge fails, and the check log names the file
  in `describe/src/`. Each collector names the Ignite classes and files it reads at its start. Update the collector in the same
  PR. The target branch is still described by its own version, so a PR that changes nothing public shows no changes.
- **An RFC changes what the check compares.** Add or change a collector and its row in the table above. A collector that
  reads files outside `modules`, `bin`, `parent` and the root `pom.xml` adds them to the path filter of the check workflow:
  otherwise a PR that changes only those files skips the builds and passes. Declarations that
  the check starts to compare are reported as added, and the ones it stops comparing as removed. The reviewer checks them
  against the RFC.
- **A PR changes only the check.** If the PR changes nothing outside `.github/public-behavior/` and the two workflows of the
  check, the check passes with a review warning and lists what the new version describes differently. If the new version
  describes the code as the old one did, the check passes without a comment. Keep such changes apart from changes of Ignite
  code, so that the list shows only the effect of the check.
- **The check fails on the target branch.** A PR that does not change the check gets a check that could not finish. A PR
  that fixes the check passes, because its version describes the target branch.
- **A PR changes `check/` or the workflows.** The PR job runs `check/` and its tests from the PR. The comment job runs from
  the target branch, so test changes to `publish.js` and to the comment workflow in a fork whose default branch has them.
  Add new fields to `result.json` without changing the old ones: the comment job of the target branch reads the result of
  the PR.

## Running locally

To describe the public declarations of a source tree, run from its root with JDK 17:

    bash .github/public-behavior/describe/describe.sh > declarations.txt

To check a change of `describe/`, describe the same tree with the old and the new version and compare the two files.

To test `check/`, run:

    node --test .github/public-behavior/check/*.test.js
