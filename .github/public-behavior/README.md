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
users see only its value or the view columns. If two codes swap values under the same names, the check reports it.
A rename is matched only within the same class, so a removed operation code is not taken for a new status code with
the same value.

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
  Markdown link works. A link to another PR does not. The link must lead to the file: a link to another branch, tag or
  commit is checked through GitHub. With such a link, the check passes with a review warning.
- **RFC needed.** Without an RFC in the PR or a valid link, the check fails and lists the changes.
- **Check could not finish.** The check fails and says whether to fix the PR or to rerun the check.

The result is shown in Checks and in one PR comment. The comment tells the author what to change and the reviewer what to
check. When the result changes, the bot deletes its previous comment and posts a new one. The reviewer checks that the RFC
covers the changes, reviews compatibility and verifies any required vote on the dev list.

## What the check compares

| Public contract | The check compares | The reviewer checks |
| --- | --- | --- |
| Programming APIs, plugins and SPIs | Public and protected types and members of the packages published in javadoc, in the modules listed in `ignite-bom`: signatures, supertypes, constant values, enum order, annotation defaults, where an annotation may be used and how long it is kept, `@Deprecated` and `@IgniteExperimental`. Members inherited from internal classes count as members of the public type. Names of type parameters are not compared. | Whether internal types used in public signatures are part of the public API. Source and binary compatibility. When callbacks run. The .NET and C++ APIs. |
| SQL | Keywords of the Calcite parser and of the native DDL parser. Operators and functions of the Calcite engine with their allowed signatures. | Data types, functions of the H2 engine, and which queries succeed and what they return. |
| REST | Command names accepted in the `cmd` parameter. | Routes, parameters and the response format. |
| CLI | `control.sh` commands and subcommands, including whether a command is experimental (its own mark, which `control.sh` checks) or deprecated (its own or a parent's mark), named and positional parameters with their types, accepted enum values and whether they are required, groups of parameters, common options and exit codes. | Custom validation, whether a command asks for confirmation, and the command output. |
| Configuration | Configuration classes through the Java API, including default value constants. System properties printed by `ignite.sh -systemProps`: name, type, default value and deprecation. | Distributed properties, environment variables of the scripts, and defaults computed at runtime. |
| Monitoring | Name, type and position of each system view column. JMX interfaces and event types through the Java API. | Metric names and units, system view names, and when events fire. Ignite registers these at runtime. |
| Client protocols and external formats | Thin client operation codes, status codes, protocol versions and feature IDs. JDBC and ODBC request types and protocol versions. Binary format constants. | Message layout, version negotiation, and supported client/server combinations. |
| Stored data | WAL record type indices, page IO types and the serialization version of snapshot metadata. | Record layout, reading old data, migration and recovery. |
| Distribution and runtime | Entries of `ignite-bom`, the Java release that the build targets, the minimum Java version in the launch scripts, NuGet package IDs and their target frameworks. | Supported JDK and OS combinations and upgrade paths. |
| Errors | SQLSTATE values, query error codes and the SQLSTATE of each code, thin client status codes and `control.sh` exit codes. | The conditions that cause each error. |
| Results, effects and operations | Public modes and defaults of transactions, retries and cancellation through the Java API. | Returned values, side effects, ordering and timeouts. |

Protocol, storage and error codes come from a list of internal classes in `PublicDeclarations.java`. A new class with such
codes needs an entry in that list.

## Running locally

To describe the public declarations of a source tree, run from its root with JDK 17:

    bash .github/public-behavior/describe.sh > declarations.txt

To test the check logic, run:

    node --test .github/public-behavior/check.test.js
