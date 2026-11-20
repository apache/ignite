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

The check compares source declarations at the PR head and its merge base. It reports changes it can identify from those declarations. The [public contract](../../rfcs/IGNITE-29219-public-api-and-compatibility.md) defines what Ignite supports. The [RFC process](../../rfcs/IGNITE-29220-rfc-development-process.md) defines when a proposal and a vote are required.

## Results

- **No action.** If the check finds no public contract change, it passes without checking RFC files. An RFC-only PR or a change to diagnostic log wording normally takes this path.
- **RFC needed.** If the check finds a contract change, add or update an RFC file in the PR. If an approved RFC already covers the change, open its file in the target branch and paste the GitHub URL into the PR description.
- **Review the change.** Once the check finds an RFC, it passes with a warning. Checks and the bot comment link to the RFC file and list the declarations or values that changed. The reviewer checks that the RFC covers the changes, reviews compatibility tests and checks any required vote on dev@.

Use `rfcs/IGNITE-<ticket>-<short-title>.md` for a new RFC. To update a plan under a new ticket, keep its original filename. If an RFC in the current PR has an invalid name, the check asks the author to fix it.

The bot replaces its comment when the result changes and confirms when earlier issues are resolved.

## What the check compares

| Area | The check reports | The reviewer checks |
| --- | --- | --- |
| Programming APIs and extensions | Java type and method declarations, constructors with parameters, enum members and literal constants in supported packages. This includes the REST extension SPI. It also compares named Windows ODBC exports. | Inherited and generic signatures that need type resolution, other C++ and .NET APIs, callback order and side effects. Check deprecated removals against the release rules. |
| SQL and errors | Keywords in Ignite's native SQL parser, literal names of selected Calcite functions, SQL error codes, SQLState values and direct error-to-SQLState mappings. | Grammar, accepted types, query results and the conditions that cause an error. |
| REST and CLI | The built-in HTTP endpoint path and command names, top-level `control.sh` commands, common option names and types, and exit code values. | HTTP parameters and JSON output, CLI subcommands and their arguments, command output and when an error is returned. |
| Configuration | Public Java setting constants and literal defaults. The check also compares selected distributed property registrations and literal defaults, and parts of the shipped .NET XML schemas. | Defaults that depend on other settings, script environment variables, and how Ignite uses the settings. |
| Monitoring | Literal names in selected metric, registry and system view registrations. Public JMX declarations and event constants use the Java API rules. | Metric meaning and units, the full view schema and the events that Ignite emits. |
| Client protocols and stored data | Client operation and feature IDs, existing binary type codes, the binary object version, existing WAL record indices, page store signatures and versions, and snapshot metadata serialization versions. | Message and record layout, version negotiation, and whether an old client or an existing snapshot still works. |
| Distribution and runtime | Maven coordinates, entries in the Ignite BOM, literal NuGet package metadata, the Maven compiler release and the minimum Java version enforced by known launchers. | Supported combinations of JDKs and operating systems, upgrade paths and runtime compatibility. |

Each rule compares the specific declarations it understands. For example, changing the numeric index of a WAL record produces a finding. A change to how recovery replays the same record needs review even when the check passes. Reviewers also check transactions, ordering, retries, cancellation and calls to user code.

## Enabling the check on master

1. Merge the public contract and process RFCs and the template, then merge the workflows.
2. On a fork PR, add a public method to a supported Java type. Verify that the check fails and names the method. Add a correctly named RFC file or put a direct link to an existing RFC file in the PR description. Verify that the check shows a review warning and that the file opens from the report.
3. Require trusted review for the workflow and checker files. Add `Public behavior changes` to the required checks for `master` in [`.asf.yaml`](../../.asf.yaml). Remove that status from the required checks to stop blocking merges.

To run the tests locally, use `python3 -m unittest discover -s tests` from `.github/public-behavior` with JDK 17 or later on PATH.
