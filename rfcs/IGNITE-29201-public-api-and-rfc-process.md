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

# Public API changes through tickets and PRs

Ticket: [IGNITE-29201](https://issues.apache.org/jira/browse/IGNITE-29201)

## Why

We should agree on changes to public API before merging them. Use a ticket to describe the problem and a PR to review an RFC stored alongside the code.

## What counts as public API?

Public API means supported interfaces and behavior that applications, integrations, and operators rely on:

- Language and client APIs, including plugins and SPIs.
- SQL syntax, types, functions, and behavior.
- REST endpoints, CLI commands, and supported machine-readable output.
- User-facing configuration, including defaults.
- Supported metrics, JMX interfaces, and system views.

The same review requirement covers client and internode protocols and persistent data formats, including metadata, WAL, and snapshots. Changes to these formats can affect communication between versions and the ability to read existing data after an upgrade.

An internal class does not become public API just because it has public methods. Missing documentation does not make an interface intended for external use internal.

## When do we need an RFC?

Adding, changing, deprecating, or removing an interface or format listed above requires an RFC. Backward-compatible additions count too. For example, a new public method, configuration option, or protocol field needs review.

Internal refactoring, optimizations, tests, documentation fixes, and ordinary log wording do not need an RFC if they preserve these contracts. Neither do bug fixes that restore established behavior without changing another contract. If unsure, explain the change in the ticket and discuss it with reviewers.

## Ticket → PR → merge

1. Open a ticket describing the problem and proposed change.
2. Use one PR for the ticket, containing `rfcs/IGNITE-<ticket>-<short-title>.md` and the implementation. The PR can start with the RFC while the implementation is in progress. Link the ticket and PR to each other; use the ticket number instead of maintaining a separate RFC counter.
3. Review the document in the PR. When ready for approval, share the RFC revision on the dev list and allow at least 72 hours for feedback. Keep the list informed of significant changes and concerns.
4. Collect explicit approval of that revision from at least three different Ignite committers in the PR. Resolve outstanding technical objections and post the approval summary and outcome on the dev list before merging. Material revisions need renewed approval and review time.
5. Once the RFC is approved and normal code review and testing are complete, merge the RFC and implementation together as one commit for the ticket.

One RFC can cover several implementation tickets. Merge it with the first implementation PR and link subsequent PRs to it. Those PRs do not need three RFC approvals each unless they change the agreed contract or compatibility plan. Such changes require an updated RFC approved through the same process.

## Keep the document useful

Use roughly the same level of detail as an IEP. Explain the problem, proposed API or format, alternatives, compatibility and migration, and how we will test and complete the work. Name the affected versions and clients. Keep it short when the change is small.

The RFC records the agreed design. Link all implementation tickets so readers can see what is complete and what remains. Existing IEPs can be referenced instead of copied.

## References

- [Apache Hudi RFC process](https://hudi.apache.org/contribute/rfc-process/)
- [TiDB design documents](https://github.com/pingcap/tidb/blob/master/docs/design/README.md)
- [Apache guidance on public decisions](https://www.apache.org/dev/pmc.html#mailing-list)
