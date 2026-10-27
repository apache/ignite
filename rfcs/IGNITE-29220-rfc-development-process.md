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

# RFC development process

Ticket: [IGNITE-29220](https://issues.apache.org/jira/browse/IGNITE-29220)

## When to use an RFC

Use an RFC to agree on changes covered by [Public API and compatibility contracts](IGNITE-29219-public-api-and-compatibility.md) before merging them. Adding, changing, deprecating, or removing a covered contract requires an RFC; backward-compatible additions count too.

Internal refactoring, optimizations, tests, and documentation fixes do not need an RFC when they preserve these contracts. Neither do bug fixes that restore established behavior without changing another contract. If unsure, explain the change in the ticket and discuss it with reviewers.

## Ticket, review, and merge

1. Open a ticket describing the problem and proposed change.
2. Use one PR for the ticket, containing `rfcs/IGNITE-<ticket>-<short-title>.md` and the implementation. The PR may start with the RFC while implementation is in progress. Link the ticket and PR to each other; the ticket number identifies the RFC.
3. Review the document in the PR. When ready for approval, share the RFC revision on the dev list and allow at least 72 hours for feedback. Keep the list informed of significant changes and concerns.
4. Collect explicit approval of that revision from at least three distinct Ignite committers in the PR. Resolve outstanding technical objections and post the approval summary and outcome on the dev list before merging. Material revisions need renewed approval and review time.
5. Once the RFC is approved and normal code review and testing are complete, merge the RFC and implementation together as one commit for the ticket. For RFCs that change only documentation or the development process, merge the document change itself; no separate code implementation is required.

One RFC may cover several implementation tickets. Merge it with the first implementation PR and link subsequent PRs to it. Those PRs do not need three additional RFC approvals unless they change the agreed contract or compatibility plan. Such changes require an updated RFC approved through the same process.

For an incompatible change, explain how existing users can migrate. If you propose skipping deprecation, say so and explain why. Describe the expected impact and the evidence behind it. Skipping deprecation needs explicit approval through the RFC process. Having no known users is not enough on its own.

## Keep the document useful

Use roughly the same level of detail as an IEP. Explain the problem, proposed API or format, alternatives, compatibility and migration, and how we will test and complete the work. Name the affected versions and clients. Keep it short when the change is small.

The RFC records the agreed design. Link all implementation tickets so readers can see what is complete and what remains. Existing IEPs can be referenced instead of copied.

## References

- [Apache Hudi RFC process](https://hudi.apache.org/contribute/rfc-process/)
- [TiDB design documents](https://github.com/pingcap/tidb/blob/master/docs/design/README.md)
- [Apache guidance on public decisions](https://www.apache.org/dev/pmc.html#mailing-list)
