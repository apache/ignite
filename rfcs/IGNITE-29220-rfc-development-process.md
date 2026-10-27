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

Use an RFC to agree on changes to the contracts defined in [Public API and compatibility contracts](IGNITE-29219-public-api-and-compatibility.md) before merging them. You need an RFC to add, change, deprecate, or remove any of these contracts. This includes additions that do not break compatibility.

Internal refactoring, optimizations, tests, and documentation fixes do not need an RFC if they keep these contracts unchanged. The same applies to bug fixes that restore previously supported behavior without changing another contract. If unsure, explain the change in the ticket and discuss it with reviewers.

## Ticket, review, and merge

1. Open a ticket with the problem and proposed change.
2. Use one PR for the ticket. Include the implementation and `rfcs/IGNITE-<ticket>-<short-title>.md`. You can open the PR with just the RFC and add the implementation later. Link the ticket and PR to each other. Use the ticket number to identify the RFC.
3. Review the document in the PR. When it is ready for approval, share that version of the RFC on the dev list. Allow at least 72 hours for feedback. Tell the list about significant changes and concerns.
4. Get explicit approval for that version from at least three different Ignite committers in the PR. Resolve any technical objections. Before merging, post the approval summary and decision on the dev list. Significant changes to the RFC need another review period and new approvals.
5. When the RFC is approved and normal code review and testing are complete, merge the RFC and implementation together as one commit for the ticket. If the RFC changes only documentation or the development process, merge the document changes. No separate code changes are needed.

One RFC can cover several implementation tickets. Merge it with the first implementation PR and link later PRs to it. Those PRs do not need three more RFC approvals unless they change the agreed contract or compatibility plan. If they do, update the RFC and get approval through the same process.

For a change that breaks compatibility, explain how existing users can migrate. If you propose skipping deprecation, say so and explain why. Describe the expected impact and the evidence behind it. Skipping deprecation needs explicit approval through the RFC process. Having no known users is not enough on its own.

## Keep the document useful

Use about the same level of detail as an IEP. Explain the problem, proposed API or format, other options, compatibility, and migration. Describe how to test the change and what needs to be done to complete the work. List the affected versions and clients. Keep it short when the change is small.

The RFC records the agreed design. Link all implementation tickets so readers can see what is done and what remains. You can link to existing IEPs instead of copying them.

## References

- [Apache Hudi RFC process](https://hudi.apache.org/contribute/rfc-process/)
- [TiDB design documents](https://github.com/pingcap/tidb/blob/master/docs/design/README.md)
- [Apache guidance on public decisions](https://www.apache.org/dev/pmc.html#mailing-list)
