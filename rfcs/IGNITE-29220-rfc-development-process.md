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

## When an RFC is required

An RFC is required to add, change, deprecate, or remove a public contract defined in [IGNITE-29219](https://issues.apache.org/jira/browse/IGNITE-29219), including compatible additions.

Refactoring, optimizations, tests, documentation fixes, and bug fixes that restore supported behavior need no RFC if they preserve all contracts. If unsure, discuss the change in the ticket.

## RFC contents

- **Proposal:** the problem, proposed change, and other options.
- **Impact:** affected versions and clients, compatibility, and migration.
- **Delivery:** tests and what must be done to complete the work.

Link all implementation tickets to track progress. Existing IEPs can be linked instead of copied.

## Review and merge

1. Create a ticket describing the problem and change. Open one PR containing the implementation and `rfcs/IGNITE-<ticket>-<short-title>.md`. Link them to each other. The ticket number identifies the RFC. The implementation may be added later.
2. Announce the RFC version on the dev list for approval. Allow at least 72 hours for feedback, starting with that message. Report changes to the proposal and concerns to the list.
3. Get explicit approval of that version from at least three different Ignite committers in the PR. The author can count if they are a committer and explicitly approve it. Resolve technical objections. Post the approval summary and decision on the dev list before merging.
4. Changes to the proposal or compatibility plan require a new announcement, a new 72-hour review period, and new approvals. Wording-only edits do not restart review.
5. After RFC approval, code review, and testing, merge the RFC and implementation as one commit for the ticket. RFCs for documentation or process changes need no code implementation.

One RFC may cover several tickets. Merge it with the first implementation PR and link later PRs to it. Repeat RFC approval only if they change the agreed contract or compatibility plan; update the RFC first.

## Breaking changes

Describe migration for existing users. Skipping deprecation requires reasons, evidence about the expected impact, and explicit approval through this process. Having no known users is not enough.

## References

- [Apache Hudi RFC process](https://hudi.apache.org/contribute/rfc-process/)
- [TiDB design documents](https://github.com/pingcap/tidb/blob/master/docs/design/README.md)
- [Apache guidance on public decisions](https://www.apache.org/dev/pmc.html#mailing-list)
