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

Fixes for undisclosed vulnerabilities follow the [ASF security process](https://www.apache.org/security/committers.html#work-in-private) instead of this public RFC process. After disclosure, document any public contract changes and migration steps in the user documentation. The fix does not need a separate RFC after disclosure.

## RFC contents

Copy [TEMPLATE.md](TEMPLATE.md) and replace its prompts with answers. Link to relevant existing IEPs instead of copying them.

## Review and merge

1. Create a ticket describing the problem and change. Open one PR with the implementation and either a new RFC or an update to an existing RFC. Name new RFCs `rfcs/IGNITE-<ticket>-<short-title>.md`. Link the ticket and PR. The implementation may be added later.
2. Review the RFC in the PR. When it is ready, start a `[VOTE]` on `dev@ignite.apache.org` with links to the PR and the RFC version being voted on. Include any unresolved concerns in the vote thread. Keep the vote open for at least 72 hours.
3. Get explicit `+1` votes from at least three different Ignite committers on the dev list. The author can count if they are a committer and vote explicitly. Any request to skip deprecation must be explicitly approved. Resolve technical objections. A valid technical veto on a code change blocks merging until withdrawn.
4. Publish `[RESULT]` on the dev list and link it from the PR.
5. The implementation must pass code review and tests. Merge it with the approved RFC as one commit for the ticket. RFCs for documentation or process changes need no code implementation.

One RFC may cover several tickets. Merge it with the first implementation PR and link later PRs to it.

## Changes to an RFC

An approved RFC can be updated through a new ticket and PR. Edit the existing RFC file and merge the update with any related code changes.

After voting starts, changes to the proposal or compatibility plan require a new vote under the same rules.

## References

- [Apache Hudi RFC process](https://hudi.apache.org/contribute/rfc-process/)
- [TiDB design documents](https://github.com/pingcap/tidb/blob/master/docs/design/README.md)
- [Apache guidance on public decisions](https://www.apache.org/dev/pmc.html#mailing-list)
- [Apache voting and veto rules](https://www.apache.org/foundation/voting.html)
