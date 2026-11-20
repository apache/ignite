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

# RFC check for pull requests

The **RFC process** check flags changes in known public API, SQL, CLI, client protocol, storage and distribution paths. A refactoring in one of these paths can trigger it. Reviewers can add the `rfc-required` label when a change elsewhere affects public behavior. The [RFC process](../rfcs/IGNITE-29220-rfc-development-process.md) defines the rules.

When the check fails, read the bot's comment in the PR. It lists the affected files, the author's next step and what the reviewer should check. When the advice changes, the bot posts a new comment and removes its previous one. Once the check passes, it posts a confirmation. PRs that pass on the first run stay quiet. The same guidance is available in the job summary under **Checks**.

## Submitting an RFC

Use the [template](../rfcs/TEMPLATE.md) for a new RFC. Name the file after the PR's Jira ticket. To amend an approved RFC, open a new ticket and edit the existing file.

Start the vote on dev@ignite.apache.org. Link the PR and the RFC file at the commit being voted on. When the vote closes, link the opening vote from the result email if you start a separate thread. Link the result in the PR description. If the PR links several archive threads, identify the result on its own line.

    RFC-result: https://lists.apache.org/thread/<result-message-id>

The check reads the public Apache archive. It checks that the vote identifies this PR and RFC, compares the voted text with the PR and checks the time between the opening and result emails. It looks for three distinct Ignite committer +1 votes and a voting period of at least 72 hours. For compatible additions, it can also accept seven days without an objection when the opening announced that option. The author can vote explicitly. The check identifies committer accounts using personal Apache addresses and the current ASF roster. It can ask a reviewer to verify a sender whose address the list rewrote.

## Giving a reviewer decision

A reviewer with write access can include one of the following lines in an approving review on the current PR commit. A new commit requires a fresh review. Replace each example with the reason or RFC that applies to the whole change.

- `RFC-exempt: Only the cache implementation changed. The supported behavior is the same.` Use this for changes that need no RFC under the process, including permitted removals of previously deprecated APIs. For edits to an existing RFC, use it only for wording changes. If the PR carries the `rfc-required` label, resolve its scope with the reviewer who flagged it before removing the label. Follow the ASF private process for undisclosed issues.
- `RFC-covered: rfcs/IGNITE-12345-example.md` Use this for a later PR that implements an already approved plan. Link that RFC from the PR and check that it covers the implementation.
- `RFC-verified: <result-permalink> <what you checked>` Use this when the check requests manual verification. Read the vote and result, check voter identities, objections, timing, and the RFC text at the current commit. A change to the proposal or migration plan needs a new vote. Confirm that the change qualifies before accepting approval by silence, and check explicit approval for any request to skip deprecation.

For failed votes, early results or missing evidence, follow the RFC process and update the result link. If the archive or GitHub API is unavailable, rerun the job after access returns. Code review, implementation tests and the final squash merge are handled separately.

## Enabling the check on master

1. Merge the RFC process and its template, then merge these workflows. Create the `rfc-required` label so reviewers can flag PRs.
2. On a PR from a fork targeting `master`, check that missing evidence fails **RFC process** and produces a PR comment. Supply the evidence and check that the job passes and the bot replaces its comment with a confirmation.
3. Protect workflow changes with trusted review. Add `RFC process` to the required status checks for `master` in [`.asf.yaml`](../.asf.yaml). To stop blocking merges, remove this status from the branch rule.

The check reads the PR on a push, a description or label change, or a review. A separate workflow from the default branch checks the evidence again before it publishes a comment. Both use checker code from the base repository.

Run the checker tests with `python3 -m unittest discover -s .github/scripts`.
