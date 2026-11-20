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

[Public behavior and compatibility](../rfcs/IGNITE-29219-public-api-and-compatibility.md) defines what users can rely on, including API behavior, client protocols, stored data and supported runtime setups. The [RFC process](../rfcs/IGNITE-29220-rfc-development-process.md) defines which changes need a vote and how approval works.

The check flags known contract paths and checks proposals in `rfcs/`. A matching path tells the reviewer where to look. A change elsewhere can also affect users. Reviewers can flag it with the `rfc-required` label.

If the check fails, the bot comments in the PR with the files to review, the author's next step and what the reviewer should check. It replaces its comment when the advice changes or the problem is resolved. A PR that passes on its first run stays quiet. The report is also available under **Checks**.

## When a change needs a vote

Ask what users can rely on before and after the change. A new field in a supported client protocol needs an RFC even if old clients keep working. A SQL fix that restores the documented result can go through normal review if it preserves the other contracts. The process also describes exceptions for previously deprecated APIs and undisclosed security fixes.

Use the [template](../rfcs/TEMPLATE.md) for a new proposal. Name it after the PR's Jira ticket. To amend an approved RFC, open a new ticket and edit the existing file. Wording-only edits can go through review without another vote.

Discuss the proposal in the PR. When it is ready, start a `[VOTE]` on dev@ignite.apache.org with links to the PR and the RFC revision. Keep it open for at least 72 hours. Approval takes three explicit `+1` votes from different Ignite committers. A committer who wrote the RFC can vote. If the RFC only adds compatible behavior, announce that seven days without objections also counts as approval. Skipping deprecation needs explicit approval.

Publish `[RESULT]` on dev@. Put the result link in the PR description so the check runs again. If the description has several archive links, mark the result on its own line.

    RFC-result: https://lists.apache.org/thread/<result-message-id>

## What the reviewer confirms

A reviewer with write access records one of these decisions in an approving review on the current commit. A new commit needs a fresh decision.

- `RFC-exempt: <reason>` — explain why the process permits this change without a vote. For example, a refactoring preserves the contract or an RFC edit only clarifies wording. If `rfc-required` was applied in error, resolve it with the reviewer who added it.
- `RFC-covered: rfcs/IGNITE-12345-example.md` — identify the approved RFC that already covers this implementation.
- `RFC-verified: <result URL> <what you checked>` — confirm the dev-list decision when the check asks for evidence. Compare the voted text with the PR. If a separate result does not identify the opening vote, put the opening URL immediately after the result URL. For approval by silence, check that the opening offered it, the RFC only adds compatible behavior and seven days passed without objections.

Merge the RFC with its first implementation in one commit after code review and tests. Later PRs that follow the approved plan can link to it. This also covers removal at the release agreed in a deprecation RFC. A documentation or process RFC can stand alone. A change to the agreed proposal or migration plan needs a new vote.

If the archive is unavailable, retry after access returns. If the PR or base branch moved, trigger a new check by updating the PR.

## Enabling the check on master

1. Merge the two RFCs and the template, then merge the workflows. Create the `rfc-required` label.
2. On a fork PR targeting `master`, verify that missing evidence fails **Public behavior changes** and produces a PR comment. Supply the evidence and check that the bot replaces the warning with a confirmation.
3. Require trusted review for the workflow and checker files. Add `Public behavior changes` to the required checks for `master` in [`.asf.yaml`](../.asf.yaml). Remove that status from the branch rule to stop blocking merges.

The check reruns on a push, a PR description or label change, or a review. A workflow from the default branch verifies the current evidence before posting a comment. Both use code from the base repository.

Run the tests with `python3 -m unittest discover -s .github/scripts`.
