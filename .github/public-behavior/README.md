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

This check helps reviewers spot changes to [Ignite's public contracts](../../rfcs/IGNITE-29219-public-api-and-compatibility.md). The [RFC process](../../rfcs/IGNITE-29220-rfc-development-process.md) explains when a proposal and a vote are required.

## What appears in a PR

- **A file may affect public behavior.** The check passes and the bot lists the files and what to review. For example, a client protocol change calls for checking old clients against the new server. If a change outside the known paths needs the same review, add the `public-behavior-change` label.
- **The PR adds or updates an RFC.** The bot lists the file. Its name must follow `rfcs/IGNITE-<ticket>-<short-title>.md`, for example `rfcs/IGNITE-12345-cache-timeout.md`. An invalid name fails the check. The comment shows the file and how to rename it. To update an existing RFC under a new ticket, edit the original file and keep its name.
- **The PR changes both code and an RFC.** Review the implementation and its compatibility tests against the plan. A wording fix in the RFC can accompany code that changes public behavior.

The reviewer decides whether an RFC or an exception applies, checks any required vote on dev@, and approves the PR when satisfied.

The check runs when a PR to `master` opens, reopens, becomes ready for review, or changes its code, details or labels. The result is available under **Checks**. When there is something to fix or review, the bot also comments in the PR. It replaces its comment when the advice changes and confirms when earlier issues are resolved. A clean PR with nothing to review gets no comment.

## Enabling the check on master

1. Merge the public contract and process RFCs and the template, then merge the workflows. Create the `public-behavior-change` label.
2. On a fork PR, change a known public API file. Check that the job passes and the bot posts review guidance. Add an RFC named `rfcs/cache-timeout.md`, then rename it to `rfcs/IGNITE-12345-cache-timeout.md`. Check that the job fails and then passes, and that the bot replaces its warning.
3. Require trusted review for the workflow and checker files. Add `Public behavior changes` to the required checks for `master` in [`.asf.yaml`](../../.asf.yaml). To stop blocking merges on this check, remove it from the required checks.

To run the tests locally, use `python3 -m unittest discover -s tests` from `.github/public-behavior`.
