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

- **No action.** If the check finds no public contract change, it passes without checking RFC files. This includes PRs that only edit an RFC or a method body.
- **RFC needed.** If the check finds a contract change, add or update an RFC file in the PR. If an approved RFC already covers the change, open its file in the target branch and paste the GitHub URL into the PR description.
- **Review the change.** Once the check finds an RFC, it passes with a warning. Checks and the bot comment link to the RFC file and list the declarations or values that changed. The reviewer checks that the RFC covers the changes, reviews compatibility tests and checks any required vote on dev@.

Use `rfcs/IGNITE-<ticket>-<short-title>.md` for a new RFC. To update a plan under a new ticket, keep its original filename. If an RFC in the current PR has an invalid name, the check asks the author to fix it.

The bot replaces its comment when the result changes and confirms when earlier issues are resolved.

## What the check compares

- Declarations in supported public Java packages, including API types, methods and literal constants. The analyzer uses the JDK parser. It skips method bodies and comparisons that need unresolved type or inheritance information. Reviewers check removals of deprecated API against the release rules.
- Literal SQLState values, SQL error codes, client operation IDs and client feature IDs in their defining Java types.
- Literal Maven `groupId` and `artifactId` changes in supported published modules.

The report identifies the file and the specific change. For example, adding a setter can produce a finding. Editing only its implementation produces none. Changes to SQL execution, on-disk formats or other runtime behavior may still need an RFC under the public contract.

## Enabling the check on master

1. Merge the public contract and process RFCs and the template, then merge the workflows.
2. On a fork PR, add a public method to a supported Java type. Verify that the check fails and names the method. Add a correctly named RFC file or put a direct link to an existing RFC file in the PR description. Verify that the check shows a review warning and that the file opens from the report.
3. Require trusted review for the workflow and checker files. Add `Public behavior changes` to the required checks for `master` in [`.asf.yaml`](../../.asf.yaml). Remove that status from the required checks to stop blocking merges.

To run the tests locally, use `python3 -m unittest discover -s tests` from `.github/public-behavior` with JDK 17 or later on PATH.
