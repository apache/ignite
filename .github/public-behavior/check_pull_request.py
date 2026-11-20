# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Check public behavior changes in a pull request from the trusted base checkout."""
import base64
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

from check_summary import report
from contract_paths import contract_review

RFC_PATH = r"rfcs/IGNITE-([0-9]+)-[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*\.md"
TEMPLATE = "rfcs/TEMPLATE.md"


@dataclass
class CheckResult:
    reason: str
    author: str
    reviewer: str
    paths: tuple = ()
    advisory: bool = False


class CheckError(ValueError):
    def __init__(self, reason, author, reviewer):
        super().__init__(reason)
        self.result = CheckResult(reason, author, reviewer)


def api(route, *, pages=False):
    args = ["gh", "api"]
    if pages:
        args.extend(["--paginate", "--slurp"])
    try:
        data = json.loads(subprocess.check_output([*args, route], stderr=subprocess.PIPE))
        return [item for page in data for item in page] if pages else data
    except (OSError, subprocess.CalledProcessError, ValueError, TypeError) as exc:
        detail = str(exc)
        if isinstance(exc, subprocess.CalledProcessError) and exc.stderr:
            detail = exc.stderr.decode(errors="replace").strip()
        raise CheckError(
            f"Could not read the GitHub API response for {route}. {detail}",
            "Check that the repository and revision are available, then rerun the job.",
            "Check the failed request in the job log and confirm the workflow has read access.",
        ) from exc


def content(repo, path, sha):
    data = api(f"repos/{repo}/contents/{quote(path)}?ref={sha}")
    try:
        if data["type"] == "file" and data["encoding"] == "base64":
            return base64.b64decode(data["content"]).decode()
    except (KeyError, ValueError, TypeError):
        pass
    raise CheckError(
        f"Cannot read the RFC as a UTF-8 file at {repo}@{sha}.",
        f"Check that {path} is a regular Markdown file at the referenced commit.",
        "Open that revision and verify that the RFC text is readable.",
    )


def validate_new(path, text, title, template):
    ticket = "IGNITE-" + re.fullmatch(RFC_PATH, path)[1]
    if not re.match(rf"{ticket}\b", title) or f"(https://issues.apache.org/jira/browse/{ticket})" not in text:
        raise CheckError(
            f"The title or the Jira link in {path} does not match the RFC ticket.",
            f"Start the PR title with {ticket} and link https://issues.apache.org/jira/browse/{ticket} from the RFC.",
            "Check that the PR, RFC filename and Jira link identify the same proposal.",
        )
    missing = []
    for heading in re.findall(r"^## .+$", template, re.M):
        section = re.search(rf"^{re.escape(heading)}\s*\n(.*?)(?=^## |\Z)", text, re.M | re.S)
        if not section or not section[1].strip():
            missing.append(heading[3:])
    if missing:
        raise CheckError(
            f"{path} has missing or empty template sections.",
            "Fill in " + ", ".join(missing) + " under the original template headings.",
            "Check that each section answers the template question for the proposed change.",
        )
    prompts = template.split("## Problem", 1)[-1].splitlines()
    if any(line in text for line in prompts if line and not line.startswith("#")):
        raise CheckError(
            f"{path} still contains instructions from the template.",
            "Replace the template prompts with the proposal's answers.",
            "Check that the proposal explains compatibility, migration and how the implementation will be tested.",
        )


def check(pr, files, template=None):
    changed = {path for f in files for path in (f["filename"], f.get("previous_filename")) if path}
    signals = {path: hint for path in sorted(changed) if (hint := contract_review(path))}
    proposals = {path for path in changed if path.startswith("rfcs/") and path.endswith(".md")}
    marked = any(label["name"] == "public-behavior-change" for label in pr["labels"])
    paths = tuple(sorted(changed if marked else signals.keys() | proposals))
    try:
        docs = validate_proposals(pr, files, template)
    except CheckError as exc:
        exc.result.paths = paths
        raise

    if not (docs or signals or marked):
        return CheckResult(
            "The changed files include no RFC or known public-contract path.",
            "Continue with the usual PR review.",
            "If another file changes public behavior, add the public-behavior-change label.",
        )

    hints = []
    if docs:
        reason = "This PR adds or edits an RFC."
        author = "Discuss the proposal in this PR. Follow the RFC process for a new proposal or a change to the agreed plan."
        hints.append("For a new proposal or a changed plan, check the vote on dev@ and any later objections. "
                     "Wording-only edits need no new vote.")
        if any(f["filename"] not in docs for f in files):
            hints.append("Review the code separately. Compare the implementation and its compatibility tests with the agreed plan.")
        else:
            hints.append("A product RFC goes with its first implementation. A documentation or process RFC can stand alone.")
    else:
        reason = ("The public-behavior-change label flags this PR for review."
                  if marked else "This PR changes a file that may affect public behavior.")
        author = ("If public behavior changes, start from rfcs/TEMPLATE.md unless an approved RFC "
                  "or a process exception already covers the work.")
        hints.append("Check whether the code preserves the contract, follows an approved plan or needs an RFC.")

    hints.extend(dict.fromkeys(signals.values()))
    return CheckResult(reason, author, " ".join(hints), paths, advisory=True)


def validate_proposals(pr, files, template):
    docs = {}
    for f in files:
        path = f["filename"]
        old = f.get("previous_filename")
        if old == TEMPLATE or (path == TEMPLATE and f["status"] == "removed"):
            raise CheckError(
                "The RFC template was moved or removed.",
                f"Restore {TEMPLATE}.", "Check that contributors can still start an RFC from the template.",
            )
        original = old if old else (path if f["status"] == "removed" else "")
        if re.fullmatch(RFC_PATH, original):
            raise CheckError(
                f"Existing links to {original} would break.",
                "Restore the RFC at its original path and edit it in place.",
                "Check that existing votes and implementation PRs still link to the proposal.",
            )
        if path.startswith("rfcs/") and path.endswith(".md") and path != TEMPLATE:
            if not re.fullmatch(RFC_PATH, path):
                raise CheckError(
                    f"The RFC filename {path} does not identify a Jira ticket and a short title.",
                    "Name the RFC rfcs/IGNITE-<ticket>-<short-title>.md.",
                    "Check that the filename identifies the proposal's Jira ticket.",
                )
            docs[path] = f["status"]
    if not docs:
        return docs

    pr_ticket = re.match(r"(IGNITE-[0-9]+)\b", pr["title"])
    if not pr_ticket:
        raise CheckError(
            "The PR title is missing its Jira ticket.",
            "Start the PR title with its Jira ticket, for example IGNITE-12345.",
            "Check that this ticket describes the proposal or amendment under review.",
        )
    if any(status == "modified" and path.startswith(f"rfcs/{pr_ticket[1]}-") for path, status in docs.items()):
        raise CheckError(
            "The PR reuses the Jira ticket of the RFC it amends.",
            "Use a new Jira ticket for the amendment and edit the existing RFC at its original path.",
            "Check the revised behavior and migration plan against the original approval.",
        )
    head_repo = pr["head"].get("repo")
    if not head_repo:
        raise CheckError(
            "GitHub cannot identify the source repository for this RFC revision.",
            "Restore access to the source repository or open a PR from an accessible branch.",
            "Check that the proposed RFC can be read at the PR head commit.",
        )
    for path, status in docs.items():
        text = content(head_repo["full_name"], path, pr["head"]["sha"])
        if not text.strip():
            raise CheckError(
                f"{path} is empty.", "Restore the proposal text in this file.",
                "Check that the RFC still describes the agreed behavior.",
            )
        if status in {"added", "renamed", "copied"}:
            if template is None:
                try:
                    template = Path(TEMPLATE).read_text()
                except (OSError, UnicodeError) as exc:
                    raise CheckError(
                        f"The base checkout could not read {TEMPLATE}.",
                        "Rerun on a base revision containing the RFC template.",
                        "Check that the RFC process and template have landed on the base branch.",
                    ) from exc
            validate_new(path, text, pr["title"], template)
    return docs


def main():
    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())["pull_request"]
    route = f"repos/{os.environ['GITHUB_REPOSITORY']}/pulls/{event['number']}"
    pr = api(route)
    if (pr["head"]["sha"], pr["base"]["sha"]) != (event["head"]["sha"], event["base"]["sha"]):
        raise CheckError(
            "This run is stale. The PR head or base revision has changed.",
            "Open the check for the current PR revision. Update the PR if it needs a new run.",
            "Use the result attached to the current revision before merging.",
        )
    files = api(route + "/files?per_page=100", pages=True)
    if len(files) != pr["changed_files"]:
        raise CheckError(
            f"GitHub returned an incomplete file list ({len(files)} of {pr['changed_files']}).",
            "Split PRs that exceed GitHub's limit of 3,000 files. For a smaller PR, rerun the job.",
            "Wait for a complete file list before assessing public-contract impact.",
        )
    return check(pr, files)


if __name__ == "__main__":
    try:
        outcome = main()
        passed = True
    except CheckError as error:
        outcome = error.result
        passed = False
    except (OSError, KeyError, TypeError, ValueError) as error:
        outcome = CheckResult(
            f"Could not read the workflow event or pull request data. {error}",
            "Rerun the job for the current PR revision.",
            "If the error recurs, check the workflow event and GitHub API response in the job log.",
        )
        passed = False
    report(passed, outcome.reason, outcome.author, outcome.reviewer, outcome.paths, advisory=outcome.advisory)
    sys.exit(0 if passed else 1)
