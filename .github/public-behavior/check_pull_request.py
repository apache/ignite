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
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from check_summary import report
from contract_paths import contract_review

RFC_PATH = re.compile(r"rfcs/IGNITE-[0-9]+-[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*\.md")


@dataclass
class CheckResult:
    reason: str
    author: str
    reviewer: str
    paths: tuple = ()
    advisory: bool = False


class CheckError(ValueError):
    def __init__(self, reason, author, reviewer, paths=()):
        super().__init__(reason)
        self.result = CheckResult(reason, author, reviewer, paths)


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


def is_rfc_candidate(path):
    filename = Path(path)
    if not path.startswith("rfcs/") or filename.name.lower() in {"template.md", "readme.md"}:
        return False
    suffix = filename.suffix.lower()
    return suffix == ".md" or (suffix in {"", ".txt"} and re.match(r"IGNITE-[0-9]+\b", filename.name) is not None)


def check(pr, files):
    changed = {path for f in files for path in (f["filename"], f.get("previous_filename")) if path}
    signals = {path: hint for path in sorted(changed) if (hint := contract_review(path))}
    docs = {f["filename"] for f in files if f["status"] != "removed" and is_rfc_candidate(f["filename"])}
    marked = any(label["name"] == "public-behavior-change" for label in pr["labels"])
    paths = tuple(sorted(changed if marked else signals.keys() | docs))
    for path in sorted(docs):
        if not RFC_PATH.fullmatch(path):
            raise CheckError(
                f"Invalid RFC filename: {path}.",
                "Use rfcs/IGNITE-<ticket>-<short-title>.md, for example rfcs/IGNITE-12345-cache-timeout.md.",
                "Check that the filename contains the proposal's Jira ticket number.", paths,
            )

    if not (docs or signals or marked):
        return CheckResult(
            "This PR does not add or update an RFC or touch a known public-contract path.",
            "Continue with the usual PR review.",
            "If another file changes public behavior, add the public-behavior-change label.",
        )

    hints = []
    if docs:
        reason = "This PR adds or updates an RFC."
        author = "Discuss the proposal in this PR. Follow the RFC process for a new proposal or a change to the agreed plan."
        hints.append("For a new proposal or a changed plan, check the vote on dev@ and any later objections. "
                     "Wording-only edits need no new vote.")
        if not (marked or signals):
            hints.append("A product RFC goes with its first implementation. A documentation or process RFC can stand alone.")
        hints.append("If this PR includes code, compare the implementation and its compatibility tests with the agreed plan.")
    else:
        reason = ("The public-behavior-change label flags this PR for review."
                  if marked else "This PR changes a file that may affect public behavior.")
        reason += " This PR does not add or update an RFC."
        author = ("If public behavior changes, start from rfcs/TEMPLATE.md unless an approved RFC "
                  "or a process exception already covers the work.")
        hints.append("Check whether the code preserves the contract, follows an approved plan or needs an RFC.")

    hints.extend(dict.fromkeys(signals.values()))
    return CheckResult(reason, author, " ".join(hints), paths, advisory=True)


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
