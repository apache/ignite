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

"""Check a pull request against the RFC process. Run from the trusted base checkout."""
import base64
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

from rfc_report import report
from rfc_scope import contract_review
from rfc_vote import EvidenceUnavailable, ManualReviewRequired, PERMALINK, VoteError, verify_vote

RFC_PATH = r"rfcs/IGNITE-([0-9]+)-[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*\.md"
TEMPLATE = "rfcs/TEMPLATE.md"


@dataclass
class CheckResult:
    reason: str
    author: str
    reviewer: str
    paths: tuple = ()


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
            return base64.b64decode(data["content"]).decode(), data["sha"]
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


def review_lines(pr):
    """A decision belongs to one head commit and to a reviewer with write access."""
    route = f"repos/{pr['base']['repo']['full_name']}"
    reviews = api(f"{route}/pulls/{pr['number']}/reviews?per_page=100", pages=True)
    latest = {r["user"]["login"]: r for r in reviews if r["state"] in {"APPROVED", "CHANGES_REQUESTED", "DISMISSED"}}
    retargeted = None
    for review in latest.values():
        if review["commit_id"] != pr["head"]["sha"] or review["state"] != "APPROVED":
            continue
        lines = [s.strip() for s in (review["body"] or "").splitlines()
                 if s.strip().startswith(("RFC-exempt:", "RFC-verified:", "RFC-covered:"))]
        user = review["user"]["login"]
        if lines and user != pr["user"]["login"]:
            if retargeted is None:
                events = api(f"{route}/issues/{pr['number']}/timeline?per_page=100", pages=True)
                retargeted = max((e["created_at"] for e in events if e["event"] == "base_ref_changed"), default="")
            if review["submitted_at"] <= retargeted:
                continue
            permission = api(f"{route}/collaborators/{quote(user, safe='')}/permission")
            if permission["permission"] in {"admin", "maintain", "write"}:
                yield from lines


def result_link(pr):
    body = (pr["body"] or "").replace("\r", "")
    explicit = re.findall(r"^RFC-result:[ \t]*(\S+)[ \t]*$", body, re.M)
    if explicit:
        links = set(explicit)
    else:
        links = {u.rstrip(".,") for u in re.findall(r"https://lists\.apache\.org/[^\s<>\[\]()]+", body)
                 if PERMALINK.fullmatch(u.rstrip(".,"))}
    if len(links) == 1:
        link = links.pop()
        if PERMALINK.fullmatch(link):
            return link
    raise CheckError(
        "The PR description does not identify one Apache dev-list result.",
        "Add RFC-result: <Apache archive URL of the RESULT message> on its own line in the PR description.",
        "Open the linked message and check that its own subject announces the result for this proposal.",
    )


def check(pr, files, template=None):
    changed = {path for f in files for path in (f["filename"], f.get("previous_filename")) if path}
    signals = {path: hint for path in sorted(changed) if (hint := contract_review(path))}
    proposals = {path for path in changed if path.startswith("rfcs/") and path.endswith(".md")}
    marked = any(label["name"] == "rfc-required" for label in pr["labels"])
    paths = tuple(sorted((signals.keys() | proposals) or (changed if marked else set())))
    try:
        result = check_evidence(pr, files, signals, marked, template)
    except CheckError as exc:
        exc.result.paths = paths
        raise
    if signals or marked or proposals - {TEMPLATE}:
        result.paths = paths
    return result


def check_evidence(pr, files, signals, marked, template):
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
    if not (docs or signals or marked):
        return CheckResult(
            "No RFC files or configured public-contract paths changed.",
            "Continue with the usual PR review.",
            "Check for user-visible behavior changes outside the configured paths. Apply rfc-required if an RFC is needed.",
        )

    # Fetch decisions lazily. Most PRs with RFCs pass using the dev-list result alone.
    decisions = None
    def reviewed(pattern):
        nonlocal decisions
        if decisions is None:
            decisions = list(review_lines(pr))
        return next((m for line in decisions if (m := re.fullmatch(pattern, line))), None)

    if not docs:
        covered = reviewed(r"RFC-covered:\s*(" + RFC_PATH + r")")
        if covered and Path(covered[1]).is_file():
            return CheckResult(
                f"Reviewer confirmed coverage by {covered[1]}.",
                "Keep the approved RFC linked in the PR. Request a fresh decision if you push another commit.",
                "Compare the implementation and its compatibility tests with the plan in the linked RFC.",
            )
        exempt = None if marked else reviewed(r"RFC-exempt:\s*(\S.*)")
        if exempt:
            return CheckResult(
                f"Reviewer confirmed the RFC exception for this revision. {exempt[1]}",
                "Request a fresh decision if you push another commit.",
                "Check the stated exception against the public behavior and compatibility tests in this revision.",
            )
        if covered:
            raise CheckError(
                f"The approving review names {covered[1]}, which is missing from the base commit.",
                "Link the approved RFC at its current path on the base branch.",
                "If you have write access, compare the implementation with that RFC and approve this commit "
                "with RFC-covered: <RFC path>.",
            )
        checks = " ".join(list(dict.fromkeys(signals.values()))[:3])
        if marked:
            raise CheckError(
                "The rfc-required label flags this change for an RFC.",
                "Draft the proposal from rfcs/TEMPLATE.md or link the approved RFC that this PR implements.",
                (checks + " " if checks else "") + "Check the behavior that prompted the label. "
                "A reviewer with write access can approve a covered "
                "implementation with RFC-covered: <RFC path>. If the label was applied in error, resolve it with its reviewer.",
            )
        raise CheckError(
            "Review contract impact. This PR changes a configured public-contract path without an RFC.",
            "Explain any exception in the PR. For an implementation, link its approved RFC. For a new proposal, "
            "start from rfcs/TEMPLATE.md and link the dev-list vote result in the PR description.",
            checks + " A reviewer with write access "
            "can approve this commit with RFC-covered: <RFC path> for an approved plan or RFC-exempt: <reason> for an exception.",
        )

    pr_ticket = re.match(r"(IGNITE-[0-9]+)\b", pr["title"])
    if not pr_ticket:
        raise CheckError(
            "The PR title is missing its Jira ticket.",
            "Start the PR title with its Jira ticket, for example IGNITE-12345.",
            "Check that this ticket describes the proposal or amendment under review.",
        )
    head_repo = pr["head"].get("repo")
    if not head_repo:
        raise CheckError(
            "GitHub cannot identify the source repository for this RFC revision.",
            "Restore access to the source repository or open a PR from an accessible branch.",
            "Check that the proposed RFC can be read at the PR head commit.",
        )
    blobs = {}
    for path, status in docs.items():
        text, blobs[path] = content(head_repo["full_name"], path, pr["head"]["sha"])
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
    if not marked and all(s == "modified" for s in docs.values()):
        exempt = reviewed(r"RFC-exempt:\s*(\S.*)")
        if exempt:
            return CheckResult(
                f"Reviewer confirmed that the RFC changes only clarify wording. {exempt[1]}",
                "Request a fresh decision if you change this revision.",
                "Compare the wording with the approved text. A change to the agreed behavior or migration plan needs a new vote.",
            )

    if any(status == "modified" and path.startswith(f"rfcs/{pr_ticket[1]}-") for path, status in docs.items()):
        raise CheckError(
            "The PR reuses the Jira ticket of the RFC it amends.",
            "Use a new Jira ticket for the amendment and edit the existing RFC at its original path.",
            "Check the revised behavior and migration plan against the original approval.",
        )

    result = result_link(pr)
    try:
        evidence = verify_vote(result, pr["html_url"], list(docs))
        for path, blob in blobs.items():
            if path not in evidence.versions:
                raise ManualReviewRequired(f"Check the voted version of {path}.")
            _, voted_blob = content(pr["base"]["repo"]["full_name"], path, evidence.versions[path])
            if voted_blob != blob:
                raise ManualReviewRequired(f"{path} differs from the voted version. Check whether the agreed plan changed.")
    except ManualReviewRequired as error:
        verified = reviewed(r"RFC-verified:\s*" + re.escape(result) + r"\s+(\S.*)")
        if verified:
            return CheckResult(
                f"Reviewer verified the result and the RFC at this revision. {verified[1]}",
                "Keep the result linked and request a fresh decision after new commits.",
                f"Compare the implementation and its tests with the RFC covered by {result}.",
            )
        raise CheckError(
            str(error),
            "Identify the relevant messages or voted file revision in the PR so the reviewer can resolve this question.",
            "Read the linked vote and compare it with this revision. If the evidence resolves the question above, "
            f"a reviewer with write access can approve this commit with RFC-verified: {result} <what you checked>.",
        ) from error
    except EvidenceUnavailable as error:
        raise CheckError(
            str(error), "Check access to the Apache archive or roster and rerun the job when it is readable.",
            "Wait for readable evidence before confirming the vote.",
        ) from error
    except VoteError as error:
        raise CheckError(
            str(error),
            "Check the linked opening and result against the finding. Link the correct result in the PR description. "
            "If the vote did not pass under the RFC process, resolve the issue on the dev list and hold a new vote.",
            f"Verify that {result} establishes approval for this PR and the proposed RFC revision.",
        ) from error
    return CheckResult(
        f"Verified the dev-list vote. {evidence.tally}",
        "Keep the result linked. Take changes to the agreed behavior or migration plan back to the dev list.",
        f"Compare the implementation and migration tests with the plan approved at {result}. "
        "Check that any approval by silence covered only compatible additions and that any request to skip deprecation had explicit approval.",
    )


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
    report(passed, outcome.reason, outcome.author, outcome.reviewer, outcome.paths)
    sys.exit(0 if passed else 1)
