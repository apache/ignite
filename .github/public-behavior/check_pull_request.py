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
from html.parser import HTMLParser
from pathlib import Path

from check_summary import report
from contract_diff import changed_contracts

RFC_PATH = re.compile(r"rfcs/IGNITE-[0-9]+-[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*\.md")


@dataclass
class CheckResult:
    reason: str
    author: str
    reviewer: str
    paths: tuple = ()
    advisory: bool = False
    changes: dict | None = None
    rfc_links: tuple = ()


class CheckError(ValueError):
    def __init__(self, reason, author, reviewer, paths=(), changes=None):
        super().__init__(reason)
        self.result = CheckResult(reason, author, reviewer, paths, changes=changes)


def api(route, *, pages=False, raw=False, full=False, allow_missing=False):
    args = ["gh", "api"]
    if pages:
        args.extend(["--paginate", "--slurp"])
    if raw:
        args.extend(["-H", "Accept: application/vnd.github.raw+json"])
    if full:
        args.extend(["-H", "Accept: application/vnd.github.full+json"])
    try:
        response = subprocess.check_output([*args, route], stderr=subprocess.PIPE)
        if raw:
            return response.decode("utf-8")
        data = json.loads(response)
        return [item for page in data for item in page] if pages else data
    except (OSError, subprocess.CalledProcessError, ValueError, TypeError) as exc:
        detail = str(exc)
        if isinstance(exc, subprocess.CalledProcessError) and exc.stderr:
            detail = exc.stderr.decode(errors="replace").strip()
            if allow_missing and "(HTTP 404)" in detail:
                return None
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
    if suffix == ".md":
        return True
    document_suffixes = {"", ".txt", ".markdown", ".mdown", ".mkd", ".adoc", ".asciidoc"}
    return (filename.parent == Path("rfcs") and suffix in document_suffixes
            and re.match(r"IGNITE-[0-9]+\b", filename.name) is not None)


class PullRequestLinks(HTMLParser):
    """Read actual link destinations from GitHub's rendered PR description."""

    def __init__(self, repository):
        super().__init__()
        self.pattern = re.compile(rf"https://github\.com/{re.escape(repository)}/pull/([1-9][0-9]*)"
                                  r"(?:/(?:files|changes|checks|commits)(?:/[a-f0-9]+)?)?/?(?:[?#].*)?", re.I)
        self.numbers = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            match = self.pattern.fullmatch(dict(attrs).get("href") or "")
            if match:
                self.numbers.append(match[1])


def rfc_link(repository, revision, path, number):
    project = f"https://github.com/{repository}"
    return path, f"{project}/blob/{revision}/{path}", f"{project}/pull/{number}"


def linked_rfcs(pr, files):
    repository = pr["base"]["repo"]["full_name"]
    parser = PullRequestLinks(repository)
    parser.feed(pr["body_html"] or "")
    removed = {f["filename"] for f in files if f["status"] == "removed"}
    removed.update(f["previous_filename"] for f in files if f["status"] == "renamed")
    found = {}
    notes = []
    request_error = None
    for number in dict.fromkeys(parser.numbers):
        if number == str(pr["number"]):
            continue
        route = f"repos/{repository}/pulls/{number}"
        try:
            source = api(route, allow_missing=True)
            if source is None:
                notes.append(f"PR #{number} was not found.")
                continue
            if not source["merged_at"]:
                notes.append(f"PR #{number} has not been merged.")
                continue
            source_files = api(route + "/files?per_page=100", pages=True)
        except CheckError as error:
            request_error = error
            continue

        names = (f["filename"] for f in source_files if f["status"] != "removed")
        paths = [p for p in names if RFC_PATH.fullmatch(p) and p not in removed and Path(p).is_file()]
        if not paths:
            if len(source_files) != source["changed_files"]:
                notes.append(f"GitHub returned an incomplete file list for PR #{number}.")
            else:
                notes.append(f"PR #{number} has no matching RFC on this branch.")
        for path in paths:
            found.setdefault(path, rfc_link(repository, pr["base"]["sha"], path, number))
    if request_error and not found:
        raise request_error
    return tuple(found.values()), " ".join(notes)


def check(files, changes, pr):
    if not changes:
        return CheckResult("No change was found by the static contract checks.", "", "")

    docs = sorted(f["filename"] for f in files if f["status"] != "removed" and is_rfc_candidate(f["filename"]))
    paths = (*docs, *changes)
    for path in docs:
        if not RFC_PATH.fullmatch(path):
            raise CheckError(
                f"Invalid RFC filename: {path}.",
                "Use rfcs/IGNITE-<ticket>-<short-title>.md, for example rfcs/IGNITE-12345-cache-timeout.md.",
                "Check that the filename contains the proposal's Jira ticket number.", paths, changes,
            )

    if docs:
        links = tuple(rfc_link(pr["base"]["repo"]["full_name"], pr["head"]["sha"], path, pr["number"])
                      for path in docs)
        review = ("Compare the changes below and their compatibility tests with the RFC. "
                  "For a new or revised plan, check its vote on dev@ and any unresolved objections.")
        return CheckResult("This PR changes a public contract and adds or updates an RFC.", "", review,
                           tuple(changes), advisory=True, changes=changes, rfc_links=links)

    links, note = linked_rfcs(pr, files)
    if links:
        return CheckResult("The PR description links to a merged PR with an RFC.", "",
                           "Check that the linked RFC covers the changes below and their compatibility tests. "
                           "Check any required vote on dev@.", paths, advisory=True, changes=changes, rfc_links=links)

    raise CheckError(
        ("Public API and other public contract changes require an RFC. " + note).rstrip(),
        "Add a new RFC from rfcs/TEMPLATE.md or update an existing RFC in this PR. "
        "If an approved plan covers the change, paste its merged PR URL into this PR's description.",
        "Check the detected changes and their effect on existing users.", paths, changes,
    )


def main():
    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())["pull_request"]
    route = f"repos/{os.environ['GITHUB_REPOSITORY']}/pulls/{event['number']}"
    pr = api(route, full=True)
    if ((pr["head"]["sha"], pr["base"]["sha"]) != (event["head"]["sha"], event["base"]["sha"])
            or (pr.get("body") or "") != (event.get("body") or "")):
        raise CheckError(
            "This run is stale. The PR revision or description has changed.",
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
    repository = os.environ["GITHUB_REPOSITORY"]
    changes = changed_contracts(files, repository, pr["base"]["sha"], pr["head"]["sha"], api)
    return check(files, changes, pr)


if __name__ == "__main__":
    try:
        outcome = main()
        passed = True
    except CheckError as error:
        outcome = error.result
        passed = False
    except (OSError, KeyError, TypeError, ValueError, subprocess.SubprocessError) as error:
        outcome = CheckResult(
            f"The public contract check could not finish. {error}",
            "Rerun the job for the current PR revision.",
            "If the error recurs, check the workflow event and GitHub API response in the job log.",
        )
        passed = False
    report(passed, outcome.reason, outcome.author, outcome.reviewer, outcome.paths,
           advisory=outcome.advisory, changes=outcome.changes, rfc_links=outcome.rfc_links)
    sys.exit(0 if passed else 1)
