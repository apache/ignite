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


class CheckError(ValueError):
    def __init__(self, reason, author, reviewer, paths=(), changes=None):
        super().__init__(reason)
        self.result = CheckResult(reason, author, reviewer, paths, changes=changes)


def api(route, *, pages=False, raw=False):
    args = ["gh", "api"]
    if pages:
        args.extend(["--paginate", "--slurp"])
    if raw:
        args.extend(["-H", "Accept: application/vnd.github.raw+json"])
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


def linked_rfcs(body, repository):
    """Find implementation plans named in the PR that exist in the trusted base checkout."""
    # Use Markdown link destinations so that a link's display text cannot identify a different RFC.
    destinations = re.sub(r"\[[^\]\n]*\]\(\s*<?([^\s)>]+)>?(?:\s+[^)\n]*)?\)", r" \1 ", body or "")
    plans = set()
    for word in destinations.split():
        target = word.lstrip("(<`").rstrip(".,;)>`")
        target = target.partition("#")[0].partition("?")[0]
        blob = re.fullmatch(r"https://github\.com/([^/?#]+/[^/?#]+)/blob/[^/?#]+/(.+)", target, re.I)
        if blob and blob[1].lower() in {"apache/ignite", repository.lower()}:
            path = blob[2]
        else:
            path = target.removeprefix("./")
        if RFC_PATH.fullmatch(path) and Path(path).is_file():
            plans.add(path)
    return sorted(plans)


def check(files, changes, description="", repository="apache/ignite"):
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
        reason = "This PR changes a public contract and adds or updates an RFC."
    else:
        removed = {f["filename"] for f in files if f["status"] == "removed"}
        removed.update(f["previous_filename"] for f in files if f["status"] == "renamed")
        linked = [p for p in linked_rfcs(description, repository) if p not in removed]
        if not linked:
            raise CheckError(
                "The public contract changes listed below need an RFC.",
                "Copy rfcs/TEMPLATE.md to rfcs/IGNITE-<ticket>-<short-title>.md and describe the change. "
                "If an existing RFC already covers it, link that file in the PR description.",
                "Check the detected changes and their effect on existing users.", paths, changes,
            )
        paths = (*linked, *changes)
        reason = "The PR description links an existing RFC for the flagged changes."

    review = ("Compare the changes below and their compatibility tests with the RFC. "
              "For a new or revised plan, check its vote on dev@ and any unresolved objections. "
              "Work covered by an approved plan needs no new vote.")
    return CheckResult(reason, "", review, paths, advisory=True, changes=changes)


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
    repository = os.environ["GITHUB_REPOSITORY"]
    changes = changed_contracts(files, repository, pr["base"]["sha"], pr["head"]["sha"], api)
    return check(files, changes, pr.get("body") or "", repository)


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
           advisory=outcome.advisory, changes=outcome.changes)
    sys.exit(0 if passed else 1)
