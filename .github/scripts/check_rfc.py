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
from pathlib import Path
from urllib.parse import quote

from rfc_scope import potential_contract
from rfc_vote import ManualReviewRequired, PERMALINK, VoteError, verify_vote

RFC_PATH = r"rfcs/IGNITE-([0-9]+)-[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*\.md"
TEMPLATE = "rfcs/TEMPLATE.md"


class CheckError(ValueError):
    pass


def api(route, *, pages=False):
    args = ["gh", "api"]
    if pages:
        args.extend(["--paginate", "--slurp"])
    data = json.loads(subprocess.check_output([*args, route]))
    return [item for page in data for item in page] if pages else data


def content(repo, path, sha):
    data = api(f"repos/{repo}/contents/{quote(path)}?ref={sha}")
    if data.get("type") != "file" or data.get("encoding") != "base64":
        raise CheckError(f"Cannot read the RFC as a file: {path}")
    return base64.b64decode(data["content"]).decode(), data["sha"]


def validate_new(path, text, title, template):
    ticket = "IGNITE-" + re.fullmatch(RFC_PATH, path)[1]
    if not re.match(rf"{ticket}\b", title) or f"(https://issues.apache.org/jira/browse/{ticket})" not in text:
        raise CheckError(f"Use {ticket} in the PR title and link its Jira ticket from {path}.")
    headings = re.findall(r"^## .+$", template, re.M)
    for heading in headings:
        section = re.search(rf"^{re.escape(heading)}\s*\n(.*?)(?=^## |\Z)", text, re.M | re.S)
        if not section or not section[1].strip():
            raise CheckError(f"Fill in '{heading[3:]}' in {path}.")
    prompts = template.split("## Problem", 1)[-1].splitlines()
    if any(line in text for line in prompts if line and not line.startswith("#")):
        raise CheckError(f"Replace the template prompts with answers in {path}.")


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
        def links_in(text):
            return {u.rstrip(".,") for u in re.findall(r"https://lists\.apache\.org/[^\s<>\[\]()]+", text)
                    if PERMALINK.fullmatch(u.rstrip(".,"))}
        links = links_in(body)
        if not links:
            route = f"repos/{pr['base']['repo']['full_name']}/issues/{pr['number']}/comments?per_page=100"
            for comment in api(route, pages=True):
                links.update(links_in(comment["body"] or ""))
    if len(links) == 1:
        link = links.pop()
        if PERMALINK.fullmatch(link):
            return link
    raise CheckError("Add one line to the PR description: RFC-result: <Apache archive URL of the RESULT message>.")


def check(pr, files, template=None):
    docs = {}
    signals = set()
    for f in files:
        path = f["filename"]
        old = f.get("previous_filename")
        if old == TEMPLATE or (path == TEMPLATE and f["status"] == "removed"):
            raise CheckError(f"Keep the RFC template at {TEMPLATE}.")
        if old and re.fullmatch(RFC_PATH, old):
            raise CheckError(f"Keep the existing RFC at {old}.")
        if path.startswith("rfcs/") and path.endswith(".md") and path != TEMPLATE:
            if not re.fullmatch(RFC_PATH, path):
                raise CheckError("Name the RFC rfcs/IGNITE-<ticket>-<short-title>.md.")
            if f["status"] == "removed":
                raise CheckError(f"Update the existing RFC at {path} so later PRs can still link to it.")
            docs[path] = f["status"]
        signals.update(p for p in (path, old) if p and potential_contract(p))
    marked = any(label["name"] == "rfc-required" for label in pr["labels"])
    if not (docs or signals or marked):
        return "No RFC files or configured public-contract paths changed."

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
            return f"Reviewer confirmed coverage by {covered[1]}."
        if not marked and reviewed(r"RFC-exempt:\s*\S.*"):
            return "Reviewer confirmed the RFC exception for this revision."
        locations = ", ".join(sorted(signals)[:3]) or "rfc-required label"
        raise CheckError(f"Review contract impact ({locations}). Include an RFC or record a reviewer decision. See .github/RFC-check.md.")

    pr_ticket = re.match(r"(IGNITE-[0-9]+)\b", pr["title"])
    if not pr_ticket:
        raise CheckError("Start the PR title with its Jira ticket, for example IGNITE-12345.")
    blobs = {}
    for path, status in docs.items():
        text, blobs[path] = content(pr["head"]["repo"]["full_name"], path, pr["head"]["sha"])
        if status in {"added", "renamed", "copied"}:
            validate_new(path, text, pr["title"], template if template is not None else Path(TEMPLATE).read_text())
    if not marked and all(s == "modified" for s in docs.values()) and reviewed(r"RFC-exempt:\s*\S.*"):
        return "Reviewer confirmed that the RFC changes only clarify wording."

    if any(status == "modified" and path.startswith(f"rfcs/{pr_ticket[1]}-") for path, status in docs.items()):
        raise CheckError("Use a new Jira ticket to update an approved RFC. Keep the existing RFC file.")

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
        if reviewed(r"RFC-verified:\s*" + re.escape(result) + r"\s+\S.*"):
            return f"Reviewer verified the result and the RFC at this revision. {error}"
        raise CheckError(f"{error} See manual verification in .github/RFC-check.md.") from error
    return f"Verified the dev-list vote. {evidence.tally}"


def main():
    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())["pull_request"]
    route = f"repos/{os.environ['GITHUB_REPOSITORY']}/pulls/{event['number']}"
    pr = api(route)
    if (pr["head"]["sha"], pr["base"]["sha"]) != (event["head"]["sha"], event["base"]["sha"]):
        raise CheckError("This run is stale. Check the run for the current PR revision.")
    files = api(route + "/files?per_page=100", pages=True)
    if len(files) != pr["changed_files"]:
        raise CheckError("GitHub returned an incomplete file list. Split the PR or review it manually.")
    print(check(pr, files))


if __name__ == "__main__":
    try:
        main()
    except (CheckError, VoteError, OSError, subprocess.CalledProcessError, ValueError) as e:
        sys.exit(f"RFC check failed: {e}")
