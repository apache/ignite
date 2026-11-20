# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Post RFC guidance from a trusted workflow run to the current pull request."""

import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys

from check_rfc import api


MARKER = re.compile(r"^<!-- ignite-rfc-check (\d+) (\d+) (success|failure) ([a-f0-9]{64}) -->\n")
RESOLVED = "## RFC check passed\n\nThe RFC issues from the previous check are resolved."
RERUN = (
    "## RFC check needs a rerun\n\n"
    "GitHub still shows a failed job. The current PR passed when the reporter checked again.\n\n"
    "### For the author\n\nRerun the failed RFC process job from the Checks tab.\n\n"
    "### For the reviewer\n\nWait for the RFC process job to pass on this commit."
)


def eligible(run, repo):
    return (
        run["repository"]["full_name"] == repo and run["path"] == ".github/workflows/rfc-check.yml"
        and run["event"] in {"pull_request", "pull_request_review"}
        and run["status"] == "completed" and run["conclusion"] in {"success", "failure"}
        and run["head_repository"] and run["head_branch"]
        and re.fullmatch(r"[0-9a-f]{40}", run["head_sha"])
    )


def matches(pr, run, repo):
    source = run["head_repository"]["full_name"]
    head = pr["head"]
    # Review events on forks can report the base repository as head_repository.
    fork_review = run["event"] == "pull_request_review" and source == repo
    return (
        run.get("display_title") == f"RFC check for PR #{pr['number']}"
        and pr["state"] == "open" and pr["base"]["ref"] == "master" and pr["base"]["repo"]["full_name"] == repo
        and head["repo"] and (head["repo"]["full_name"] == source or fork_review)
        and head["ref"] == run["head_branch"] and head["sha"] == run["head_sha"]
        and (not run["pull_requests"] or any(link["number"] == pr["number"] for link in run["pull_requests"]))
    )


def review_state(route):
    reviews = api(route + "/reviews?per_page=100", pages=True)
    decisions = [(r["id"], r["state"], r["body"], r["commit_id"], r["submitted_at"]) for r in reviews
                 if r["state"] in {"APPROVED", "CHANGES_REQUESTED", "DISMISSED"}]
    return hashlib.sha256(json.dumps(decisions).encode()).hexdigest()


def metadata(pr):
    state = (pr["head"]["sha"], pr["base"]["sha"], pr["title"], pr["body"],
             sorted(label["name"] for label in pr["labels"]))
    return hashlib.sha256(json.dumps(state).encode()).hexdigest()


def recorded_job(root, run):
    attempt = f"{root}/actions/runs/{int(run['id'])}/attempts/{int(run['run_attempt'])}"
    jobs = [job for job in api(f"{attempt}/jobs?per_page=100")["jobs"] if job["name"] == "RFC process"]
    return jobs[0]["conclusion"] if len(jobs) == 1 else None


def newer_run(repo, run, pr):
    root = f"repos/{repo}"
    route = f"{root}/actions/workflows/rfc-check.yml/runs?head_sha={run['head_sha']}&status=completed&per_page=100"
    page = 1
    while True:
        response = api(f"{route}&page={page}")
        for other in response["workflow_runs"]:
            if ((other["id"], other["run_attempt"]) <= (run["id"], run["run_attempt"])
                    or not eligible(other, repo) or not matches(pr, other, repo)):
                continue
            if recorded_job(root, other) in {"success", "failure"}:
                return True
        if page * 100 >= response["total_count"]:
            return False
        # GitHub returns up to 1,000 filtered runs. Keep the existing comment if the list is incomplete.
        if not response["workflow_runs"] or page == 10:
            return True
        page += 1


def prepare(repo, run, destination):
    if not eligible(run, repo):
        return
    root = f"repos/{repo}"
    conclusion = recorded_job(root, run)
    if conclusion not in {"success", "failure"}:
        return

    # Fork events can omit workflow_run.pull_requests. Verify the run's PR number against the live head.
    title = re.fullmatch(r"RFC check for PR #([1-9][0-9]*)", run.get("display_title") or "")
    if not title:
        return
    pr = api(f"{root}/pulls/{title[1]}")
    if not matches(pr, run, repo):
        return
    state = review_state(f"{root}/pulls/{pr['number']}")
    event = {"number": pr["number"], "head": {"sha": pr["head"]["sha"]}, "base": {"sha": pr["base"]["sha"]}}
    context = {"pull_request": event, "metadata": metadata(pr), "reviews": state, "source_conclusion": conclusion}
    destination.write_text(json.dumps(context), encoding="utf-8")
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as out:
        print(f"pr_number={int(pr['number'])}", f"base_sha={pr['base']['sha']}", sep="\n", file=out)


def update(method, route, body=None):
    args = ["gh", "api", "--method", method, "--silent", route]
    if body is not None:
        args.extend(["--input", "-"])
    subprocess.run(args, input=json.dumps({"body": body}) if body is not None else None, text=True, check=True)


def publish(repo, run, context, summary_file, outcome):
    if not eligible(run, repo) or outcome not in {"success", "failure"}:
        return
    root = f"repos/{repo}"
    number = int(context["pull_request"]["number"])
    route = f"{root}/pulls/{number}"
    comments = []
    for comment in api(f"{root}/issues/{number}/comments?per_page=100", pages=True):
        user = comment.get("user") or {}
        marker = MARKER.match(comment["body"] or "")
        if user.get("login") == "github-actions[bot]" and user.get("type") == "Bot" and marker:
            comments.append((comment["id"], marker))

    source = int(run["id"]), int(run["run_attempt"])
    if any((int(m[1]), int(m[2])) > source for _, m in comments):
        return
    original = context["source_conclusion"]
    if original == outcome == "success" and not comments:
        return
    summary = summary_file.read_text(encoding="utf-8").strip()
    heading = "## RFC check passed" if outcome == "success" else "## RFC check failed"
    if not summary.startswith(heading + "\n") or len(summary) > 63000:
        raise ValueError("The checker did not produce a complete job summary.")
    body = RESOLVED if outcome == "success" else summary
    if original != outcome:
        if outcome == "success":
            body = RERUN
        else:
            body += "\n\nThe original job passed. Rerun RFC process to update its status."
        outcome = "failure"
    # Avoid notifications from @mentions in file names or diagnostic text.
    body = re.sub(r"(?<![\w.])@(?=[\w-])", "@\u200b", body)
    fingerprint = hashlib.sha256(body.encode()).hexdigest()
    retained = None
    if comments:
        cid, latest = max(comments, key=lambda item: item[0])
        if (latest[3], latest[4]) == (outcome, fingerprint):
            retained = cid
    if retained is not None and len(comments) == 1:
        return

    current = api(route)
    if not matches(current, run, repo) or metadata(current) != context["metadata"]:
        return
    if review_state(route) != context["reviews"] or newer_run(repo, run, current):
        return
    if retained is None:
        marker = f"<!-- ignite-rfc-check {source[0]} {source[1]} {outcome} {fingerprint} -->"
        details = f"https://github.com/{repo}/actions/runs/{source[0]}/attempts/{source[1]}"
        update("POST", f"{root}/issues/{number}/comments", f"{marker}\n{body}\n\n[Open the check]({details})")
    for cid, _ in comments:
        if cid != retained:
            update("DELETE", f"{root}/issues/comments/{cid}")


if __name__ == "__main__":
    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
    repo = os.environ["GITHUB_REPOSITORY"]
    if event["action"] == "completed":
        if sys.argv[1] == "prepare":
            prepare(repo, event["workflow_run"], Path(sys.argv[2]))
        elif sys.argv[1] == "publish":
            context = json.loads(Path(sys.argv[2]).read_text())
            publish(repo, event["workflow_run"], context, Path(sys.argv[3]), sys.argv[4])
