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

"""Report public behavior changes from a trusted workflow run on the current pull request."""

import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys

from check_pull_request import api
from check_summary import report_title, rules_reference


MARKER = re.compile(
    r"^<!-- ignite-(?P<check>public-behavior|rfc-check) (?P<run>\d+) (?P<attempt>\d+) "
    r"(?P<outcome>success|failure) (?P<fingerprint>[a-f0-9]{64}) -->\n"
)
JOB_CONCLUSIONS = {"success", "failure", "timed_out"}
RESOLVED = f"## {report_title(True)}\n\nThe check passed. There is nothing left to flag in this PR."
RERUN = (
    "## Public behavior changes need a fresh check\n\n"
    "GitHub still shows a failed job. The current PR passed when the reporter checked again.\n\n"
    "### For the author\n\nEdit the PR description or push a commit to start a fresh check.\n\n"
    "### For the reviewer\n\nWait for **Public behavior changes** to pass on this commit."
)
TIMEOUT = (
    "## Public behavior check timed out\n\n"
    "GitHub stopped the check before it could verify the change.\n\n"
    "### For the author\n\nCheck the job log for the stalled step and update the PR to start a fresh check.\n\n"
    "### For the reviewer\n\nWait for **Public behavior changes** to finish on this commit."
)


def eligible(run, repo):
    return (
        run["repository"]["full_name"] == repo and run["path"] == ".github/workflows/check-public-behavior.yml"
        and run["event"] == "pull_request"
        and run["status"] == "completed" and run["conclusion"] in JOB_CONCLUSIONS
        and run["head_repository"] and run["head_branch"]
        and re.fullmatch(r"[0-9a-f]{40}", run["head_sha"])
    )


def matches(pr, run, repo):
    source = run["head_repository"]["full_name"]
    head = pr["head"]
    return (
        run.get("display_title") == f"Public behavior changes for PR #{pr['number']}"
        and pr["state"] == "open" and pr["base"]["ref"] == "master" and pr["base"]["repo"]["full_name"] == repo
        and head["repo"] and head["repo"]["full_name"] == source
        and head["ref"] == run["head_branch"] and head["sha"] == run["head_sha"]
        and (not run["pull_requests"] or any(link["number"] == pr["number"] for link in run["pull_requests"]))
    )


def description_digest(pr):
    return hashlib.sha256((pr.get("body") or "").encode()).hexdigest()


def recorded_job(root, run):
    attempt = f"{root}/actions/runs/{int(run['id'])}/attempts/{int(run['run_attempt'])}"
    jobs = [job for job in api(f"{attempt}/jobs?per_page=100")["jobs"] if job["name"] == "Public behavior changes"]
    return jobs[0]["conclusion"] if len(jobs) == 1 else None


def newer_run(repo, run, pr):
    root = f"repos/{repo}"
    route = f"{root}/actions/workflows/check-public-behavior.yml/runs?head_sha={run['head_sha']}&status=completed&per_page=100"
    page = 1
    while True:
        response = api(f"{route}&page={page}")
        for other in response["workflow_runs"]:
            if ((other["id"], other["run_attempt"]) <= (run["id"], run["run_attempt"])
                    or not eligible(other, repo) or not matches(pr, other, repo)):
                continue
            if recorded_job(root, other) in JOB_CONCLUSIONS:
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
    if conclusion not in JOB_CONCLUSIONS:
        return

    # Fork events can omit workflow_run.pull_requests. Verify the run's PR number against the live head.
    title = re.fullmatch(r"Public behavior changes for PR #([1-9][0-9]*)", run.get("display_title") or "")
    if not title:
        return
    pr = api(f"{root}/pulls/{title[1]}")
    if not matches(pr, run, repo):
        return
    event = {"number": pr["number"], "head": {"sha": pr["head"]["sha"]}, "base": {"sha": pr["base"]["sha"]}}
    context = {"pull_request": event, "source_conclusion": conclusion, "description_digest": description_digest(pr)}
    destination.write_text(json.dumps(context), encoding="utf-8")
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as out:
        print(f"pr_number={int(pr['number'])}", f"base_sha={pr['base']['sha']}", f"conclusion={conclusion}", sep="\n", file=out)


def update(method, route, body=None):
    args = ["gh", "api", "--method", method, "--silent", route]
    if body is not None:
        args.extend(["--input", "-"])
    subprocess.run(args, input=json.dumps({"body": body}) if body is not None else None, text=True, check=True)


def comment_body(summary_file, original, outcome):
    if original == outcome == "timed_out":
        return TIMEOUT + "\n\n" + rules_reference()
    if outcome not in {"success", "failure"} or original == "timed_out":
        raise ValueError("The public behavior check returned inconsistent conclusions.")
    summary = summary_file.read_text(encoding="utf-8").strip()
    heading, _, content = summary.partition("\n")
    passed = outcome == "success"
    headings = {"## " + report_title(passed), "## " + report_title(passed, advisory=True)}
    if not content or heading not in headings or len(summary) > 63000:
        raise ValueError("The public behavior check did not produce a complete job summary.")
    if passed:
        if original != "success":
            return RERUN + "\n\n" + rules_reference()
        if heading == "## " + report_title(True, advisory=True):
            return summary
        return RESOLVED
    if original == "success":
        summary += "\n\nThe original job passed. Update the PR to start a fresh **Public behavior changes** check."
    return summary


def publish(repo, run, context, summary_file, outcome):
    if not eligible(run, repo) or outcome not in JOB_CONCLUSIONS:
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
    if any((int(m["run"]), int(m["attempt"])) > source for _, m in comments):
        return
    original = context["source_conclusion"]
    passed = original == outcome == "success"
    body = comment_body(summary_file, original, outcome)
    advisory = body.startswith("## " + report_title(True, advisory=True) + "\n")
    if passed and not advisory and not comments:
        return
    outcome = "success" if passed else "failure"
    # Avoid notifications from @mentions in file names or diagnostic text.
    body = re.sub(r"(?<![\w.])@(?=[\w-])", "@\u200b", body)
    fingerprint = hashlib.sha256(body.encode()).hexdigest()
    retained = None
    if comments:
        cid, latest = max(comments, key=lambda item: item[0])
        if (latest["check"], latest["outcome"], latest["fingerprint"]) == ("public-behavior", outcome, fingerprint):
            retained = cid
    if retained is not None and len(comments) == 1:
        return

    current = api(route)
    if not matches(current, run, repo) or current["base"]["sha"] != context["pull_request"]["base"]["sha"]:
        return
    if description_digest(current) != context["description_digest"]:
        return
    if newer_run(repo, run, current):
        return
    if retained is None:
        marker = f"<!-- ignite-public-behavior {source[0]} {source[1]} {outcome} {fingerprint} -->"
        if advisory and comments and latest["outcome"] == "failure":
            body += "\n\nThe issues from the previous check are resolved."
        details = f"https://github.com/{repo}/pull/{number}/checks"
        update("POST", f"{root}/issues/{number}/comments", f"{marker}\n{body}\n\n[Open PR checks]({details})")
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
