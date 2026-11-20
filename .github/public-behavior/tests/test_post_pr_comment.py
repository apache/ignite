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

from contextlib import redirect_stdout
from copy import deepcopy
from io import StringIO
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import post_pr_comment as rc
from check_summary import report


REPO = "apache/ignite"
FORK = {"full_name": "contributor/ignite"}
RUN = {"status": "completed", "conclusion": "failure", "event": "pull_request",
       "repository": {"full_name": REPO}, "head_repository": FORK, "head_sha": "a" * 40,
       "head_branch": "proposal", "path": ".github/workflows/check-public-behavior.yml", "id": 12, "run_attempt": 1,
       "pull_requests": [], "display_title": "Public behavior changes for PR #42"}
PR = {"state": "open", "number": 42, "head": {"ref": "proposal", "sha": RUN["head_sha"], "repo": FORK},
      "base": {"ref": "master", "sha": "b" * 40, "repo": RUN["repository"]},
      "title": "IGNITE-123 Proposal", "body": ""}
BOT = {"login": "github-actions[bot]", "type": "Bot"}
ADVISORY_TITLE = "Public behavior to review"
CHECKS_LINK = f"[Open PR checks](https://github.com/{REPO}/pull/42/checks)"


class PostPrCommentTest(unittest.TestCase):
    def setUp(self):
        """Expose a fork PR, its workflow runs and its comments through a local GitHub API fixture."""
        self.pr = deepcopy(PR)
        self.comments = []
        self.writes = []
        self.runs = [RUN]
        self.jobs = [{"name": "Public behavior changes", "conclusion": "failure"}]
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.enterContext(patch.dict(os.environ, GITHUB_OUTPUT=str(self.root / "output")))
        self.enterContext(patch.object(rc, "api", self.read))
        self.enterContext(patch.object(rc, "update", self.write))

    def read(self, route, *, pages=False):
        prefix = f"repos/{REPO}"
        if route == f"{prefix}/actions/runs/12/attempts/1/jobs?per_page=100":
            return {"jobs": self.jobs}
        if route == f"{prefix}/actions/runs/13/attempts/1/jobs?per_page=100":
            return {"jobs": [{"name": "Public behavior changes", "conclusion": "success"}]}
        if route == (f"{prefix}/actions/workflows/check-public-behavior.yml/runs?head_sha={RUN['head_sha']}"
                     "&status=completed&per_page=100&page=1"):
            return {"total_count": len(self.runs), "workflow_runs": self.runs}
        if route == f"{prefix}/pulls/42":
            return self.pr
        if route == f"{prefix}/issues/42/comments?per_page=100":
            self.assertTrue(pages)
            return self.comments
        self.fail(f"Unexpected request to {route}")

    def write(self, method, route, body=None):
        self.writes.append(method)
        if method == "POST":
            self.assertEqual(route, f"repos/{REPO}/issues/42/comments")
            self.comments.append({"id": self.next_id, "body": body, "user": BOT})
        elif method == "DELETE":
            self.comments = [c for c in self.comments if route != f"repos/{REPO}/issues/comments/{c['id']}"]
        else:
            self.fail(f"Unexpected request {method}")

    @property
    def next_id(self):
        return max([0] + [c["id"] for c in self.comments]) + 1

    def prepare(self):
        """Capture the current PR for the trusted checker."""
        filename = self.root / "pr.json"
        rc.prepare(REPO, RUN, filename)
        return json.loads(filename.read_text())

    def publish(self, context, outcome, text="", *, original=None, title=None):
        """Write the trusted summary and pair it with the original job outcome."""
        summary = self.root / "summary.md"
        if title is None:
            title = "Public behavior check passed" if outcome == "success" else "Public behavior check failed"
        summary.write_text(f"## {title}\n\n{text}\n\n{rc.rules_reference()}\n")
        rc.publish(REPO, RUN, dict(context, source_conclusion=original or outcome), summary, outcome)

    def test_prepare_resolves_fork_and_skips_an_unrelated_job(self):
        """Given a fork run without a PR number, resolve its current PR only for the public behavior job."""
        self.jobs[0]["name"] = "Public behavior check tests"
        rc.prepare(REPO, RUN, self.root / "pr.json")
        self.assertFalse((self.root / "output").exists())
        self.jobs[0]["name"] = "Public behavior changes"
        context = self.prepare()
        self.assertEqual(context["pull_request"],
                         {"number": 42, "head": {"sha": PR["head"]["sha"]}, "base": {"sha": PR["base"]["sha"]}})
        self.assertEqual(context["source_conclusion"], "failure")
        self.assertIn(f"pr_number=42\nbase_sha={PR['base']['sha']}\n", (self.root / "output").read_text())

    def test_skips_review_events(self):
        """When a review triggers a workflow on a fork, leave the PR alone."""
        dest = self.root / "pr.json"
        rc.prepare(REPO, dict(RUN, event="pull_request_review"), dest)
        self.assertFalse(dest.exists())

    def test_run_must_match_the_head_repository_and_pr(self):
        """When the same commit is reported for another repository or PR, skip the report."""
        dest = self.root / "pr.json"
        for details in ({"head_repository": {"full_name": REPO}}, {"pull_requests": [{"number": 17}]}):
            with self.subTest(details=details):
                rc.prepare(REPO, dict(RUN, **details), dest)
                self.assertFalse(dest.exists())

    def test_run_title_alone_cannot_select_a_different_pr(self):
        """Given a run title from a fork, require the exact template and matching live head before checking."""
        dest = self.root / "pr.json"
        rc.prepare(REPO, dict(RUN, display_title="A reference to Public behavior changes for PR #42"), dest)
        self.assertFalse(dest.exists())
        rc.prepare(REPO, dict(RUN, head_sha="9" * 40), dest)
        self.assertFalse(dest.exists())
        rc.prepare(REPO, RUN, dest)
        self.assertTrue(dest.exists())

    def test_posts_changed_guidance_and_a_single_resolution(self):
        """Given repeated runs, post new guidance and a resolution only when the situation changes."""
        context = self.prepare()
        self.publish(context, "success", "No matching paths.")
        self.assertEqual(self.writes, [])
        self.publish(context, "failure", "Use the required RFC filename.")
        self.assertEqual(self.writes, ["POST"])
        self.publish(context, "failure", "Use the required RFC filename.")
        self.assertEqual(self.writes, ["POST"])

        copied_marker = {**self.comments[0], "id": 98, "user": {"login": "contributor", "type": "User"}}
        self.comments.append(copied_marker)
        self.publish(context, "failure", "Include the Jira number in the filename.")
        self.assertEqual(self.writes[-2:], ["POST", "DELETE"])
        self.assertIn("Include the Jira number in the filename.", self.comments[-1]["body"])
        self.publish(context, "success", "RFC filename checked.")
        self.assertEqual(self.writes, ["POST", "POST", "DELETE", "POST", "DELETE"])
        self.assertIn("The check passed. There is nothing left to flag in this PR.", self.comments[-1]["body"])
        self.assertNotIn(rc.rules_reference(), self.comments[-1]["body"])
        self.publish(context, "success", "No matching paths.")
        self.assertEqual(len(self.writes), 5)
        self.assertIn(copied_marker, self.comments)

    def test_posts_review_guidance_on_the_first_green_run(self):
        """When the first check finds an RFC, publish its review marker even though the job passed."""
        self.jobs[0]["conclusion"] = "success"
        summary = self.root / "summary.md"
        with patch.dict(os.environ, GITHUB_STEP_SUMMARY=str(summary)), redirect_stdout(StringIO()):
            report(True, "This PR adds an RFC.", "", "Review the SQL result.", advisory=True)
        rc.publish(REPO, RUN, self.prepare(), summary, "success")
        self.assertEqual(self.writes, ["POST"])
        body = self.comments[0]["body"]
        self.assertIn("## Public behavior to review", body)
        self.assertIn("🟡 Reviewer action needed", body)
        self.assertIn("Review the SQL result.", body)
        self.assertIn(CHECKS_LINK, body)
        self.assertNotIn("resolved", body)

    def test_fixed_error_keeps_remaining_review_guidance(self):
        """When an RFC error is fixed, confirm the fix and keep the advice until the affected code changes."""
        context = self.prepare()
        self.publish(context, "failure", "Use the required RFC filename.")
        self.publish(context, "success", "Review the SQL result.", title=ADVISORY_TITLE)
        self.assertEqual(self.writes, ["POST", "POST", "DELETE"])
        self.assertEqual(len(self.comments), 1)
        self.assertIn("The issues from the previous check are resolved.", self.comments[0]["body"])
        self.assertIn("Review the SQL result.", self.comments[0]["body"])

        self.publish(context, "success", "Review the SQL result.", title=ADVISORY_TITLE)
        self.assertEqual(self.writes, ["POST", "POST", "DELETE"])
        self.publish(context, "success", "No matching paths.")
        self.assertEqual(self.writes[-2:], ["POST", "DELETE"])
        self.assertEqual(len(self.comments), 1)
        self.assertIn("There is nothing left to flag in this PR.", self.comments[0]["body"])
        self.assertNotIn("issues", self.comments[0]["body"])
        self.publish(context, "success", "No matching paths.")
        self.assertEqual(self.writes, ["POST", "POST", "DELETE", "POST", "DELETE"])

    def test_replaces_only_its_own_comment_from_the_old_workflow(self):
        """Given an old bot status, post the renamed status once and preserve comments from other authors."""
        context = self.prepare()
        for verdict in ("failure", "success"):
            with self.subTest(verdict=verdict):
                self.comments.clear()
                self.writes.clear()
                self.publish(context, "failure", "Use the required RFC filename.")
                if verdict == "success":
                    self.publish(context, "success", "RFC filename checked.")
                old = self.comments[0]
                old["body"] = old["body"].replace("<!-- ignite-public-behavior ", "<!-- ignite-rfc-check ", 1)
                other_bot = {**old, "id": 20, "user": {"login": "another[bot]", "type": "Bot"}}
                human = {**old, "id": 21, "user": {"login": "contributor", "type": "User"}}
                self.comments.extend([other_bot, human])
                self.writes.clear()
                self.publish(context, verdict, "Use the required RFC filename." if verdict == "failure" else "RFC filename checked.")
                self.assertEqual(self.writes, ["POST", "DELETE"])
                self.assertEqual(self.comments[:-1], [other_bot, human])
                self.assertTrue(self.comments[-1]["body"].startswith("<!-- ignite-public-behavior "))

    def test_requires_a_green_job_before_reporting_resolution(self):
        """Given a failed job, ask for a rerun even if the evidence already passes on a second check."""
        context = self.prepare()
        self.publish(context, "success", "RFC filename checked.", original="failure")
        self.assertEqual(self.writes, ["POST"])
        self.assertIn("Re-run the failed job or ask a maintainer", self.comments[-1]["body"])
        self.assertIn(CHECKS_LINK, self.comments[-1]["body"])
        self.assertEqual(self.comments[-1]["body"].count(rc.rules_reference()), 1)
        self.assertNotIn(rc.RESOLVED, self.comments[-1]["body"])
        self.publish(context, "success", "RFC filename checked.", original="success")
        self.assertIn(rc.RESOLVED, self.comments[-1]["body"])
        self.assertEqual(self.writes, ["POST", "POST", "DELETE"])

    def test_shows_new_findings_if_the_original_job_passed(self):
        """Given a green original job, publish current failures and ask for an updated check."""
        self.publish(self.prepare(), "failure", "Use the required RFC filename.", original="success")
        self.assertIn("Use the required RFC filename.", self.comments[0]["body"])
        self.assertIn("fresh", self.comments[0]["body"])
        self.assertIn(CHECKS_LINK, self.comments[0]["body"])
        self.assertEqual(self.comments[0]["body"].count(rc.rules_reference()), 1)

    def test_timeout_replaces_a_previous_resolution_without_waiting_for_evidence(self):
        """Given a resolved warning, when the next job times out, replace the old confirmation with recovery steps."""
        context = self.prepare()
        self.publish(context, "failure", "Use the required RFC filename.")
        self.publish(context, "success", "RFC filename checked.")
        self.jobs[0]["conclusion"] = "timed_out"
        run = dict(RUN, conclusion="timed_out")
        rc.prepare(REPO, run, self.root / "timeout.json")
        timeout = json.loads((self.root / "timeout.json").read_text())
        self.assertIn("conclusion=timed_out\n", (self.root / "output").read_text())
        rc.publish(REPO, run, timeout, self.root / "no-summary", "timed_out")
        self.assertEqual(len(self.comments), 1)
        self.assertIn(rc.TIMEOUT, self.comments[0]["body"])
        self.assertEqual(self.comments[0]["body"].count(rc.rules_reference()), 1)

    def test_late_failure_cannot_replace_a_newer_silent_success(self):
        """Given a first green with no comment, ignore an older failure delivered afterward on the same commit."""
        context = self.prepare()
        self.runs.append(dict(RUN, id=13, conclusion="success"))
        self.publish(context, "success", "RFC filename checked.", original="failure")
        self.assertEqual(self.writes, [])

    def test_skips_a_stale_pr_revision(self):
        """When the head or base commit changes, discard the outdated report."""
        context = self.prepare()
        changes = {"head": dict(PR["head"], sha="c" * 40), "base": dict(PR["base"], sha="c" * 40)}
        for field, value in changes.items():
            with self.subTest(field=field):
                self.pr = dict(PR, **{field: value})
                self.publish(context, "failure", "Use the required RFC filename.")
                self.assertEqual(self.writes, [])

    def test_unrelated_edits_keep_the_report_current(self):
        """Changing the PR title does not affect its RFC or the public behavior check."""
        context = self.prepare()
        self.pr["title"] = "IGNITE-456 Update the original RFC"
        self.publish(context, "failure", "Use the required RFC filename.")
        self.assertEqual(self.writes, ["POST"])

    def test_rfc_link_added_while_reporting_keeps_the_missing_rfc_error(self):
        """A link added to the description leaves the missing RFC file error in effect."""
        context = self.prepare()
        self.pr["body"] = "See `rfcs/IGNITE-123-cache-timeout.md`."
        self.publish(context, "failure", "Add an RFC for this change.")
        self.assertEqual(self.writes, ["POST"])
        self.assertIn("Add an RFC for this change.", self.comments[-1]["body"])

    def test_retry_keeps_the_new_comment_when_deletion_failed(self):
        """Given a failed cleanup, retry by deleting the old comment without reposting the new one."""
        context = self.prepare()
        self.publish(context, "failure", "Use the required RFC filename.")

        def interrupted(method, route, body=None):
            if method == "DELETE":
                raise OSError("API unavailable")
            self.write(method, route, body)

        with patch.object(rc, "update", interrupted), self.assertRaises(OSError):
            self.publish(context, "success", "RFC filename checked.")
        new_id = self.comments[-1]["id"]
        self.publish(context, "success", "RFC filename checked.")
        self.assertEqual([c["id"] for c in self.comments], [new_id])
        self.assertEqual(self.writes, ["POST", "POST", "DELETE"])
