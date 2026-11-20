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

from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import check_rfc as ci


DOC = "rfcs/IGNITE-123-sql.md"
RESULT = "https://lists.apache.org/thread/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
API = "modules/core/src/main/java/org/apache/ignite/IgniteCache.java"
TEMPLATE = ("## Problem\nDescribe the problem.\n\n## Proposed change\nDescribe the change.")
TEXT = ("# New setting\nTicket: [IGNITE-123](https://issues.apache.org/jira/browse/IGNITE-123)\n"
        "## Problem\nUsers need a timeout.\n## Proposed change\nAdd a timeout option.")
PR = {"head": {"repo": {"full_name": "fork/ignite"}, "sha": "h"},
      "base": {"repo": {"full_name": "apache/ignite"}, "sha": "b"},
      "number": 1, "user": {"login": "author"}, "labels": [],
      "title": "IGNITE-123 Add a timeout", "body": "RFC-result: " + RESULT,
      "html_url": "https://github.com/apache/ignite/pull/1"}


def file(name, status="modified", **kwargs):
    return dict(filename=name, status=status, **kwargs)


class RFCCheckTest(unittest.TestCase):
    def test_rename_out_of_api_still_requires_review(self):
        with patch.object(ci, "review_lines", return_value=[]), self.assertRaises(ci.CheckError):
            ci.check(PR, [file("other", "renamed", previous_filename=API)], TEMPLATE)

    def test_ordinary_test_change_does_not_require_review(self):
        ci.check(PR, [file("modules/core/src/test/java/FooTest.java")], TEMPLATE)

    def test_reviewer_can_explain_refactoring_but_author_cannot(self):
        def run(login="maintainer", sha="h", permission="write"):
            review = {"commit_id": sha, "submitted_at": "2026-11-20T10:00:00Z", "state": "APPROVED", "user": {"login": login},
                      "body": "RFC-exempt: this refactor keeps the same inputs and results"}
            with patch.object(ci, "api", side_effect=[[review], [], {"permission": permission}]):
                ci.check(PR, [file(API)], TEMPLATE)
        run()
        for args in [("author", "h", "write"), ("maintainer", "old", "write"), ("outsider", "h", "read")]:
            with self.subTest(args=args), self.assertRaises(ci.CheckError):
                run(*args)

    def test_required_label_also_catches_behavior_outside_known_paths(self):
        pr = dict(PR, labels=[{"name": "rfc-required"}])
        with patch.object(ci, "review_lines", return_value=[]), self.assertRaises(ci.CheckError):
            ci.check(pr, [file("other")], TEMPLATE)

    def test_follow_up_links_existing_rfc(self):
        with patch.object(ci, "review_lines", return_value=["RFC-covered: " + DOC]):
            with patch.object(Path, "is_file", return_value=True):
                ci.check(PR, [file(API)], TEMPLATE)
            with patch.object(Path, "is_file", return_value=False), self.assertRaises(ci.CheckError):
                ci.check(PR, [file(API)], TEMPLATE)

    def test_new_rfc_must_match_approved_document(self):
        evidence = SimpleNamespace(versions={DOC: "voted"}, tally="positive result")
        with patch.object(ci, "content", return_value=(TEXT, "blob")), patch.object(ci, "verify_vote", return_value=evidence):
            ci.check(PR, [file(DOC, "added"), file(API)], TEMPLATE)
        with patch.object(ci, "content", side_effect=[(TEXT, "changed"), (TEXT, "voted")]), \
                patch.object(ci, "verify_vote", return_value=evidence), \
                patch.object(ci, "review_lines", return_value=[]), self.assertRaises(ci.CheckError):
            ci.check(PR, [file(DOC, "added")], TEMPLATE)

    def test_new_rfc_cannot_use_wording_exception_or_empty_template(self):
        for text in (TEXT.replace("Add a timeout option.", ""), TEMPLATE, TEXT):
            with self.subTest(text=text), patch.object(ci, "content", return_value=(text, "blob")), \
                    patch.object(ci, "review_lines", return_value=["RFC-exempt: wording only"]), self.assertRaises(ci.CheckError):
                ci.check(dict(PR, body="RFC-result: missing"), [file(DOC, "added")], TEMPLATE)

    def test_existing_file_can_be_clarified_or_amended_in_a_new_ticket(self):
        with patch.object(ci, "content", return_value=(TEXT, "blob")), \
                patch.object(ci, "review_lines", return_value=["RFC-exempt: typo only"]):
            ci.check(dict(PR, body=""), [file(DOC)], TEMPLATE)
        evidence = SimpleNamespace(versions={DOC: "voted"}, tally="positive result")
        with patch.object(ci, "content", return_value=(TEXT, "blob")), patch.object(ci, "review_lines", return_value=[]), \
                patch.object(ci, "verify_vote", return_value=evidence):
            ci.check(dict(PR, title="IGNITE-456 Extend the plan"), [file(DOC)], TEMPLATE)

    def test_early_or_rejected_vote_cannot_use_manual_verification(self):
        with patch.object(ci, "content", return_value=(TEXT, "blob")), \
                patch.object(ci, "verify_vote", side_effect=ci.VoteError("Vote closed too early")), \
                patch.object(ci, "review_lines", return_value=["RFC-verified: " + RESULT + " reviewed"]), self.assertRaises(ci.VoteError):
            ci.check(PR, [file(DOC, "added")], TEMPLATE)

    def test_withdrawn_or_retargeted_review_cannot_exempt_pr(self):
        review = {"state": "APPROVED", "user": {"login": "reviewer"}, "commit_id": "h",
                  "submitted_at": "2026-11-20T10:00:00Z", "body": "RFC-exempt: refactoring"}
        changed = dict(review, state="CHANGES_REQUESTED")
        retargeted = {"event": "base_ref_changed", "created_at": "2026-11-20T11:00:00Z"}
        for responses in ([[review, changed]], [[review], [retargeted]]):
            with patch.object(ci, "api", side_effect=responses), self.assertRaises(ci.CheckError):
                ci.check(PR, [file(API)], TEMPLATE)

    def test_result_link_accepts_ordinary_pr_links(self):
        self.assertEqual(ci.result_link(dict(PR, body=f"The vote passed. [Result]({RESULT})")), RESULT)
        with patch.object(ci, "api", return_value=[{"body": "Vote result: " + RESULT}]):
            self.assertEqual(ci.result_link(dict(PR, body="")), RESULT)

    def test_manual_check_binds_to_current_result(self):
        decision = "RFC-verified: " + RESULT + " confirmed three committer identities and the voted text"
        with patch.object(ci, "content", return_value=(TEXT, "blob")), \
                patch.object(ci, "verify_vote", side_effect=ci.ManualReviewRequired("Check identities")):
            with patch.object(ci, "review_lines", return_value=[decision]):
                ci.check(PR, [file(DOC, "added")], TEMPLATE)
            with patch.object(ci, "review_lines", return_value=[decision.replace(RESULT, RESULT + "2")]), self.assertRaises(ci.CheckError):
                ci.check(PR, [file(DOC, "added")], TEMPLATE)
