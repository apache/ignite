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

from contextlib import chdir
from pathlib import Path
from tempfile import TemporaryDirectory
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
        with patch.object(ci, "review_lines", return_value=[]), self.assertRaises(ci.CheckError) as caught:
            ci.check(PR, [file("other", "renamed", previous_filename=API)], TEMPLATE)
        self.assertIn(API, caught.exception.result.paths)

    def test_ordinary_test_change_does_not_require_review(self):
        result = ci.check(PR, [file("modules/core/src/test/java/FooTest.java"), file(ci.TEMPLATE)], TEMPLATE)
        self.assertEqual(result.paths, ())

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

    def test_deleted_reviewer_does_not_hide_a_current_review(self):
        review = {"state": "APPROVED", "commit_id": "h", "submitted_at": "2026-11-20T10:00:00Z",
                  "body": "RFC-exempt: refactoring"}
        reviews = [dict(review, user=None), dict(review, user={"login": "maintainer"})]
        with patch.object(ci, "api", side_effect=[reviews, [], {"permission": "write"}]):
            result = ci.check(PR, [file(API)], TEMPLATE)
        self.assertIn("Reviewer confirmed the RFC exception", result.reason)

    def test_required_label_also_catches_behavior_outside_known_paths(self):
        pr = dict(PR, labels=[{"name": "rfc-required"}])
        with patch.object(ci, "review_lines", return_value=[]), self.assertRaises(ci.CheckError) as caught:
            ci.check(pr, [file("other")], TEMPLATE)
        self.assertIn("rfc-required label", caught.exception.result.reason)
        self.assertNotIn("RFC-exempt", caught.exception.result.reviewer)
        self.assertEqual(caught.exception.result.paths, ("other",))

    def test_missing_rfc_names_the_compatibility_checks_for_this_diff(self):
        """Given storage, configuration and Maven changes, a missing RFC prompts one check for each contract."""
        java = "modules/core/src/main/java/org/apache/ignite/"
        paths = [
            java + "internal/processors/cache/persistence/wal/serializer/RecordSerializer.java",
            java + "configuration/CacheConfiguration.java", java + "configuration/DataStorageConfiguration.java",
            "modules/core/pom.xml",
        ]
        with patch.object(ci, "review_lines", return_value=[]), self.assertRaises(ci.CheckError) as caught:
            ci.check(PR, [file(path) for path in paths])
        reviewer = caught.exception.result.reviewer
        self.assertIn("data written by the previous version", reviewer)
        self.assertEqual(reviewer.count("configuration names, accepted values and defaults"), 1)
        self.assertIn("Maven coordinates and dependencies", reviewer)
        self.assertNotIn("client and server", reviewer)
        self.assertIn("RFC-exempt:", reviewer)

    def test_follow_up_accepts_an_existing_rfc_after_an_incorrect_path(self):
        reviews = ["RFC-covered: rfcs/IGNITE-123-typo.md", "RFC-covered: " + DOC]
        with TemporaryDirectory() as base, chdir(base), patch.object(ci, "review_lines", return_value=reviews):
            with self.assertRaises(ci.CheckError) as missing:
                ci.check(PR, [file(API)], TEMPLATE)
            self.assertIn("rfcs/IGNITE-123-typo.md", missing.exception.result.reason)
            Path(DOC).parent.mkdir()
            Path(DOC).write_text(TEXT)
            result = ci.check(PR, [file(API)], TEMPLATE)
        self.assertIn(DOC, result.reason)

    def test_new_rfc_must_match_approved_document(self):
        evidence = SimpleNamespace(versions={DOC: "voted"}, tally="positive result", review_note="")
        with patch.object(ci, "content", return_value=(TEXT, "blob")), patch.object(ci, "verify_vote", return_value=evidence):
            result = ci.check(PR, [file(DOC, "added"), file(API)], TEMPLATE)
            self.assertEqual(set(result.paths), {DOC, API})
            self.assertIn(evidence.tally, result.reason)
            self.assertIn("first implementation", result.reviewer)
            self.assertIn("vote explicitly approved", result.reviewer)
        with patch.object(ci, "content", side_effect=[(TEXT, "changed"), (TEXT, "voted")]), \
                patch.object(ci, "verify_vote", return_value=evidence), \
                patch.object(ci, "review_lines", return_value=[]), self.assertRaises(ci.CheckError):
            ci.check(PR, [file(DOC, "added")], TEMPLATE)

    def test_new_rfc_cannot_use_wording_exception_or_empty_template(self):
        for text in (TEXT.replace("Add a timeout option.", ""), TEMPLATE, TEXT):
            with self.subTest(text=text), patch.object(ci, "content", return_value=(text, "blob")), \
                    patch.object(ci, "review_lines", return_value=["RFC-exempt: wording only"]), self.assertRaises(ci.CheckError):
                ci.check(dict(PR, body="RFC-result: missing"), [file(DOC, "added")], TEMPLATE)

    def test_reports_all_missing_or_empty_sections(self):
        """Given an RFC with an empty section and a missing section, the author sees both headings in one run."""
        text = TEXT.split("## Problem")[0] + "## Problem\n\n"
        with self.assertRaises(ci.CheckError) as caught:
            ci.validate_new(DOC, text, PR["title"], TEMPLATE)
        self.assertIn("Problem, Proposed change", caught.exception.result.author)

    def test_existing_file_can_be_clarified_or_amended_in_a_new_ticket(self):
        with patch.object(ci, "content", return_value=(TEXT, "blob")), \
                patch.object(ci, "review_lines", return_value=["RFC-exempt: typo only"]):
            with self.assertRaisesRegex(ci.CheckError, "reuses the Jira ticket"):
                ci.check(dict(PR, body=""), [file(DOC)], TEMPLATE)
            ci.check(dict(PR, title="IGNITE-456 Clarify the timeout", body=""), [file(DOC)], TEMPLATE)
        evidence = SimpleNamespace(versions={DOC: "voted"}, tally="positive result", review_note="")
        with patch.object(ci, "content", return_value=(TEXT, "blob")), patch.object(ci, "review_lines", return_value=[]), \
                patch.object(ci, "verify_vote", return_value=evidence):
            ci.check(dict(PR, title="IGNITE-456 Extend the plan"), [file(DOC)], TEMPLATE)

    def test_early_or_rejected_vote_cannot_use_manual_verification(self):
        with patch.object(ci, "content", return_value=(TEXT, "blob")), \
                patch.object(ci, "verify_vote", side_effect=ci.VoteError("Vote closed too early")), \
                patch.object(ci, "review_lines", return_value=["RFC-verified: " + RESULT + " reviewed"]), \
                self.assertRaisesRegex(ci.CheckError, "Vote closed too early") as caught:
            ci.check(PR, [file(DOC, "added")], TEMPLATE)
        self.assertIn("dev list", caught.exception.result.author)
        self.assertNotIn("RFC-verified", caught.exception.result.reviewer)

    def test_withdrawn_or_retargeted_review_cannot_exempt_pr(self):
        review = {"state": "APPROVED", "user": {"login": "reviewer"}, "commit_id": "h",
                  "submitted_at": "2026-11-20T10:00:00Z", "body": "RFC-exempt: refactoring"}
        changed = dict(review, state="CHANGES_REQUESTED")
        retargeted = {"event": "base_ref_changed", "created_at": "2026-11-20T11:00:00Z"}
        for responses in ([[review, changed]], [[review], [retargeted]]):
            with patch.object(ci, "api", side_effect=responses), self.assertRaises(ci.CheckError):
                ci.check(PR, [file(API)], TEMPLATE)

    def test_result_link_must_be_in_the_pr_description(self):
        """Given a result in a comment, checking the PR still asks the author to link it in the description."""
        self.assertEqual(ci.result_link(dict(PR, body=f"The vote passed. [Result]({RESULT})")), RESULT)
        with patch.object(ci, "api", return_value=[{"body": "Vote result: " + RESULT}]):
            with self.assertRaisesRegex(ci.CheckError, "PR description") as caught:
                ci.result_link(dict(PR, body=""))
        self.assertIn("RFC-result:", caught.exception.result.author)
        self.assertIn("Discuss the RFC", caught.exception.result.author)
        self.assertIn("Review the proposal", caught.exception.result.reviewer)

    def test_separate_result_uses_opening_from_review_of_this_result(self):
        opening = "https://lists.apache.org/thread/opening"

        def verify(*args, reviewer_opening):
            if reviewer_opening() != opening:
                raise ci.OpeningReviewRequired("The result does not identify its opening vote.")
            return SimpleNamespace(versions={DOC: "voted"}, tally="positive result", review_note="")

        with patch.object(ci, "content", return_value=(TEXT, "blob")), patch.object(ci, "verify_vote", side_effect=verify):
            line = f"RFC-verified: {RESULT} {opening} checked the vote and RFC text"
            with patch.object(ci, "review_lines", return_value=[line.replace(RESULT, RESULT + "2")]), \
                    self.assertRaises(ci.CheckError) as caught:
                ci.check(PR, [file(DOC, "added")], TEMPLATE)
            self.assertIn("<opening VOTE URL>", caught.exception.result.reviewer)
            with patch.object(ci, "review_lines", return_value=[line]):
                self.assertIn("positive result", ci.check(PR, [file(DOC, "added")], TEMPLATE).reason)

    def test_unclear_ballot_and_changed_rfc_are_reported_together(self):
        evidence = SimpleNamespace(versions={DOC: "voted"}, tally="", review_note="Check the committer's identity.")
        with patch.object(ci, "content", side_effect=[(TEXT, "current"), (TEXT, "voted")]), \
                patch.object(ci, "verify_vote", return_value=evidence), patch.object(ci, "review_lines", return_value=[]), \
                self.assertRaises(ci.CheckError) as caught:
            ci.check(PR, [file(DOC, "added")], TEMPLATE)
        self.assertIn("identity", caught.exception.result.reason)
        self.assertIn("differs from the voted version", caught.exception.result.reason)

    def test_archive_outage_calls_for_a_rerun(self):
        """Given an unavailable archive, checking a new RFC asks for readable evidence and a rerun."""
        with patch.object(ci, "content", return_value=(TEXT, "blob")), \
                patch("rfc_vote.urlopen", side_effect=OSError("archive unavailable")), \
                self.assertRaisesRegex(ci.CheckError, "archive unavailable") as caught:
            ci.check(PR, [file(DOC, "added")], TEMPLATE)
        self.assertIn("rerun", caught.exception.result.author)
        self.assertEqual(caught.exception.result.paths, (DOC,))

    def test_unreadable_proposal_identifies_the_file_and_revision(self):
        """Given a directory at the RFC path, the author sees which file and revision to fix."""
        with patch.object(ci, "api", return_value=[]), self.assertRaises(ci.CheckError) as caught:
            ci.check(PR, [file(DOC, "added")], TEMPLATE)
        outcome = caught.exception.result
        self.assertIn("fork/ignite@h", outcome.reason)
        self.assertIn(DOC, outcome.author)
        self.assertEqual(outcome.paths, (DOC,))

    def test_manual_check_binds_to_current_result(self):
        decision = "RFC-verified: " + RESULT + " confirmed three committer identities and the voted text"
        with patch.object(ci, "content", return_value=(TEXT, "blob")), \
                patch.object(ci, "verify_vote", side_effect=ci.ManualReviewRequired("Check identities")):
            with patch.object(ci, "review_lines", return_value=[decision]):
                ci.check(PR, [file(DOC, "added")], TEMPLATE)
            with patch.object(ci, "review_lines", return_value=[decision.replace(RESULT, RESULT + "2")]), \
                    self.assertRaises(ci.CheckError) as caught:
                ci.check(PR, [file(DOC, "added")], TEMPLATE)
            self.assertIn("RFC-verified: " + RESULT + " ", caught.exception.result.reviewer)
