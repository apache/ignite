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

import unittest
from unittest.mock import patch

import check_pull_request as ci


DOC = "rfcs/IGNITE-123-sql.md"
API = "modules/core/src/main/java/org/apache/ignite/IgniteCache.java"
TEMPLATE = ("## Problem\nDescribe the problem.\n\n## Proposed change\nDescribe the change.")
TEXT = ("# New setting\nTicket: [IGNITE-123](https://issues.apache.org/jira/browse/IGNITE-123)\n"
        "## Problem\nUsers need a timeout.\n## Proposed change\nAdd a timeout option.")
PR = {"head": {"repo": {"full_name": "fork/ignite"}, "sha": "h"}, "labels": [], "title": "IGNITE-123 Add a timeout"}


def file(name, status="modified", **kwargs):
    return dict(filename=name, status=status, **kwargs)


class CheckPullRequestTest(unittest.TestCase):
    def test_rename_out_of_api_reports_the_old_path(self):
        """Renaming a public API file keeps its original path in the advice."""
        result = ci.check(PR, [file("other", "renamed", previous_filename=API)])
        self.assertTrue(result.advisory)
        self.assertIn(API, result.paths)

    def test_ordinary_test_change_stays_quiet(self):
        """A test and a template edit have no public-behavior advice."""
        result = ci.check(PR, [file("modules/core/src/test/java/FooTest.java"), file(ci.TEMPLATE)])
        self.assertFalse(result.advisory)
        self.assertEqual(result.paths, ())

    def test_label_requests_advice_for_an_unrecognized_path(self):
        """A labeled change gets review guidance even when no contract path matches."""
        pr = dict(PR, labels=[{"name": "public-behavior-change"}])
        result = ci.check(pr, [file("internal/Scheduler.java")])
        self.assertTrue(result.advisory)
        self.assertIn("public-behavior-change", result.reason)
        self.assertIn("process exception", result.author)
        self.assertEqual(result.paths, ("internal/Scheduler.java",))

    def test_known_paths_suggest_the_relevant_compatibility_checks(self):
        """Storage, configuration and Maven changes each receive their own compatibility advice."""
        java = "modules/core/src/main/java/org/apache/ignite/"
        paths = [
            java + "internal/processors/cache/persistence/wal/serializer/RecordSerializer.java",
            java + "configuration/CacheConfiguration.java", java + "configuration/DataStorageConfiguration.java",
            "modules/core/pom.xml",
        ]
        result = ci.check(PR, [file(path) for path in paths])
        self.assertTrue(result.advisory)
        self.assertIn("data written by the previous version", result.reviewer)
        self.assertEqual(result.reviewer.count("configuration names, accepted values and defaults"), 1)
        self.assertIn("Maven coordinates and dependencies", result.reviewer)
        self.assertNotIn("client and server", result.reviewer)

    def test_new_rfc_reminds_the_reviewer_to_check_the_vote(self):
        """A complete proposal can be reviewed with an empty PR description."""
        with patch.object(ci, "content", return_value=TEXT):
            result = ci.check(PR, [file(DOC, "added")], TEMPLATE)
        self.assertTrue(result.advisory)
        self.assertEqual(result.paths, (DOC,))
        self.assertIn("vote on dev@ and any later objections", result.reviewer)
        self.assertIn("first implementation", result.reviewer)

    def test_new_rfc_requires_the_template_sections(self):
        """An incomplete proposal reports the missing answer to its author."""
        with patch.object(ci, "content", return_value=TEXT.replace("Add a timeout option.", "")), \
                self.assertRaises(ci.CheckError) as caught:
            ci.check(PR, [file(DOC, "added")], TEMPLATE)
        self.assertIn("Proposed change", caught.exception.result.author)

    def test_reports_all_missing_or_empty_sections(self):
        """Given an RFC with an empty section and a missing section, the author sees both headings in one run."""
        text = TEXT.split("## Problem")[0] + "## Problem\n\n"
        with self.assertRaises(ci.CheckError) as caught:
            ci.validate_new(DOC, text, PR["title"], TEMPLATE)
        self.assertIn("Problem, Proposed change", caught.exception.result.author)

    def test_rfc_edit_and_implementation_get_separate_review_advice(self):
        """Changing an RFC and code shows both kinds of advice on known and manually flagged paths."""
        for path, labels in [(API, []), ("internal/Scheduler.java", [{"name": "public-behavior-change"}])]:
            pr = dict(PR, title="IGNITE-456 Clarify the timeout", labels=labels)
            with self.subTest(path=path), patch.object(ci, "content", return_value=TEXT):
                result = ci.check(pr, [file(DOC), file(path)], TEMPLATE)
                self.assertTrue(result.advisory)
                self.assertEqual(set(result.paths), {DOC, path})
                self.assertIn("Wording-only edits need no new vote", result.reviewer)
                self.assertIn("implementation and its compatibility tests", result.reviewer)

    def test_empty_rfc_edit_names_the_file(self):
        """Replacing a proposal with an empty file reports a structural error."""
        pr = dict(PR, title="IGNITE-456 Clarify the timeout")
        with patch.object(ci, "content", return_value=""), self.assertRaises(ci.CheckError) as caught:
            ci.check(pr, [file(DOC)])
        self.assertIn(f"{DOC} is empty", caught.exception.result.reason)

    def test_existing_file_can_be_clarified_or_amended_in_a_new_ticket(self):
        """An existing RFC needs a new ticket, and its reviewer decides whether the edit needs a vote."""
        with patch.object(ci, "content", return_value=TEXT):
            with self.assertRaisesRegex(ci.CheckError, "reuses the Jira ticket"):
                ci.check(PR, [file(DOC)], TEMPLATE)
            result = ci.check(dict(PR, title="IGNITE-456 Clarify the timeout"), [file(DOC)], TEMPLATE)
        self.assertIn("Wording-only edits need no new vote", result.reviewer)

    def test_unreadable_proposal_identifies_the_file_and_revision(self):
        """Given a directory at the RFC path, the author sees which file and revision to fix."""
        with patch.object(ci, "api", return_value=[]), self.assertRaises(ci.CheckError) as caught:
            ci.check(PR, [file(DOC, "added")], TEMPLATE)
        outcome = caught.exception.result
        self.assertIn("fork/ignite@h", outcome.reason)
        self.assertIn(DOC, outcome.author)
        self.assertEqual(outcome.paths, (DOC,))
