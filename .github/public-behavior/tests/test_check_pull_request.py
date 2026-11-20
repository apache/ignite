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

import check_pull_request as ci


DOC = "rfcs/IGNITE-123-sql.md"
API = "modules/core/src/main/java/org/apache/ignite/IgniteCache.java"


def file(name, status="modified", **kwargs):
    return dict(filename=name, status=status, **kwargs)


class CheckPullRequestTest(unittest.TestCase):
    def test_rename_out_of_api_reports_the_old_path(self):
        """Renaming a public API file keeps its original path in the advice."""
        result = ci.check([file("other", "renamed", previous_filename=API)])
        self.assertTrue(result.advisory)
        self.assertIn(API, result.paths)

    def test_ordinary_test_change_stays_quiet(self):
        """A test and a template edit have no public-behavior advice."""
        result = ci.check([file("modules/core/src/test/java/FooTest.java"), file("rfcs/TEMPLATE.md")])
        self.assertFalse(result.advisory)
        self.assertEqual(result.paths, ())
        self.assertNotIn("RFC", result.reason)

    def test_known_paths_suggest_the_relevant_compatibility_checks(self):
        """Storage, configuration and Maven changes each receive their own compatibility advice."""
        java = "modules/core/src/main/java/org/apache/ignite/"
        paths = [
            java + "internal/processors/cache/persistence/wal/serializer/RecordSerializer.java",
            java + "configuration/CacheConfiguration.java", java + "configuration/DataStorageConfiguration.java",
            "modules/core/pom.xml",
        ]
        result = ci.check([file(path) for path in paths])
        self.assertTrue(result.advisory)
        self.assertIn("This PR does not add or update an RFC.", result.reason)
        self.assertIn("data written by the previous version", result.reviewer)
        self.assertEqual(result.reviewer.count("configuration names, accepted values and defaults"), 1)
        self.assertIn("Maven coordinates and dependencies", result.reviewer)
        self.assertNotIn("client and server", result.reviewer)

    def test_added_updated_and_renamed_rfcs_are_present(self):
        for proposal in [file(DOC, "added"), file(DOC), file(DOC, "renamed", previous_filename="notes/sql.md")]:
            with self.subTest(status=proposal["status"]):
                result = ci.check([proposal])
                self.assertTrue(result.advisory)
                self.assertEqual(result.paths, (DOC,))
                self.assertIn("This PR adds or updates an RFC.", result.reason)
                self.assertIn("vote on dev@ and any later objections", result.reviewer)

    def test_invalid_filename_identifies_the_file_and_gives_an_example(self):
        for name in ["rfcs/cache-timeout.md", "rfcs/IGNITE-timeout.md", "rfcs/IGNITE-42-.md",
                     "rfcs/IGNITE-42 two.md", "rfcs/IGNITE-42-timeout.txt", "rfcs/IGNITE-42-timeout",
                     "rfcs/IGNITE-42-timeout.markdown", "rfcs/IGNITE-42-timeout.adoc"]:
            with self.subTest(name=name):
                with self.assertRaises(ci.CheckError) as caught:
                    ci.check([file(name, "added")])
                result = caught.exception.result
                self.assertIn(f"Invalid RFC filename: {name}", result.reason)
                self.assertIn("rfcs/IGNITE-12345-cache-timeout.md", result.author)
                self.assertEqual(result.paths, (name,))

    def test_support_files_and_removed_proposals_do_not_count(self):
        changes = [file("rfcs/TEMPLATE.md", "removed"), file("rfcs/README.md"), file(DOC, "removed"),
                   file("rfcs/IGNITE-123-timeout.png"), file("rfcs/notes.adoc"),
                   file("rfcs/assets/IGNITE-123-example.adoc"),
                   file("notes/sql.md", "renamed", previous_filename="rfcs/IGNITE-42-original.md")]
        self.assertFalse(ci.check(changes).advisory)
        result = ci.check(changes + [file(API)])
        self.assertIn("This PR does not add or update an RFC.", result.reason)
        self.assertEqual(result.paths, (API,))

    def test_rfc_with_other_documents_needs_no_implementation_review(self):
        result = ci.check([file(DOC), file("rfcs/TEMPLATE.md"), file("CONTRIBUTING.md")])
        self.assertIn("documentation or process RFC can stand alone", result.reviewer)

    def test_rfc_edit_and_implementation_get_separate_review_advice(self):
        result = ci.check([file(DOC), file(API)])
        self.assertTrue(result.advisory)
        self.assertEqual(result.paths, (DOC, API))
        self.assertIn("Wording-only edits need no new vote", result.reviewer)
        self.assertIn("implementation and its compatibility tests", result.reviewer)
        self.assertIn("Check results, errors and calls", result.reviewer)
