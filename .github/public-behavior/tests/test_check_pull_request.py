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
import unittest

import check_pull_request as ci


DOC = "rfcs/IGNITE-123-sql.md"
API = "modules/core/src/main/java/org/apache/ignite/IgniteCache.java"


def file(name, status="modified", **kwargs):
    return dict(filename=name, status=status, **kwargs)


class CheckPullRequestTest(unittest.TestCase):
    def test_implementation_only_and_standalone_rfc_pass_quietly(self):
        for files in ([file(API)], [file(DOC)], [file("rfcs/TEMPLATE.md")]):
            with self.subTest(files=files):
                result = ci.check(files, {})
                self.assertFalse(result.advisory)
                self.assertEqual((result.author, result.reviewer, result.paths), ("", "", ()))

    def test_public_change_requires_a_proposal(self):
        changes = {API: ["Added API method cacheTimeout"]}
        with self.assertRaises(ci.CheckError) as caught:
            ci.check([file(API)], changes)
        result = caught.exception.result
        self.assertIn("need an RFC", result.reason)
        self.assertIn("rfcs/TEMPLATE.md", result.author)
        self.assertEqual(result.changes, changes)

    def test_public_change_with_rfc_goes_to_review(self):
        changes = {API: ["Added API method cacheTimeout"]}
        for status in ("added", "modified"):
            with self.subTest(status=status):
                result = ci.check([file(API), file(DOC, status)], changes)
                self.assertTrue(result.advisory)
                self.assertEqual(result.paths, (DOC, API))
                self.assertEqual(result.changes, changes)
                self.assertIn("vote on dev@", result.reviewer)

    def test_invalid_rfc_name_with_public_change(self):
        """When a public method is added, a misnamed RFC fails with the path and an example."""
        changes = {API: ["Added API method cacheTimeout"]}
        for name in ["rfcs/cache-timeout.md", "rfcs/IGNITE-timeout.md", "rfcs/IGNITE-42-.md",
                     "rfcs/IGNITE-42 two.md", "rfcs/IGNITE-42-timeout.txt", "rfcs/IGNITE-42-timeout",
                     "rfcs/IGNITE-42-timeout.markdown", "rfcs/IGNITE-42-timeout.adoc"]:
            with self.subTest(name=name):
                with self.assertRaises(ci.CheckError) as caught:
                    ci.check([file(API), file(name, "added")], changes)
                result = caught.exception.result
                self.assertIn(f"Invalid RFC filename: {name}", result.reason)
                self.assertIn("rfcs/IGNITE-12345-cache-timeout.md", result.author)
                self.assertEqual(result.paths, (name, API))
                self.assertEqual(result.changes, changes)

class LinkedRfcsTest(unittest.TestCase):
    def setUp(self):
        self.enterContext(chdir(self.enterContext(TemporaryDirectory())))
        Path("rfcs").mkdir()
        Path(DOC).touch()

    def test_implementation_can_follow_an_existing_plan(self):
        changes = {API: ["Added API method cacheTimeout"]}
        result = ci.check([file(API)], changes, f"See {DOC}")
        self.assertTrue(result.advisory)
        self.assertEqual(result.paths, (DOC, API))
        self.assertIn("no new vote", result.reviewer)

    def test_deleted_or_moved_plan_cannot_cover_implementation(self):
        changes = {API: ["Added API method cacheTimeout"]}
        for edit in (file(DOC, "removed"), file("docs/archive.md", "renamed", previous_filename=DOC)):
            with self.subTest(edit=edit), self.assertRaises(ci.CheckError):
                ci.check([file(API), edit], changes, f"See {DOC}")

    def test_existing_plan_is_found_in_the_base_checkout(self):
        """A later PR can reference its plan by path or by a link to its file in GitHub."""
        for reference in (
            f"The plan is `{DOC}`.",
            f"See [SQL plan](./{DOC}).",
            f"https://github.com/apache/ignite/blob/master/{DOC}",
            f"[Implementation plan](https://github.com/fixture/ignite/blob/master/{DOC})",
            f"[Approved change](https://github.com/apache/ignite/blob/master/{DOC}#proposed-change)",
        ):
            with self.subTest(reference=reference):
                self.assertEqual(ci.linked_rfcs(reference, "fixture/ignite"), [DOC])

    def test_reference_must_identify_an_existing_plan_in_the_project(self):
        """Only links to existing files in the project can identify a plan."""
        for reference in (
            f"[{DOC}](https://example.com/design)",
            f"https://example.com/blob/master/{DOC}",
            f"https://github.com/another/project/blob/master/{DOC}",
            f"https://github.com/apache/ignite/issues/42?file={DOC}",
            f"https://github.com/apache/ignite/issues/42#{DOC}",
            "rfcs/IGNITE-123-missing.md",
        ):
            with self.subTest(reference=reference):
                self.assertEqual(ci.linked_rfcs(reference, "fixture/ignite"), [])
