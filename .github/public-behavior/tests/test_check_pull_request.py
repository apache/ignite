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
        self.assertIn("require an RFC file in this PR", result.reason)
        self.assertIn("rfcs/TEMPLATE.md", result.author)
        self.assertEqual(result.changes, changes)

    def test_removing_an_rfc_does_not_cover_a_public_change(self):
        """Removing or moving a plan out of rfcs leaves the PR without an RFC file."""
        changes = {API: ["Added API method cacheTimeout"]}
        for edit in (file(DOC, "removed"), file("docs/archive.md", "renamed", previous_filename=DOC)):
            with self.subTest(edit=edit), self.assertRaises(ci.CheckError) as caught:
                ci.check([file(API), edit], changes)
            self.assertIn("require an RFC file in this PR", caught.exception.result.reason)

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
