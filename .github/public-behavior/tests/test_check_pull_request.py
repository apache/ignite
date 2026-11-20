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
from unittest.mock import patch

import check_pull_request as ci


DOC = "rfcs/IGNITE-123-sql.md"
PR = {"number": 42, "head": {"sha": "a" * 40}, "base": {"repo": {"full_name": "apache/ignite"}, "sha": "b" * 40},
      "body_html": ""}
API = "modules/core/src/main/java/org/apache/ignite/IgniteCache.java"


def file(name, status="modified", **kwargs):
    return dict(filename=name, status=status, **kwargs)


class CheckPullRequestTest(unittest.TestCase):
    def test_only_actual_links_to_project_prs_count(self):
        """Only a link destination can identify a prior PR in a rendered description."""
        parser = ci.PullRequestLinks("apache/ignite")
        parser.feed('<code>https://github.com/apache/ignite/pull/1</code>'
                    '<a href="https://example.com">https://github.com/apache/ignite/pull/2</a>'
                    '<a href="https://github.com/other/ignite/pull/3">plan</a>'
                    '<a href="https://github.com/apache/ignite/pull/4/files#diff-a">plan</a>')
        self.assertEqual(parser.numbers, ["4"])

    def test_an_unavailable_link_does_not_hide_a_valid_rfc(self):
        """One link fails to load. A second merged PR still supplies the RFC for review."""
        body = ('<a href="https://github.com/apache/ignite/pull/10">Prior work</a> '
                '<a href="https://github.com/apache/ignite/pull/11">Plan</a>')
        responses = [ci.CheckError("GitHub is unavailable.", "Re-run the check.", ""), {"merged_at": "2026-10-01"},
                     [file(DOC, "added")]]
        with TemporaryDirectory() as root, chdir(root), patch.object(ci, "api", side_effect=responses):
            Path(DOC).parent.mkdir()
            Path(DOC).touch()
            result = ci.check([file(API)], {API: ["Added API method cacheTimeout"]}, dict(PR, body_html=body))
        self.assertTrue(result.advisory)
        self.assertEqual(result.rfc_links[0][2], "https://github.com/apache/ignite/pull/11")

    def test_implementation_only_and_standalone_rfc_pass_quietly(self):
        for files in ([file(API)], [file(DOC)], [file("rfcs/TEMPLATE.md")]):
            with self.subTest(files=files):
                result = ci.check(files, {}, PR)
                self.assertFalse(result.advisory)
                self.assertEqual((result.author, result.reviewer, result.paths), ("", "", ()))

    def test_public_change_requires_a_proposal(self):
        changes = {API: ["Added API method cacheTimeout"]}
        with self.assertRaises(ci.CheckError) as caught:
            ci.check([file(API)], changes, PR)
        result = caught.exception.result
        self.assertIn("require an RFC", result.reason)
        self.assertIn("rfcs/TEMPLATE.md", result.author)
        self.assertEqual(result.changes, changes)

    def test_removing_an_rfc_does_not_cover_a_public_change(self):
        """Removing or moving a plan out of rfcs leaves the PR without an RFC file."""
        changes = {API: ["Added API method cacheTimeout"]}
        for edit in (file(DOC, "removed"), file("docs/archive.md", "renamed", previous_filename=DOC)):
            with self.subTest(edit=edit), self.assertRaises(ci.CheckError) as caught:
                ci.check([file(API), edit], changes, PR)
            self.assertIn("require an RFC", caught.exception.result.reason)

    def test_public_change_with_rfc_goes_to_review(self):
        changes = {API: ["Added API method cacheTimeout"]}
        for status in ("added", "modified"):
            with self.subTest(status=status):
                result = ci.check([file(API), file(DOC, status)], changes, PR)
                self.assertTrue(result.advisory)
                self.assertEqual(result.paths, (API,))
                self.assertEqual(result.rfc_links, ((DOC, f"https://github.com/apache/ignite/blob/{PR['head']['sha']}/{DOC}",
                                                     "https://github.com/apache/ignite/pull/42"),))
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
                    ci.check([file(API), file(name, "added")], changes, PR)
                result = caught.exception.result
                self.assertIn(f"Invalid RFC filename: {name}", result.reason)
                self.assertIn("rfcs/IGNITE-12345-cache-timeout.md", result.author)
                self.assertEqual(result.paths, (name, API))
                self.assertEqual(result.changes, changes)
