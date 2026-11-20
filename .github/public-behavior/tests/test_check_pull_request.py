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
PR = {"number": 42, "head": {"sha": "a" * 40}, "base": {"repo": {"full_name": "apache/ignite"}, "ref": "master", "sha": "b" * 40},
      "body_html": ""}
API = "modules/core/src/main/java/org/apache/ignite/IgniteCache.java"


def file(name, status="modified", **kwargs):
    return dict(filename=name, status=status, **kwargs)


class CheckPullRequestTest(unittest.TestCase):
    def test_only_actual_links_to_project_rfc_files_count(self):
        """A link label, code sample, or file in a fork cannot identify the RFC."""
        parser = ci.RFCFileLinks("apache/ignite")
        plan = f"https://github.com/apache/ignite/blob/master/{DOC}"
        parser.feed(f'<code>{plan}</code><a href="https://example.com">{plan}</a>'
                    f'<a href="https://github.com/other/ignite/blob/master/{DOC}">fork</a>'
                    f'<a href="{plan}?plain=1#L38">plan</a>')
        self.assertEqual(parser.targets, [("master", DOC)])

    def test_an_unavailable_permalink_does_not_hide_a_working_link(self):
        """A second link to the target branch identifies the plan even if a metadata request fails."""
        project = "https://github.com/apache/ignite"
        old = "c" * 40
        body = f'<a href="{project}/blob/{old}/{DOC}">old</a> <a href="{project}/blob/master/{DOC}">current</a>'
        failure = ci.CheckError("GitHub is unavailable.", "Re-run the check.", "")
        with TemporaryDirectory() as root, chdir(root), patch.object(ci, "api", side_effect=failure) as request:
            Path(DOC).parent.mkdir()
            Path(DOC).touch()
            result = ci.check([file(API)], {API: ["Added API method cacheTimeout"]}, dict(PR, body_html=body))
        self.assertTrue(result.advisory)
        self.assertEqual(result.rfc_links, ((DOC, f"{project}/blob/{PR['base']['sha']}/{DOC}"),))
        request.assert_called_once_with(f"repos/apache/ignite/contents/{DOC}?ref={old}", exists=True)

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
                self.assertEqual(result.rfc_links, ((DOC, f"https://github.com/apache/ignite/blob/{PR['head']['sha']}/{DOC}"),))
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
