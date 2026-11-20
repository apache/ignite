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

from contextlib import chdir, redirect_stderr, redirect_stdout
from io import StringIO
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from check_summary import report, rules_reference


class CheckSummaryTest(unittest.TestCase):
    def test_summary_lists_actions_and_displays_file_names_as_text(self):
        """Given a flagged PR, report each role's next step and quote file names safely."""
        paths = ["a</code>\n##[warning]"] + [f"public/path{i}" for i in range(8)]
        with TemporaryDirectory() as directory:
            summary = Path(directory) / "summary.md"
            with patch.dict(os.environ, GITHUB_STEP_SUMMARY=str(summary)), redirect_stderr(StringIO()) as log:
                report(False, "Missing section.", "Fill in the section.", "Check the migration plan.", paths)
            rendered = summary.read_text()

        self.assertIn("## Public behavior check failed", rendered)
        self.assertIn(rules_reference(), rendered)
        self.assertIn("### For the author\n\n<p>Fill in the section.</p>", rendered)
        self.assertIn("### For the reviewer\n\n<p>Check the migration plan.</p>", rendered)
        self.assertIn("<code>a&lt;/code&gt;\\n##[warning]</code>", rendered)
        self.assertIn("See 1 more in the PR diff.", rendered)
        self.assertNotIn("public/path7", rendered)
        self.assertNotIn("##[warning]", log.getvalue())

    def test_passing_check_shows_what_to_review(self):
        with TemporaryDirectory() as directory:
            summary = Path(directory) / "summary.md"
            with patch.dict(os.environ, GITHUB_STEP_SUMMARY=str(summary)), redirect_stdout(StringIO()):
                report(True, "A client protocol file changed.", "Describe the change.", "Check old clients.", advisory=True)
            rendered = summary.read_text()

        self.assertTrue(rendered.startswith("## Public behavior to review\n"))
        self.assertIn("The automated checks passed.", rendered)
        self.assertIn("Check old clients.", rendered)

    def test_local_run_writes_guidance_to_the_log(self):
        """Given no GitHub summary file, a passing local run prints the verdict and both actions."""
        with patch.dict(os.environ, GITHUB_STEP_SUMMARY=""), redirect_stdout(StringIO()) as log:
            report(True, "RFC sections checked.", "Continue the usual review.", "Check the implementation.")

        self.assertIn("Public behavior check passed: RFC sections checked.", log.getvalue())
        self.assertIn(rules_reference(), log.getvalue())
        self.assertIn("For the author: Continue the usual review.", log.getvalue())
        self.assertIn("For the reviewer: Check the implementation.", log.getvalue())

    def test_links_to_the_process_document_available_on_apache(self):
        """Use the PR until the process has landed in the trusted Ignite checkout."""
        with TemporaryDirectory() as directory, chdir(directory), patch.dict(os.environ, GITHUB_REPOSITORY="apache/ignite"):
            self.assertIn("github.com/apache/ignite/pull/13760/files#diff-", rules_reference())
            path = Path("rfcs/IGNITE-29220-rfc-development-process.md")
            path.parent.mkdir()
            path.touch()
            links = rules_reference()
            self.assertIn("github.com/apache/ignite/blob/master/rfcs/IGNITE-29219-public-api-and-compatibility.md", links)
            self.assertIn("github.com/apache/ignite/blob/master/rfcs/IGNITE-29220-rfc-development-process.md", links)
            with patch.dict(os.environ, GITHUB_REPOSITORY="contributor/validation"):
                self.assertIn("github.com/apache/ignite/pull/13760/files#diff-", rules_reference())
