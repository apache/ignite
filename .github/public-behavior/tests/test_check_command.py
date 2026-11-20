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

import json
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest


CHECKER = Path(__file__).resolve().parents[1] / "check_pull_request.py"
PUBLIC_API = "modules/core/src/main/java/org/apache/ignite/IgniteCache.java"
HEAD = "a" * 40


def run_checker(file_pages, *, sources=None, changed_files=None, title="", linked_rfc=None):
    """Run the workflow command with a fork PR event and GitHub responses for its list of files."""
    repo = "apache/ignite"
    pull = f"repos/{repo}/pulls/42"
    pr = {
        "number": 42, "title": title,
        "head": {"repo": {"full_name": "contributor/ignite"}, "sha": HEAD},
        "base": {"repo": {"full_name": repo}, "sha": "b" * 40},
        "changed_files": sum(map(len, file_pages)) if changed_files is None else changed_files,
    }
    paged = "api --paginate --slurp"
    responses = {
        f"api {pull}": pr,
        f"{paged} {pull}/files?per_page=100": file_pages,
    }
    merge_base = "c" * 40
    responses[f"api repos/{repo}/compare/{pr['base']['sha']}...{HEAD}?per_page=1"] = {
        "merge_base_commit": {"sha": merge_base}
    }
    for (side, path), source in (sources or {}).items():
        rev = merge_base if side == "before" else HEAD
        responses[f"api -H Accept: application/vnd.github.raw+json repos/{repo}/contents/{path}?ref={rev}"] = source
    with TemporaryDirectory() as directory:
        work = Path(directory)
        if linked_rfc:
            plan = work / linked_rfc
            plan.parent.mkdir()
            plan.write_text("# Accepted plan\n")
            pr["body"] = f"See https://github.com/{repo}/blob/master/{linked_rfc}"
        event = work / "event.json"
        event.write_text(json.dumps({"pull_request": pr}))
        (work / "responses.json").write_text(json.dumps(responses))
        gh = work / "gh"
        gh.write_text(
            "#!/usr/bin/env python3\n"
            "import json, sys\n"
            "from pathlib import Path\n"
            "responses = json.loads(Path('responses.json').read_text())\n"
            "value = responses[' '.join(sys.argv[1:])]\n"
            "sys.stdout.write(value if isinstance(value, str) else json.dumps(value))\n"
        )
        gh.chmod(0o755)
        summary = work / "summary.md"
        env = {
            "PATH": os.pathsep.join((directory, str(Path(sys.executable).parent), os.environ["PATH"])),
            "GITHUB_EVENT_PATH": str(event), "GITHUB_REPOSITORY": repo,
            "GITHUB_STEP_SUMMARY": str(summary),
        }
        result = subprocess.run([sys.executable, str(CHECKER)], cwd=work, env=env, capture_output=True, text=True)
        return result, summary.read_text()


class CheckCommandTest(unittest.TestCase):
    def test_new_public_method_is_reported_on_page_two(self):
        first_page = [{"filename": f"docs/page-{i}.md", "status": "modified"} for i in range(100)]
        pages = [first_page, [{"filename": PUBLIC_API, "status": "modified"}]]
        original = "package org.apache.ignite; public interface IgniteCache {}"
        sources = {("before", PUBLIC_API): original,
                   ("after", PUBLIC_API): original.replace("{}", "{ int cacheTimeout(); }")}
        result, summary = run_checker(pages, sources=sources, linked_rfc="rfcs/IGNITE-123-timeout.md")
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("require an RFC file in this PR", summary)
        self.assertIn(PUBLIC_API, summary)
        self.assertIn("cacheTimeout", summary)
        self.assertIn("rfcs/TEMPLATE.md", summary)
        named = {"filename": "rfcs/IGNITE-123-timeout.md", "status": "added"}
        result, summary = run_checker([first_page, [*pages[1], named]], sources=sources)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("🟡 Reviewer action needed", summary)
        self.assertIn("cacheTimeout", summary)
        self.assertIn("vote on dev@", summary)

    def test_method_body_change_skips_rfc_checks(self):
        """A PR edits a method body and adds a misnamed RFC. The check reports no contract change."""
        source = "package org.apache.ignite; public class IgniteCache { public int size() { return 1; } }"
        edit = {"filename": PUBLIC_API, "status": "modified"}
        rfc = {"filename": "rfcs/cache-timeout.md", "status": "added"}
        sources = {("before", PUBLIC_API): source, ("after", PUBLIC_API): source.replace("return 1", "return 2")}
        result, summary = run_checker([[edit, rfc]], sources=sources)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("No change was found by the static contract checks", summary)
        self.assertNotIn("Reviewer action needed", summary)

    def test_empty_correctly_named_rfc_needs_no_action(self):
        file = {"filename": "rfcs/IGNITE-123-cache-timeout.md", "status": "modified"}
        result, summary = run_checker([[file]], title="IGNITE-456 Amend an existing RFC")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("Reviewer action needed", summary)

    def test_rfc_only_pr_passes_with_an_invalid_filename(self):
        """When only an RFC changes, its filename does not affect the result."""
        file = {"filename": "rfcs/cache-timeout.md", "status": "added"}
        result, summary = run_checker([[file]])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("No change was found by the static contract checks", summary)
        self.assertNotIn("rfcs/cache-timeout.md", summary)

    def test_incomplete_file_list_requires_a_complete_pr(self):
        """Given an API file count mismatch, the job stops before review and explains GitHub's limit."""
        result, summary = run_checker([[{"filename": PUBLIC_API, "status": "modified"}]], changed_files=3001)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("incomplete file list (1 of 3001)", result.stderr)
        self.assertIn("3,000 files", summary)
