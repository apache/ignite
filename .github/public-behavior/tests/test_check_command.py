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


def run_checker(file_pages, *, changed_files=None):
    """Run the workflow command with a fork PR event and GitHub responses for its list of files."""
    repo = "apache/ignite"
    pull = f"repos/{repo}/pulls/42"
    pr = {
        "number": 42, "user": {"login": "contributor"}, "labels": [],
        "head": {"repo": {"full_name": "contributor/ignite"}, "sha": HEAD},
        "base": {"repo": {"full_name": repo}, "sha": "b" * 40},
        "changed_files": sum(map(len, file_pages)) if changed_files is None else changed_files,
    }
    paged = "api --paginate --slurp"
    responses = {
        f"api {pull}": pr,
        f"{paged} {pull}/files?per_page=100": [
            [{"filename": path, "status": "modified"} for path in page] for page in file_pages
        ],
    }
    with TemporaryDirectory() as directory:
        work = Path(directory)
        event = work / "event.json"
        event.write_text(json.dumps({"pull_request": pr}))
        (work / "responses.json").write_text(json.dumps(responses))
        gh = work / "gh"
        gh.write_text(
            "#!/usr/bin/env python3\n"
            "import json, sys\n"
            "from pathlib import Path\n"
            "responses = json.loads(Path('responses.json').read_text())\n"
            "print(json.dumps(responses[' '.join(sys.argv[1:])]))\n"
        )
        gh.chmod(0o755)
        summary = work / "summary.md"
        env = {
            "PATH": os.pathsep.join((directory, str(Path(sys.executable).parent), os.defpath)),
            "GITHUB_EVENT_PATH": str(event), "GITHUB_REPOSITORY": repo,
            "GITHUB_STEP_SUMMARY": str(summary),
        }
        result = subprocess.run([sys.executable, str(CHECKER)], cwd=work, env=env, capture_output=True, text=True)
        return result, summary.read_text()


class CheckCommandTest(unittest.TestCase):
    def test_public_api_on_second_page_shows_advice(self):
        """A public API on page two produces guidance and a successful job."""
        first_page = [f"docs/page-{i}.md" for i in range(100)]
        result, summary = run_checker([first_page, [PUBLIC_API]])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(PUBLIC_API, summary)
        self.assertIn("Check results, errors and calls", summary)
        self.assertIn("process exception", summary)

    def test_incomplete_file_list_requires_a_complete_pr(self):
        """Given an API file count mismatch, the job stops before review and explains GitHub's limit."""
        result, summary = run_checker([[PUBLIC_API]], changed_files=3001)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("incomplete file list (1 of 3001)", result.stderr)
        self.assertIn("3,000 files", summary)
