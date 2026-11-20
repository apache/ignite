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
BASE = "b" * 40
PLAN = "rfcs/IGNITE-123-timeout.md"
PLAN_LINK = f"https://github.com/apache/ignite/blob/master/{PLAN}"


def run_checker(file_pages, *, sources=None, changed_files=None, title="", link="", base_rfc=None, earlier_revision=None):
    """Run the workflow command with a fork PR event and GitHub responses for its list of files."""
    repo = "apache/ignite"
    pull = f"repos/{repo}/pulls/42"
    pr = {
        "number": 42, "title": title, "body": link,
        "head": {"repo": {"full_name": "contributor/ignite"}, "sha": HEAD},
        "base": {"repo": {"full_name": repo}, "ref": "master", "sha": BASE},
        "changed_files": sum(map(len, file_pages)) if changed_files is None else changed_files,
    }
    paged = "api --paginate --slurp"
    responses = {
        f"api -H Accept: application/vnd.github.full+json {pull}":
            dict(pr, body_html=f'<p><a href="{link}">{link}</a></p>' if link else ""),
        f"{paged} {pull}/files?per_page=100": file_pages,
    }
    if earlier_revision:
        responses[f"api --method HEAD --silent repos/{repo}/contents/{base_rfc}?ref={earlier_revision}"] = ""
    merge_base = "c" * 40
    responses[f"api repos/{repo}/compare/{pr['base']['sha']}...{HEAD}?per_page=1"] = {
        "merge_base_commit": {"sha": merge_base}
    }
    for (side, path), source in (sources or {}).items():
        rev = merge_base if side == "before" else HEAD
        responses[f"api -H Accept: application/vnd.github.raw+json repos/{repo}/contents/{path}?ref={rev}"] = source
    with TemporaryDirectory() as directory:
        work = Path(directory)
        if base_rfc:
            plan = work / base_rfc
            plan.parent.mkdir()
            plan.write_text("# Accepted plan\n")
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


def run_artifact_rename(*rfc_files, **fixture):
    """Run the workflow on a public artifact rename and the RFC files supplied by the test."""
    pom = "modules/core/pom.xml"
    before = "<project><artifactId>ignite-core</artifactId></project>"
    sources = {("before", pom): before, ("after", pom): before.replace("ignite-core", "ignite-runtime")}
    return run_checker([[{"filename": pom, "status": "modified"}, *rfc_files]], sources=sources, **fixture)


class CheckCommandTest(unittest.TestCase):
    def test_new_public_method_is_reported_on_page_two(self):
        first_page = [{"filename": f"docs/page-{i}.md", "status": "modified"} for i in range(100)]
        pages = [first_page, [{"filename": PUBLIC_API, "status": "modified"}]]
        original = "package org.apache.ignite; public interface IgniteCache {}"
        sources = {("before", PUBLIC_API): original,
                   ("after", PUBLIC_API): original.replace("{}", "{ int cacheTimeout(); }")}
        result, summary = run_checker(pages, sources=sources)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("require an RFC", summary)
        self.assertIn(PUBLIC_API, summary)
        self.assertIn("cacheTimeout", summary)
        self.assertIn("rfcs/TEMPLATE.md", summary)
        added_rfc = {"filename": PLAN, "status": "added"}
        result, summary = run_checker([first_page, [*pages[1], added_rfc]], sources=sources)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("🟡 Reviewer action needed", summary)
        self.assertIn("cacheTimeout", summary)
        self.assertIn("vote on dev@", summary)

    def test_follow_up_pr_links_the_existing_rfc_file(self):
        """A link to an RFC on the base branch puts its checked revision in the review report."""
        result, summary = run_artifact_rename(link=PLAN_LINK, base_rfc=PLAN)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("🟡 Reviewer action needed", summary)
        self.assertIn(f"[{PLAN}](https://github.com/apache/ignite/blob/{BASE}/{PLAN})", summary)
        self.assertIn("Maven artifactId changed from ignite-core to ignite-runtime", summary)

    def test_link_to_an_earlier_revision_shows_the_rfc_on_the_target_branch(self):
        """A permalink identifies an existing RFC. The reviewer sees the version on the target branch."""
        revision = "d" * 40
        result, summary = run_artifact_rename(link=f"https://github.com/apache/ignite/blob/{revision}/{PLAN}",
                                              base_rfc=PLAN, earlier_revision=revision)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(f"[{PLAN}](https://github.com/apache/ignite/blob/{BASE}/{PLAN})", summary)

    def test_link_must_point_to_an_rfc_file_in_this_repository(self):
        """An unrelated link leaves the change without an RFC even when the base contains a plan."""
        for link, base_rfc in (("https://github.com/apache/ignite/pull/17", PLAN),
                               (PLAN_LINK.replace("apache/ignite", "other/ignite"), PLAN),
                               (PLAN_LINK.replace(PLAN, "rfcs/timeout.md"), "rfcs/timeout.md")):
            with self.subTest(link=link):
                result, summary = run_artifact_rename(link=link, base_rfc=base_rfc)
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertIn("rfcs/TEMPLATE.md", summary)
                self.assertIn("Maven artifactId changed", summary)

    def test_linked_plan_must_still_exist_after_the_change(self):
        """The link must still point to an RFC on the base branch after this PR's changes."""
        removed = {"filename": PLAN, "status": "removed"}
        moved = {"filename": "docs/archive.md", "status": "renamed", "previous_filename": PLAN}
        for base_rfc, edits in ((None, []), (PLAN, [removed]), (PLAN, [moved])):
            with self.subTest(base_rfc=base_rfc, edits=edits):
                result, summary = run_artifact_rename(*edits, link=PLAN_LINK, base_rfc=base_rfc)
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertIn("Maven artifactId changed", summary)
                self.assertIn("rfcs/TEMPLATE.md", summary)

    def test_direct_rfc_link_does_not_hide_an_invalid_local_name(self):
        """A PR adds a misnamed RFC and links a valid file. The author must still fix the name."""
        local = {"filename": "rfcs/timeout.md", "status": "added"}
        result, summary = run_artifact_rename(local, link=PLAN_LINK, base_rfc=PLAN)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("Invalid RFC filename: rfcs/timeout.md", summary)

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

    def test_changed_jdbc_mapping_loads_the_unchanged_sqlstate_file(self):
        """A PR edits only the error mapping. The report shows the values a client receives."""
        core = "modules/core/src/main/java/org/apache/ignite/internal/processors/"
        errors = core + "cache/query/IgniteQueryErrorCode.java"
        states = core + "odbc/SqlStateCode.java"
        expr = """package org.apache.ignite.internal.processors.cache.query;
            class IgniteQueryErrorCode {
                public static final int DUPLICATE_KEY = 4001;
                static String codeToSqlState(int code) {
                    switch (code) { case DUPLICATE_KEY: return SqlStateCode.CONSTRAINT;
                        default: return SqlStateCode.INVALID; }
                }
            }"""
        sources = {("before", errors): expr,
                   ("after", errors): expr.replace("return SqlStateCode.CONSTRAINT", "return SqlStateCode.INVALID"),
                   ("after", states): """package org.apache.ignite.internal.processors.odbc;
                       class SqlStateCode { static final String CONSTRAINT="23000", INVALID="22000"; }"""}
        result, summary = run_checker([[{"filename": errors, "status": "modified"}]], sources=sources)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("require an RFC", summary)
        self.assertIn("SQLState for error 4001 from 23000 to 22000", summary)
