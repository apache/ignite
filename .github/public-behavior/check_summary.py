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

from html import escape
import os
from pathlib import Path
import sys


def _one_line(text, limit=1000):
    line = "".join(c if c.isprintable() else ascii(c)[1:-1] for c in text[:limit + 1])
    return line[:limit - 1] + "…" if len(line) > limit else line


def report_title(passed, advisory=False):
    if passed and advisory:
        return "Public behavior to review"
    return "Public behavior check passed" if passed else "Public behavior check failed"


def rules_reference():
    project = "https://github.com/apache/ignite"
    scope = f"{project}/blob/master/rfcs/IGNITE-29219-public-api-and-compatibility.md"
    process_doc = Path("rfcs/IGNITE-29220-rfc-development-process.md")
    if os.environ.get("GITHUB_REPOSITORY") == "apache/ignite" and process_doc.is_file():
        process = f"{project}/blob/master/{process_doc.as_posix()}"
    else:
        process = f"{project}/pull/13760/files#diff-964c536dd887c02e46887244405c46d2d8b5e15dc7a4480eb3a33fb64844b15a"
    return (
        f"See [Ignite's public contracts]({scope}) for the scope of compatibility review. "
        f"The [RFC process]({process}) describes the review rules."
    )


def report(passed, reason, author, reviewer, paths=(), *, advisory=False, changes=None, rfc_links=()):
    title = report_title(passed, advisory)
    reason, author, reviewer = _one_line(reason), _one_line(author), _one_line(reviewer, limit=3000)
    status = "🟡 Reviewer action needed. " if passed and advisory else ""
    log = [f"{title}: {reason}"]
    summary = [f"## {title}", f"<p>{status}{escape(reason, quote=False)}</p>"]
    for role, task in (("author", author), ("reviewer", reviewer)):
        if task:
            log.append(f"For the {role}: {task}")
            summary.extend([f"### For the {role}", f"<p>{escape(task, quote=False)}</p>"])

    if rfc_links:
        summary.append("### RFC files")
        for path, file_url in rfc_links[:8]:
            summary.append(f"[{path}]({file_url})")
            log.append(f"RFC: {file_url}")
        if len(rfc_links) > 8:
            summary.append(f"{len(rfc_links) - 8} more RFC files are listed in the PR.")

    if paths:
        items = []
        log.append("Files to review:")
        for path in paths[:8]:
            name = _one_line(path)
            log.append("  - " + name)
            facts = (changes or {}).get(path, ())
            lines = [_one_line(fact, limit=320) for fact in facts[:6]]
            if len(facts) > len(lines):
                lines.append(f"{len(facts) - len(lines)} more changes in this file.")
            log.extend("    - " + line for line in lines)
            details = "".join(f"<li>{escape(line, quote=False)}</li>" for line in lines)
            listing = f"<ul>{details}</ul>" if details else ""
            items.append(f"<li><code>{escape(name, quote=False)}</code>{listing}</li>")
        summary.extend(["### Files to review", "<ul>\n" + "\n".join(items) + "\n</ul>"])
        if len(paths) > 8:
            more = f"See {len(paths) - 8} more in the PR diff."
            log.append(more)
            summary.append(more)

    if not passed or advisory:
        rules = rules_reference()
        summary.append(rules)
        log.append(rules)

    # GitHub also accepts the older ##[command] syntax inside log lines.
    print("\n".join(log).replace("##[", r"##\["), file=sys.stdout if passed else sys.stderr)
    if passed and advisory and os.environ.get("PUBLIC_BEHAVIOR_REVIEW_WARNING") == "true":
        print("::warning title=Public behavior review::Review the RFC, any required dev@ vote, "
              "and the files listed in the check summary.")
    destination = os.environ.get("GITHUB_STEP_SUMMARY")
    if destination:
        try:
            with open(destination, "a", encoding="utf-8") as out:
                out.write("\n\n".join(summary) + "\n\n")
        except OSError:
            print("Could not write the GitHub job summary. See the result in this log.", file=sys.stderr)
