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
        f"The check flags known files. Reviewers decide whether a change affects [Ignite's public contracts]({scope}). "
        f"Before merge, the reviewer checks any vote required by the [RFC process]({process})."
    )


def report(passed, reason, author, reviewer, paths=(), *, advisory=False):
    title = report_title(passed, advisory)
    reason, author, reviewer = _one_line(reason), _one_line(author), _one_line(reviewer, limit=3000)
    status = "The automated checks passed. " if passed and advisory else ""
    log = [f"{title}: {reason}", f"For the author: {author}", f"For the reviewer: {reviewer}"]
    summary = [
        f"## {title}", f"<p>{status}{escape(reason, quote=False)}</p>",
        "### For the author", f"<p>{escape(author, quote=False)}</p>",
        "### For the reviewer", f"<p>{escape(reviewer, quote=False)}</p>",
    ]

    paths = sorted(set(paths))
    if paths:
        shown = [_one_line(path) for path in paths[:8]]
        log.extend(["Files to review:", *("  - " + path for path in shown)])
        items = "\n".join(f"<li><code>{escape(path, quote=False)}</code></li>" for path in shown)
        summary.extend(["### Files to review", f"<ul>\n{items}\n</ul>"])
        if len(paths) > len(shown):
            more = f"See {len(paths) - len(shown)} more in the PR diff."
            log.append(more)
            summary.append(more)

    rules = rules_reference()
    summary.append(rules)
    log.append(rules)

    # GitHub also accepts the older ##[command] syntax inside log lines.
    print("\n".join(log).replace("##[", r"##\["), file=sys.stdout if passed else sys.stderr)
    destination = os.environ.get("GITHUB_STEP_SUMMARY")
    if destination:
        try:
            with open(destination, "a", encoding="utf-8") as out:
                out.write("\n\n".join(summary) + "\n\n")
        except OSError:
            print("Could not write the GitHub job summary. See the result in this log.", file=sys.stderr)
