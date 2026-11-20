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
import sys


def _one_line(text):
    line = "".join(c if c.isprintable() else ascii(c)[1:-1] for c in text[:1001])
    return line[:999] + "…" if len(line) > 1000 else line


def report(passed, reason, author, reviewer, paths=()):
    title = "RFC check passed" if passed else "RFC check failed"
    reason, author, reviewer = map(_one_line, (reason, author, reviewer))
    log = [f"{title}: {reason}", f"For the author: {author}", f"For the reviewer: {reviewer}"]
    summary = [
        f"## {title}", f"<p>{escape(reason, quote=False)}</p>",
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

    # GitHub also accepts the older ##[command] syntax inside log lines.
    print("\n".join(log).replace("##[", r"##\["), file=sys.stdout if passed else sys.stderr)
    destination = os.environ.get("GITHUB_STEP_SUMMARY")
    if destination:
        try:
            with open(destination, "a", encoding="utf-8") as out:
                out.write("\n\n".join(summary) + "\n\n")
        except OSError:
            print("Could not write the GitHub job summary. See the result in this log.", file=sys.stderr)
