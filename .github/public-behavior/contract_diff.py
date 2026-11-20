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

"""Compare the contract declarations on the two sides of a pull request."""

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path, PurePosixPath
import subprocess
from tempfile import TemporaryDirectory
from urllib.parse import quote

from contract_paths import REST_SOURCES, SQL_ERRORS, SQL_STATES, java_contract_source
from launcher_contracts import launcher_changes, supports_launcher
from registered_contracts import supports_registered_contracts
from contract_values import contract_values, supports_values, value_changes


def _candidate(path):
    if not path or path.startswith("/") or ".." in PurePosixPath(path).parts:
        return False
    return java_contract_source(path) or supports_values(path) or supports_launcher(path)


def changed_contracts(files, repository, base, head, api):
    pairs = []
    for item in files:
        path = item["filename"]
        old = None if item["status"] in {"added", "copied"} else item.get("previous_filename", path)
        new = None if item["status"] == "removed" else path
        # We can compare a rename only when both locations are in a supported contract.
        if all(_candidate(name) for name in (old, new) if name):
            pairs.append((old, new))
    if not pairs:
        return {}

    old_paths = {old for old, _ in pairs if old}
    new_paths = {new for _, new in pairs if new}
    touched = {name for item in files for name in (item["filename"], item.get("previous_filename")) if name}
    companions = set()
    if REST_SOURCES & (old_paths | new_paths):
        companions.update(REST_SOURCES - touched)
    if SQL_ERRORS in old_paths & new_paths and SQL_STATES not in touched:
        companions.add(SQL_STATES)
    revisions = {"after": head}
    if old_paths:
        # Compare against the PR's branch point. The base branch can move independently.
        comparison = api(f"repos/{repository}/compare/{base}...{head}?per_page=1")
        revisions["before"] = comparison["merge_base_commit"]["sha"]

    def read(item):
        side, path = item
        route = f"repos/{repository}/contents/{quote(path)}?ref={revisions[side]}"
        if path in companions:
            return item, api(route, raw=True, optional=True)
        return item, api(route, raw=True)

    jobs = [("before", path) for path in sorted(old_paths)] + [("after", path) for path in sorted(new_paths | companions)]
    with ThreadPoolExecutor(max_workers=4) as pool:
        sources = dict(pool.map(read, jobs))
    for path in companions:
        contents = sources.pop(("after", path))
        if contents is not None:
            sources["before", path] = sources["after", path] = contents
    return compare_sources(pairs, sources)


def compare_sources(pairs, sources):
    changes = defaultdict(list)
    java = [(key, text) for key, text in sources.items() if java_contract_source(key[1])]
    for path, fact in java_changes(java):
        changes[path].append(fact)

    for old, new in pairs:
        if old == new and supports_launcher(new):
            findings = launcher_changes(new, sources["before", old], sources["after", new])
            if findings:
                changes[new].extend(findings)
        if old and new and supports_values(old) and supports_values(new):
            before = contract_values(old, sources["before", old])
            after = contract_values(new, sources["after", new])
            for key in sorted(before.keys() & after.keys()):
                findings = value_changes(key, before[key], after[key])
                if findings:
                    changes[new].extend(findings)
    return dict(sorted(changes.items()))


def java_changes(java):
    if not java:
        return []
    # Only the trusted analyzer is executed. The two directories contain PR sources as data.
    with TemporaryDirectory() as directory:
        root = Path(directory)
        for side in ("before", "after"):
            (root / side).mkdir()
        for (side, path), text in java:
            target = root / side / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(text, encoding="utf-8")
        checker = Path(__file__).resolve().parent
        compiled = root / "checker"
        compiled.mkdir()
        subprocess.run(["javac", "-proc:none", "-d", str(compiled), *map(str, sorted(checker.glob("*.java")))],
                       check=True)
        registered = sorted({path for (_, path), _ in java if supports_registered_contracts(path)})
        run = subprocess.run(["java", "-cp", str(compiled), "JavaContracts", str(root / "before"), str(root / "after"), *registered],
                             text=True, stdout=subprocess.PIPE, check=True)
        return [line.split("\t", 1) for line in run.stdout.splitlines()]
