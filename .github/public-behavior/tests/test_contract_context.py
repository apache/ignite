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

from pathlib import PurePosixPath
import unittest

from cli_sources import CLI_RULE_SOURCES, ROOT_REGISTRY, cli_packages
from contract_diff import changed_contracts
from test_subcommand_arguments import builtin, DTO
from test_system_view_columns import CACHES, REGISTRY, REGISTER, ROW
from view_sources import VIEW_RULE_SOURCES


def pr_changes(base, edits):
    """Run a PR diff with unchanged registrations supplied by GitHub at the requested revision."""
    def api(route, *, raw=False, fields=None, optional=False):
        if "/compare/" in route:
            return {"merge_base_commit": {"sha": "branch-point"}}
        if route == "graphql":
            revision, package = fields["path"].split(":")
            assert revision == "branch-point"
            entries = [{"type": "blob", "name": PurePosixPath(path).name, "object": {"text": src, "isTruncated": False}}
                       for path, src in base.items() if str(PurePosixPath(path).parent) == package]
            return {"data": {"repository": {"object": {"entries": entries}}}}
        assert raw
        path, rev = route.split("/contents/")[1].split("?ref=")
        assert rev in {"branch-point", "head"}
        if rev == "head" and path in edits:
            return edits[path]
        if optional:
            return base.get(path)
        return base[path]

    files = [{"filename": path, "status": "modified" if path in base else "added"} for path in edits]
    return changed_contracts(files, "apache/ignite", "target-tip", "head", api)


class ContractContextTest(unittest.TestCase):
    def test_cli_arguments_use_the_snapshot_of_the_registered_family(self):
        """A DTO changes alone. The command and its unchanged registries still prove which flag changed."""
        before = builtin("@Argument(optional=true) boolean groups;")
        renamed = before[DTO].replace("boolean groups", "boolean showGroups")
        self.assertCountEqual(pr_changes(before, {DTO: renamed})[DTO],
                              ["Removed CLI option --cache list --groups", "Added CLI option --cache list --show-groups"])
        parser = next(path for path in CLI_RULE_SOURCES if path.endswith("/CommandUtils.java"))
        self.assertEqual(pr_changes(before, {DTO: renamed, parser: "// different argument naming rules"}), {})
        self.assertEqual(pr_changes({k: v for k, v in before.items() if k != ROOT_REGISTRY}, {DTO: renamed}), {})

    def test_new_dto_alone_cannot_change_an_existing_binding(self):
        self.assertEqual(pr_changes({}, {DTO: builtin("@Argument boolean groups;")[DTO]}), {})

    def test_nested_cli_packages_include_the_parent_registry(self):
        """A DTO under cache/scan uses both that package and the cache registry."""
        nested = DTO.replace("/cache/", "/cache/scan/")
        self.assertEqual(cli_packages([nested]), {str(PurePosixPath(nested).parent), str(PurePosixPath(DTO).parent)})

    def test_view_order_uses_the_registration_at_the_pr_revision(self):
        """A changed row produces a finding only when the registration and generator still support the inference."""
        before = {CACHES: ROW, REGISTRY: REGISTER}
        reordered = ROW.replace("@Order(1)", "@Order(-1)")
        findings = pr_changes(before, {CACHES: reordered})[CACHES]
        self.assertTrue(any("cacheId moved from position 2 to 1" in finding for finding in findings), findings)
        generator = next(path for path in VIEW_RULE_SOURCES if path.endswith("/SystemViewRowAttributeWalkerGenerator.java"))
        for also_changed in ({REGISTRY: REGISTER.replace("new CacheViewWalker()", "custom")},
                             {generator: "// new generator rules"}):
            self.assertEqual(pr_changes(before, {CACHES: reordered, **also_changed}), {})
