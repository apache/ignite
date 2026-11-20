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

import unittest

from test_java_contracts import compare


ROOT = "modules/core/src/main/java/org/apache/ignite/internal/management/"
REGISTRY = ROOT + "IgniteCommandRegistry.java"
FAMILY = ROOT + "cache/CacheCommand.java"
COMMAND = ROOT + "cache/CacheListCommand.java"
DTO = ROOT + "cache/CacheListCommandArg.java"


def builtin(fields, group="", dto="CacheListCommandArg"):
    """Build a registry, executable subcommand and argument class with the given public fields."""
    return {
        REGISTRY: """package org.apache.ignite.internal.management;
            import org.apache.ignite.internal.management.api.CommandRegistryImpl;
            import org.apache.ignite.internal.management.cache.CacheCommand;
            class IgniteCommandRegistry extends CommandRegistryImpl {
                IgniteCommandRegistry() { super(new CacheCommand()); }
            }""",
        FAMILY: """package org.apache.ignite.internal.management.cache;
            import org.apache.ignite.internal.management.api.CommandRegistryImpl;
            class CacheCommand extends CommandRegistryImpl {
                CacheCommand() { super(new CacheListCommand()); }
            }""",
        COMMAND: """package org.apache.ignite.internal.management.cache;
            import org.apache.ignite.internal.management.api.ComputeCommand;
            class CacheListCommand implements ComputeCommand {
                Class<?> argClass() { return """ + dto + """.class; }
            }""",
        ROOT + "cache/" + dto + ".java": """package org.apache.ignite.internal.management.cache;
            import java.util.UUID;
            import org.apache.ignite.internal.dto.IgniteDataTransferObject;
            import org.apache.ignite.internal.management.api.Argument;
            import org.apache.ignite.internal.management.api.ArgumentGroup;
            import org.apache.ignite.internal.management.api.Positional;
            """ + group + " class " + dto + " extends IgniteDataTransferObject {" + fields + "}",
    }


class SubcommandArgumentsTest(unittest.TestCase):
    def test_named_option_changes_on_a_registered_command(self):
        """A command exposes new names and changes an existing parsed type and requirement."""
        fields = """@Argument(optional=true) int nodeId;
            @Argument boolean groups;
            @Argument(javaStyleName=true, withoutPrefix=true) boolean isLocal;"""
        old = builtin(fields, '@ArgumentGroup(value={"groups"}, optional=true)')
        new = builtin(fields.replace("optional=true) int", "optional=false) long")
                     .replace("boolean groups", "boolean groupList").replace("withoutPrefix=true", "withoutPrefix=false"),
                     '@ArgumentGroup(value={"groupList"}, optional=true)')
        self.assertCountEqual(compare(old, new), [
            f"{DTO}\tRemoved CLI option --cache list --groups",
            f"{DTO}\tAdded CLI option --cache list --group-list",
            f"{DTO}\tRemoved CLI option --cache list islocal",
            f"{DTO}\tAdded CLI option --cache list --islocal",
            f"{DTO}\tChanged CLI option --cache list --node-id type from int to long",
            f"{DTO}\tCLI option --cache list --node-id is now required",
        ])

    def test_positional_count_and_single_type_ignore_field_names(self):
        """A single slot changes type or becomes two slots. Renaming it preserves the input."""
        old = builtin("@Positional @Argument String filter;")
        self.assertEqual(compare(old, builtin("@Positional @Argument UUID filter;")),
                         [f"{DTO}\tChanged CLI positional argument for --cache list type from string to UUID"])
        self.assertEqual(compare(old, builtin("@Positional @Argument String pattern;")), [])
        two = "@Positional @Argument String filter; @Positional @Argument(optional=true) UUID node;"
        self.assertEqual(compare(old, builtin(two)),
                         [f"{DTO}\tChanged positional argument count for --cache list from 1 to 2"])
        a, b = two.split("; ", 1)
        self.assertEqual(compare(builtin(two), builtin(b + a + ";")), [])

    def test_same_public_spelling_and_grouped_requirement(self):
        """Changes to Java spelling, help or optional flags on grouped fields keep the CLI form."""
        old = builtin('@Argument(javaStyleName=true, optional=true, description="local") Boolean isLocal;',
                      '@ArgumentGroup(value={"isLocal"}, optional=true)')
        new = builtin('@Argument(javaStyleName=true, description="new help") boolean islocal;',
                      '@ArgumentGroup(value={"islocal"}, optional=true)')
        self.assertEqual(compare(old, new), [])

    def test_requires_registry_binding_and_known_dto(self):
        """An unreachable command, another DTO, or a computed field name leaves the decision to review."""
        old = builtin("@Argument boolean groups;")
        new = builtin("@Argument boolean config;")
        for path, start, end in ((REGISTRY, "new CacheCommand()", "provider()"),
                                 (FAMILY, "new CacheListCommand()", "subcommand()"),
                                 (COMMAND, "CacheListCommandArg.class", "currentArgs()"),
                                 (COMMAND, "implements ComputeCommand", "implements OtherCommand"),
                                 (DTO, "extends IgniteDataTransferObject", "extends UnknownParent")):
            with self.subTest(form=end):
                self.assertEqual(compare({**old, path: old[path].replace(start, end)},
                                         {**new, path: new[path].replace(start, end)}), [])
        child = builtin("@Argument(javaStyleName=USE_JAVA_STYLE) boolean groups;")
        self.assertEqual(compare(old, child), [])

    def test_explicit_argclass_switch_reports_against_command(self):
        """Switching a command to another existing DTO changes which options its users can pass."""
        old = builtin("@Argument boolean groups;")
        new = builtin("@Argument boolean config;", dto="OtherArgs")
        self.assertCountEqual(compare({**new, **old}, {**old, **new}), [
            f"{COMMAND}\tRemoved CLI option --cache list --groups",
            f"{COMMAND}\tAdded CLI option --cache list --config",
        ])

    def test_removal_of_a_deprecated_option_stays_with_the_reviewer(self):
        """The old command help marks one option as deprecated. Release rules govern its removal."""
        before = builtin('@Argument(description="Deprecated. Use --all instead") UUID nodeId; @Argument boolean all;')
        self.assertEqual(compare(before, builtin("@Argument boolean all;")), [])

    def test_an_inherited_dto_field_belongs_to_the_subcommand(self):
        """The DTO inherits a named option from a known parent. Only its defining file changes."""
        old = builtin("@Argument boolean groups;")
        old[DTO] = old[DTO].replace("extends IgniteDataTransferObject", "extends CommonArgs")
        parent = ROOT + "cache/CommonArgs.java"
        old[parent] = """package org.apache.ignite.internal.management.cache;
            import org.apache.ignite.internal.management.api.Argument;
            import org.apache.ignite.internal.dto.IgniteDataTransferObject;
            class CommonArgs extends IgniteDataTransferObject { @Argument int timeout; }"""
        self.assertEqual(compare(old, {**old, parent: old[parent].replace("int timeout", "long timeout")}),
                         [f"{parent}\tChanged CLI option --cache list --timeout type from int to long"])

    def test_literal_group_restrictions_govern_the_same_public_options(self):
        """Two named alternatives become required and exclusive. Reordering the list preserves the rules."""
        fields = "@Argument boolean groups; @Argument boolean seq;"
        group = '@ArgumentGroup(value={"seq","groups"}, optional=true, onlyOneOf=false)'
        before = builtin(fields, group)
        after = builtin(fields, group.replace("optional=true", "optional=false").replace("onlyOneOf=false", "onlyOneOf=true"))
        self.assertCountEqual(compare(before, after), [
            f"{DTO}\t--cache list now requires at least one of --groups, --seq",
            f"{DTO}\t--cache list now accepts at most one of --groups, --seq",
        ])
        self.assertEqual(compare(before, builtin(fields, group.replace('"seq","groups"', '"groups","seq"'))), [])

    def test_first_option_after_noarg_and_inherited_executable_command(self):
        """A registered subclass inherits argClass and ComputeCommand. Editing the base adds its first option."""
        old = builtin("@Argument boolean groups;")
        old[COMMAND] = """package org.apache.ignite.internal.management.cache;
            class CacheListCommand extends CacheAbstractCommand {}"""
        base = ROOT + "cache/CacheAbstractCommand.java"
        old[base] = """package org.apache.ignite.internal.management.cache;
            import org.apache.ignite.internal.management.api.ComputeCommand;
            import org.apache.ignite.internal.management.api.NoArg;
            abstract class CacheAbstractCommand implements ComputeCommand {
                Class<?> argClass() { return NoArg.class; }
            }"""
        old[ROOT + "api/NoArg.java"] = """package org.apache.ignite.internal.management.api;
            import org.apache.ignite.internal.dto.IgniteDataTransferObject;
            class NoArg extends IgniteDataTransferObject {}"""
        after = {**old, base: old[base].replace("return NoArg.class", "return CacheListCommandArg.class")}
        self.assertEqual(compare(old, after), [f"{base}\tAdded CLI option --cache list --groups"])
        # A runtime choice on the subclass overrides the base binding.
        override = old[COMMAND].replace("{}", "{ Class<?> argClass() { return chooseDto(); } }")
        self.assertEqual(compare({**old, COMMAND: override}, {**after, COMMAND: override}), [])

    def test_shared_dto_reports_both_registered_invocations(self):
        """Two executable subcommands share a DTO; each report uses its accepted command spelling."""
        before = builtin("@Argument int timeout;")
        before[FAMILY] = before[FAMILY].replace("class CacheCommand", "import org.apache.ignite.internal.management.api.CliSubcommandsWithPrefix;\n"
                                                 "@CliSubcommandsWithPrefix class CacheCommand")
        before[FAMILY] = before[FAMILY].replace("new CacheListCommand()", "new CacheListCommand(), new CacheListNodesCommand()")
        before[ROOT + "cache/CacheListNodesCommand.java"] = before[COMMAND].replace("class CacheListCommand ", "class CacheListNodesCommand ")
        after = {**before, DTO: before[DTO].replace("int timeout", "long timeout")}
        self.assertCountEqual(compare(before, after), [
            f"{DTO}\tChanged CLI option --cache --list --timeout type from int to long",
            f"{DTO}\tChanged CLI option --cache --list-nodes --timeout type from int to long",
        ])

    def test_existing_optional_flags_gain_or_lose_a_group_rule(self):
        """Initially independent flags become required alternatives. Removing the group restores either choice."""
        opts = "@Argument(optional=true) boolean left; @Argument(optional=true) boolean right;"
        group = '@ArgumentGroup(value={"left", "right"}, optional=false, onlyOneOf=true)'
        separate, restricted = builtin(opts), builtin(opts, group)
        self.assertCountEqual(compare(separate, restricted), [
            f"{DTO}\t--cache list now requires at least one of --left, --right",
            f"{DTO}\t--cache list now accepts at most one of --left, --right",
        ])
        self.assertCountEqual(compare(restricted, separate), [
            f"{DTO}\t--cache list now allows omitting --left, --right",
            f"{DTO}\t--cache list now allows combining --left, --right",
        ])
        unconstrained = group.replace("optional=false", "optional=true").replace("onlyOneOf=true", "onlyOneOf=false")
        self.assertEqual(compare(separate, builtin(opts, unconstrained)), [])
        unknown = group.replace("optional=false", "optional=OPTIONAL").replace("onlyOneOf=true", "onlyOneOf=EXCLUSIVE")
        self.assertEqual(compare(separate, builtin(opts, unknown)), [])
        overlapping = '@ArgumentGroup(value={"left"}, optional=true) ' + group
        self.assertEqual(compare(separate, builtin(opts, overlapping)), [])

    def test_named_string_option_can_prompt_for_a_missing_value(self):
        """A literal sensitive flag allows interactive input when a named string option has no value."""
        old = builtin("@Argument String password;")
        new = builtin("@Argument(sensitive=true) char[] password;")
        self.assertEqual(compare(old, new), [
            f"{DTO}\tCLI option --cache list --password now prompts for a missing value on an interactive console",
        ])
        self.assertEqual(compare(new, old), [
            f"{DTO}\tCLI option --cache list --password now requires a value on the command line",
        ])
        self.assertEqual(compare(old, builtin("@Argument(sensitive=SECRET) String password;")), [])
        self.assertEqual(compare(builtin("@Argument @Positional String password;"),
                                 builtin("@Argument(sensitive=true) @Positional String password;")), [])
