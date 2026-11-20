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


CLI_DIR = "modules/control-utility/src/main/java/org/apache/ignite/internal/commandline/"
MANAGEMENT = "modules/core/src/main/java/org/apache/ignite/internal/management/"
REGISTRY = MANAGEMENT + "IgniteCommandRegistry.java"
PARSER = CLI_DIR + "ArgumentParser.java"
HANDLER = CLI_DIR + "CommandHandler.java"


class CliContractsTest(unittest.TestCase):
    def test_registered_commands_and_common_options(self):
        registry = """package org.apache.ignite.internal.management;
            class IgniteCommandRegistry extends CommandRegistryImpl {
                IgniteCommandRegistry() { super(new StateCommand(), new SystemViewCommand()); }
            }"""
        parser = """package org.apache.ignite.internal.commandline;
            class ArgumentParser {
                static final String PORT = "--port";
                static final String UNUSED = "--unused";
                ArgumentParser() {
                    common.addAll(List.of(optionalArgument(PORT, Integer.class).withUsage("PORT").build(),
                        optionalArgument("--yes", boolean.class).build()));
                }
            }"""
        before = {REGISTRY: registry, PARSER: parser}
        after = {REGISTRY: registry.replace("new SystemViewCommand()", "new SetStateCommand()"),
                 PARSER: parser.replace('"--port"', '"--tcp-port"')}
        self.assertCountEqual(compare(before, after), [
            f"{REGISTRY}\tRemoved CLI command --system-view", f"{REGISTRY}\tAdded CLI command --set-state",
            f"{PARSER}\tRemoved CLI option --port", f"{PARSER}\tAdded CLI option --tcp-port",
        ])

        same_names = {
            REGISTRY: registry.replace("new StateCommand(), new SystemViewCommand()",
                                       "new commands.SystemViewCommand(), new StateCommand()"),
            PARSER: parser.replace("PORT", "CONNECTION_PORT").replace("--unused", "--other")
                          .replace("Integer.class", "int.class"),
        }
        self.assertEqual(compare(before, same_names), [])

        self.assertEqual(compare({PARSER: parser}, {PARSER: parser.replace("Integer.class", "Long.class")}),
                         [f"{PARSER}\tChanged CLI option --port type from int to long"])
        self.assertEqual(compare({PARSER: parser}, {PARSER: parser.replace("Integer.class", "portType()")}), [])

        delegated = {REGISTRY: registry.replace("new SystemViewCommand()", "systemView()"),
                     PARSER: parser.replace('"--port"', 'Prefix.CLI + "port"')}
        self.assertEqual(compare(before, delegated), [])
        moved = {REGISTRY: registry.replace(", new SystemViewCommand()", "")
                                  .replace("StateCommand());", "StateCommand()); registerSystemView();")}
        self.assertEqual(compare({REGISTRY: registry}, moved), [])

    def test_exit_codes_returned_by_the_control_utility(self):
        src = """package org.apache.ignite.internal.commandline;
            class CommandHandler {
                static final int EXIT_CODE_INVALID_ARGUMENTS = 1;
                static final int UNUSED = 2;
                int execute(List<String> args) {
                    IntSupplier helper = () -> { return UNUSED; };
                    return EXIT_CODE_INVALID_ARGUMENTS;
                }
            }"""
        edited = src.replace("UNUSED = 2", "UNUSED = 4").replace("ARGUMENTS = 1", "ARGUMENTS = 6")
        self.assertEqual(compare({HANDLER: src}, {HANDLER: edited}),
                         [f"{HANDLER}\tChanged CLI exit code for EXIT_CODE_INVALID_ARGUMENTS from 1 to 6"])
        for status in (src.replace("EXIT_CODE_INVALID_ARGUMENTS", "INVALID"),
                       src.replace("ARGUMENTS = 1", "ARGUMENTS = Defaults.INVALID")):
            with self.subTest(status=status):
                self.assertEqual(compare({HANDLER: src}, {HANDLER: status}), [])

        # Swapping two field names and their uses preserves both return values.
        both = src.replace("return EXIT_CODE_INVALID_ARGUMENTS;",
                           "if (args.isEmpty()) return UNUSED; return EXIT_CODE_INVALID_ARGUMENTS;")
        swapped = both.replace("UNUSED", "TMP").replace("EXIT_CODE_INVALID_ARGUMENTS", "UNUSED")
        self.assertEqual(compare({HANDLER: both}, {HANDLER: swapped.replace("TMP", "EXIT_CODE_INVALID_ARGUMENTS")}), [])
