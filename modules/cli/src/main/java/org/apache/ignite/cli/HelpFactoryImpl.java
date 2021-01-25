/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cli;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.cli.spec.SpecAdapter;
import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Help.ColorScheme;
import picocli.CommandLine.Model.OptionSpec;
import picocli.CommandLine.Model.PositionalParamSpec;

/**
 * Implementation of Picocli factory for help message formatting.
 */
public class HelpFactoryImpl implements CommandLine.IHelpFactory {
    /** Section key banner. */
    public static final String SECTION_KEY_BANNER = "banner";

    /** Section key parameter option table. */
    public static final String SECTION_KEY_PARAMETER_OPTION_TABLE = "paramsOptsTable";

    /** {@inheritDoc} */
    @Override public CommandLine.Help create(CommandLine.Model.CommandSpec cmdSpec, ColorScheme cs) {
        boolean hasCommands = !cmdSpec.subcommands().isEmpty();
        boolean hasOptions = cmdSpec.options().stream().anyMatch(o -> !o.hidden());
        boolean hasParameters = cmdSpec.positionalParameters().stream().anyMatch(o -> !o.hidden());

        // Any command can have either subcommands or options/parameters, but not both.
        assert !(hasCommands && (hasOptions || hasParameters));

        cmdSpec.usageMessage().sectionKeys(Arrays.asList(
            SECTION_KEY_BANNER,
            CommandLine.Model.UsageMessageSpec.SECTION_KEY_SYNOPSIS,
            CommandLine.Model.UsageMessageSpec.SECTION_KEY_DESCRIPTION,
            CommandLine.Model.UsageMessageSpec.SECTION_KEY_COMMAND_LIST,
            SECTION_KEY_PARAMETER_OPTION_TABLE
        ));

        var sectionMap = new HashMap<String, CommandLine.IHelpSectionRenderer>();

        if (cmdSpec.commandLine().isUsageHelpRequested()) {
            sectionMap.put(SECTION_KEY_BANNER,
                help -> {
                    assert help.commandSpec().commandLine().getCommand() instanceof SpecAdapter;

                    return ((SpecAdapter)help.commandSpec().commandLine().getCommand()).banner();
                }
            );
        }

        if (!hasCommands) {
            sectionMap.put(CommandLine.Model.UsageMessageSpec.SECTION_KEY_SYNOPSIS,
                help -> {
                    StringBuilder sb = new StringBuilder();

                    List<Ansi.IStyle> boldCmdStyle = new ArrayList<>(cs.commandStyles());

                    boldCmdStyle.add(Ansi.Style.bold);

                    sb.append(cs.apply(help.commandSpec().qualifiedName(), boldCmdStyle));

                    if (hasOptions)
                        sb.append(cs.optionText(" [OPTIONS]"));

                    if (hasParameters) {
                        for (PositionalParamSpec parameter : cmdSpec.positionalParameters())
                            sb.append(' ').append(cs.parameterText(parameter.paramLabel()));
                    }

                    sb.append("\n\n");

                    return sb.toString();
                }
            );
        }

        sectionMap.put(CommandLine.Model.UsageMessageSpec.SECTION_KEY_DESCRIPTION,
            help -> Ansi.AUTO.string(help.description() + '\n'));

        if (hasCommands) {
            sectionMap.put(CommandLine.Model.UsageMessageSpec.SECTION_KEY_COMMAND_LIST, help -> {
                Table tbl = new Table(0, cs);

                tbl.addSection("@|bold COMMANDS|@");

                for (Map.Entry<String, CommandLine.Help> entry : help.subcommands().entrySet()) {
                    String name = entry.getKey();

                    CommandLine.Help cmd = entry.getValue();

                    if (cmd.subcommands().isEmpty())
                        tbl.addRow(cs.commandText(name), cmd.description().trim());
                    else {
                        for (Map.Entry<String, CommandLine.Help> subEntry : cmd.subcommands().entrySet()) {
                            String subName = subEntry.getKey();

                            CommandLine.Help subCmd = subEntry.getValue();

                            // Further hierarchy is prohibited.
                            assert subCmd.subcommands().isEmpty();

                            tbl.addRow(cs.commandText(name + " " + subName), subCmd.description().trim());
                        }
                    }
                }

                return tbl.toString() + "\n";
            });
        }
        else if (hasParameters || hasOptions) {
            sectionMap.put(SECTION_KEY_PARAMETER_OPTION_TABLE, help -> {
                Table tbl = new Table(0, cs);

                if (hasParameters) {
                    tbl.addSection("@|bold REQUIRED PARAMETERS|@");

                    for (PositionalParamSpec param : help.commandSpec().positionalParameters()) {
                        if (!param.hidden()) {
                            // TODO: IGNITE-14022 Support multiple-line descriptions.
                            assert param.description().length == 1;

                            tbl.addRow(cs.parameterText(param.paramLabel()), param.description()[0]);
                        }
                    }
                }

                if (hasOptions) {
                    tbl.addSection("@|bold OPTIONS|@");

                    for (OptionSpec option : help.commandSpec().options()) {
                        if (!option.hidden()) {
                            // TODO: IGNITE-14022 Support multiple names.
                            assert option.names().length == 1;
                            // TODO: IGNITE-14022 Support multiple-line descriptions.
                            assert option.description().length == 1;

                            tbl.addRow(
                                cs.optionText(option.names()[0] + '=' + option.paramLabel()),
                                option.description()[0]);
                        }
                    }
                }

                return tbl.toString() + "\n";
            });
        }

        cmdSpec.usageMessage().sectionMap(sectionMap);

        return new CommandLine.Help(cmdSpec, cs);
    }
}
