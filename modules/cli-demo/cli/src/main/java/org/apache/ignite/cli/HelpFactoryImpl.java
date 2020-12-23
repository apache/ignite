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

public class HelpFactoryImpl implements CommandLine.IHelpFactory {
    public static final String SECTION_KEY_BANNER = "banner";
    public static final String SECTION_KEY_PARAMETER_OPTION_TABLE = "paramsOptsTable";

    @Override public CommandLine.Help create(CommandLine.Model.CommandSpec commandSpec, ColorScheme cs) {
        boolean hasCommands = !commandSpec.subcommands().isEmpty();
        boolean hasOptions = commandSpec.options().stream().anyMatch(o -> !o.hidden());
        boolean hasParameters = commandSpec.positionalParameters().stream().anyMatch(o -> !o.hidden());

        // Any command can have either subcommands or options/parameters, but not both.
        assert !(hasCommands && (hasOptions || hasParameters));

        commandSpec.usageMessage().sectionKeys(Arrays.asList(
            SECTION_KEY_BANNER,
            CommandLine.Model.UsageMessageSpec.SECTION_KEY_SYNOPSIS,
            CommandLine.Model.UsageMessageSpec.SECTION_KEY_DESCRIPTION,
            CommandLine.Model.UsageMessageSpec.SECTION_KEY_COMMAND_LIST,
            SECTION_KEY_PARAMETER_OPTION_TABLE
        ));

        var sectionMap = new HashMap<String, CommandLine.IHelpSectionRenderer>();

        if (commandSpec.commandLine().isUsageHelpRequested()) {
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
                        for (PositionalParamSpec parameter : commandSpec.positionalParameters())
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
                Table table = new Table(0, cs);

                table.addSection("@|bold COMMANDS|@");

                for (Map.Entry<String, CommandLine.Help> entry : help.subcommands().entrySet()) {
                    String name = entry.getKey();
                    CommandLine.Help cmd = entry.getValue();

                    if (cmd.subcommands().isEmpty()) {
                        table.addRow(cs.commandText(name), cmd.description().trim());
                    }
                    else {
                        for (Map.Entry<String, CommandLine.Help> subEntry : cmd.subcommands().entrySet()) {
                            String subName = subEntry.getKey();
                            CommandLine.Help subCmd = subEntry.getValue();

                            // Further hierarchy is prohibited.
                            assert subCmd.subcommands().isEmpty();

                            table.addRow(cs.commandText(name + " " + subName), subCmd.description().trim());
                        }
                    }
                }

                return table.toString() + "\n";
            });
        }
        else if (hasParameters || hasOptions) {
            sectionMap.put(SECTION_KEY_PARAMETER_OPTION_TABLE, help -> {
                Table table = new Table(0, cs);

                if (hasParameters) {
                    table.addSection("@|bold REQUIRED PARAMETERS|@");

                    for (PositionalParamSpec param : help.commandSpec().positionalParameters()) {
                        if (!param.hidden()) {
                            // TODO: Support multiple-line descriptions.
                            assert param.description().length == 1;

                            table.addRow(cs.parameterText(param.paramLabel()), param.description()[0]);
                        }
                    }
                }

                if (hasOptions) {
                    table.addSection("@|bold OPTIONS|@");

                    for (OptionSpec option : help.commandSpec().options()) {
                        if (!option.hidden()) {
                            // TODO: Support multiple names.
                            assert option.names().length == 1;
                            // TODO: Support multiple-line descriptions.
                            assert option.description().length == 1;

                            table.addRow(
                                cs.optionText(option.names()[0] + '=' + option.paramLabel()),
                                option.description()[0]);
                        }
                    }
                }

                return table.toString() + "\n";
            });
        }

        commandSpec.usageMessage().sectionMap(sectionMap);

        return new CommandLine.Help(commandSpec, cs);
    }
}
