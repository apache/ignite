package org.apache.ignite.cli;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import picocli.CommandLine;

public class HelpFactoryImpl implements CommandLine.IHelpFactory {

    public static final String SECTION_KEY_BANNER = "banner";
    public static final String SECTION_KEY_SYNOPSIS_EXTENSION = "synopsisExt";

    private static final String[] BANNER = new String[] {
        "                       ___                         __",
        "                      /   |   ____   ____ _ _____ / /_   ___",
        "  @|red,bold       ⣠⣶⣿|@          / /| |  / __ \\ / __ `// ___// __ \\ / _ \\",
        "  @|red,bold      ⣿⣿⣿⣿|@         / ___ | / /_/ // /_/ // /__ / / / //  __/",
        "  @|red,bold  ⢠⣿⡏⠈⣿⣿⣿⣿⣷|@       /_/  |_|/ .___/ \\__,_/ \\___//_/ /_/ \\___/",
        "  @|red,bold ⢰⣿⣿⣿⣧⠈⢿⣿⣿⣿⣿⣦|@            /_/",
        "  @|red,bold ⠘⣿⣿⣿⣿⣿⣦⠈⠛⢿⣿⣿⣿⡄|@       ____               _  __           _____",
        "  @|red,bold  ⠈⠛⣿⣿⣿⣿⣿⣿⣦⠉⢿⣿⡟|@      /  _/____ _ ____   (_)/ /_ ___     |__  /",
        "  @|red,bold ⢰⣿⣶⣀⠈⠙⠿⣿⣿⣿⣿ ⠟⠁|@      / / / __ `// __ \\ / // __// _ \\     /_ <",
        "  @|red,bold ⠈⠻⣿⣿⣿⣿⣷⣤⠙⢿⡟|@       _/ / / /_/ // / / // // /_ /  __/   ___/ /",
        "  @|red,bold       ⠉⠉⠛⠏⠉|@      /___/ \\__, //_/ /_//_/ \\__/ \\___/   /____/",
        "                        /____/\n"};

    @Override public CommandLine.Help create(CommandLine.Model.CommandSpec commandSpec,
        CommandLine.Help.ColorScheme colorScheme) {
        commandSpec.usageMessage().sectionKeys(Arrays.asList(
            SECTION_KEY_BANNER,
            CommandLine.Model.UsageMessageSpec.SECTION_KEY_HEADER,
            CommandLine.Model.UsageMessageSpec.SECTION_KEY_DESCRIPTION,
            CommandLine.Model.UsageMessageSpec.SECTION_KEY_SYNOPSIS_HEADING,
            CommandLine.Model.UsageMessageSpec.SECTION_KEY_SYNOPSIS,
            SECTION_KEY_SYNOPSIS_EXTENSION,
            CommandLine.Model.UsageMessageSpec.SECTION_KEY_PARAMETER_LIST_HEADING,
            CommandLine.Model.UsageMessageSpec.SECTION_KEY_PARAMETER_LIST,
            CommandLine.Model.UsageMessageSpec.SECTION_KEY_OPTION_LIST_HEADING,
            CommandLine.Model.UsageMessageSpec.SECTION_KEY_OPTION_LIST,
            CommandLine.Model.UsageMessageSpec.SECTION_KEY_COMMAND_LIST_HEADING,
            CommandLine.Model.UsageMessageSpec.SECTION_KEY_COMMAND_LIST));

        var sectionMap = new HashMap<String, CommandLine.IHelpSectionRenderer>();

        boolean hasCommands = !commandSpec.subcommands().isEmpty();
        boolean hasOptions = commandSpec.options().stream().anyMatch(o -> !o.hidden());
        boolean hasParameters = !commandSpec.positionalParameters().isEmpty();

        sectionMap.put(SECTION_KEY_BANNER,
            help -> Arrays
                .stream(BANNER)
                .map(CommandLine.Help.Ansi.AUTO::string)
                .collect(Collectors.joining("\n")) +
                "\n"
        );

        sectionMap.put(CommandLine.Model.UsageMessageSpec.SECTION_KEY_HEADER,
            help -> CommandLine.Help.Ansi.AUTO.string(
                Arrays.stream(help.commandSpec().version())
                    .collect(Collectors.joining("\n")) + "\n\n")
        );

        sectionMap.put(CommandLine.Model.UsageMessageSpec.SECTION_KEY_DESCRIPTION,
            help -> CommandLine.Help.Ansi.AUTO.string("@|bold,green " + help.commandSpec().qualifiedName() +
                "|@\n  " + help.description() + "\n"));

        sectionMap.put(CommandLine.Model.UsageMessageSpec.SECTION_KEY_SYNOPSIS_HEADING,
            help -> CommandLine.Help.Ansi.AUTO.string("@|bold USAGE|@\n"));

        sectionMap.put(CommandLine.Model.UsageMessageSpec.SECTION_KEY_SYNOPSIS,
            help -> {
                StringBuilder sb = new StringBuilder();

                sb.append("  ");
                sb.append(help.colorScheme().commandText(help.commandSpec().qualifiedName()));

                if (hasCommands) {
                    sb.append(help.colorScheme().commandText(" <COMMAND>"));
                }
                else {
                    if (hasOptions)
                        sb.append(help.colorScheme().optionText(" [OPTIONS]"));

                    if (hasParameters) {
                        for (CommandLine.Model.PositionalParamSpec parameter : commandSpec.positionalParameters())
                            sb.append(' ').append(help.colorScheme().parameterText(parameter.paramLabel()));
                    }
                }

                sb.append("\n\n");

                return sb.toString();
            });


        Optional.ofNullable(commandSpec.usageMessage().sectionMap().get(SECTION_KEY_SYNOPSIS_EXTENSION))
            .ifPresent(v -> sectionMap.put(SECTION_KEY_SYNOPSIS_EXTENSION, v));

        sectionMap.put(CommandLine.Model.UsageMessageSpec.SECTION_KEY_PARAMETER_LIST_HEADING,
            help -> hasParameters ? CommandLine.Help.Ansi.AUTO.string("@|bold REQUIRED PARAMETERS|@\n") : "");

        sectionMap.put(CommandLine.Model.UsageMessageSpec.SECTION_KEY_PARAMETER_LIST, new TableRenderer<>(
            h -> h.commandSpec().positionalParameters(),
            p -> p.paramLabel().length(),
            (h, p) -> h.colorScheme().parameterText(p.paramLabel()),
            (h, p) -> h.colorScheme().text(p.description()[0])));

        sectionMap.put(CommandLine.Model.UsageMessageSpec.SECTION_KEY_OPTION_LIST_HEADING,
            help -> hasOptions ? CommandLine.Help.Ansi.AUTO.string("@|bold OPTIONS|@\n") : "");

        if (hasOptions)
            sectionMap.put(CommandLine.Model.UsageMessageSpec.SECTION_KEY_OPTION_LIST, new TableRenderer<>(
                h -> h.commandSpec().options(),
                o -> o.shortestName().length() + o.paramLabel().length() + 1,
                (h, o) -> h.colorScheme().optionText(o.shortestName()).concat("=").concat(o.paramLabel()),
                (h, o) -> h.colorScheme().text(o.description()[0])));

        sectionMap.put(CommandLine.Model.UsageMessageSpec.SECTION_KEY_COMMAND_LIST_HEADING,
            help -> hasCommands ? CommandLine.Help.Ansi.AUTO.string("@|bold COMMANDS|@\n") : "");

        sectionMap.put(CommandLine.Model.UsageMessageSpec.SECTION_KEY_COMMAND_LIST, new TableRenderer<>(
            h -> h.subcommands().values(),
            c -> c.commandSpec().name().length(),
            (h, c) -> h.colorScheme().commandText(c.commandSpec().name()),
            (h, c) -> h.colorScheme().text(c.description().stripTrailing())));
        commandSpec.usageMessage().sectionMap(sectionMap);
        return new CommandLine.Help(commandSpec, colorScheme);
    }

    private static class TableRenderer<T> implements CommandLine.IHelpSectionRenderer {
        private final Function<CommandLine.Help, Collection<T>> itemsFunc;
        private final ToIntFunction<T> nameLenFunc;
        private final BiFunction<CommandLine.Help, T, CommandLine.Help.Ansi.Text> nameFunc;
        private final BiFunction<CommandLine.Help, T, CommandLine.Help.Ansi.Text> descriptionFunc;

        TableRenderer(Function<CommandLine.Help, Collection<T>> itemsFunc, ToIntFunction<T> nameLenFunc,
            BiFunction<CommandLine.Help, T, CommandLine.Help.Ansi.Text> nameFunc, BiFunction<CommandLine.Help, T, CommandLine.Help.Ansi.Text> descriptionFunc) {
            this.itemsFunc = itemsFunc;
            this.nameLenFunc = nameLenFunc;
            this.nameFunc = nameFunc;
            this.descriptionFunc = descriptionFunc;
        }

        @Override public String render(CommandLine.Help help) {
            Collection<T> items = itemsFunc.apply(help);

            if (items.isEmpty())
                return "";

            int len = 2 + items.stream().mapToInt(nameLenFunc).max().getAsInt();

            CommandLine.Help.TextTable table = CommandLine.Help.TextTable.forColumns(help.colorScheme(),
                new CommandLine.Help.Column(len, 2, CommandLine.Help.Column.Overflow.SPAN),
                new CommandLine.Help.Column(160 - len, 4, CommandLine.Help.Column.Overflow.WRAP));

            for (T item : items)
                table.addRowValues(nameFunc.apply(help, item), descriptionFunc.apply(help, item));

            return table.toString() + '\n';
        }
    }
}
