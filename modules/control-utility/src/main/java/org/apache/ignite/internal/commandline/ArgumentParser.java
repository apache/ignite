/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.commandline;

import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.commandline.argument.parser.CLIArgument;
import org.apache.ignite.internal.commandline.argument.parser.CLIArgumentParser;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.IgniteCommandRegistry;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.ArgumentGroup;
import org.apache.ignite.internal.management.api.CliSubcommandsWithPrefix;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.management.api.CommandsRegistry;
import org.apache.ignite.internal.management.api.Positional;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteExperimental;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_EXPERIMENTAL_COMMAND;
import static org.apache.ignite.internal.commandline.CommandHandler.DFLT_HOST;
import static org.apache.ignite.internal.commandline.CommandHandler.DFLT_PORT;
import static org.apache.ignite.internal.commandline.CommandHandler.UTILITY_NAME;
import static org.apache.ignite.internal.commandline.argument.parser.CLIArgument.CLIArgumentBuilder.argument;
import static org.apache.ignite.internal.commandline.argument.parser.CLIArgument.CLIArgumentBuilder.optionalArgument;
import static org.apache.ignite.internal.commandline.argument.parser.CLIArgumentParser.readNextValueToken;
import static org.apache.ignite.internal.management.api.CommandUtils.CMD_WORDS_DELIM;
import static org.apache.ignite.internal.management.api.CommandUtils.NAME_PREFIX;
import static org.apache.ignite.internal.management.api.CommandUtils.PARAM_WORDS_DELIM;
import static org.apache.ignite.internal.management.api.CommandUtils.argument;
import static org.apache.ignite.internal.management.api.CommandUtils.asOptional;
import static org.apache.ignite.internal.management.api.CommandUtils.executable;
import static org.apache.ignite.internal.management.api.CommandUtils.fromFormattedCommandName;
import static org.apache.ignite.internal.management.api.CommandUtils.isBoolean;
import static org.apache.ignite.internal.management.api.CommandUtils.toFormattedCommandName;
import static org.apache.ignite.internal.management.api.CommandUtils.toFormattedFieldName;
import static org.apache.ignite.internal.management.api.CommandUtils.visitCommandParams;
import static org.apache.ignite.ssl.SslContextFactory.DFLT_KEY_ALGORITHM;
import static org.apache.ignite.ssl.SslContextFactory.DFLT_SSL_PROTOCOL;
import static org.apache.ignite.ssl.SslContextFactory.DFLT_STORE_TYPE;

/**
 * Argument parser.
 * Also would parse high-level command and delegate parsing for its argument to the command.
 */
public class ArgumentParser {
    /** */
    private final IgniteLogger log;

    /** */
    private final IgniteCommandRegistry registry;

    /** Path to the specific command. Command to execute is on top of stack, parent commands below. */
    Deque<Command<?, ?>> cmdPath = new ArrayDeque<>();

    /** */
    static final String CMD_HOST = "--host";

    /** */
    static final String CMD_PORT = "--port";

    /** */
    static final String CMD_PASSWORD = "--password";

    /** */
    static final String CMD_USER = "--user";

    /** Option is used for auto confirmation. */
    public static final String CMD_AUTO_CONFIRMATION = "--yes";

    /** Verbose mode. */
    public static final String CMD_VERBOSE = "--verbose";

    // SSL configuration section

    /** */
    static final String CMD_SSL_PROTOCOL = "--ssl-protocol";

    /** */
    static final String CMD_SSL_KEY_ALGORITHM = "--ssl-key-algorithm";

    /** */
    static final String CMD_SSL_CIPHER_SUITES = "--ssl-cipher-suites";

    /** */
    static final String CMD_KEYSTORE = "--keystore";

    /** */
    static final String CMD_KEYSTORE_PASSWORD = "--keystore-password";

    /** */
    static final String CMD_KEYSTORE_TYPE = "--keystore-type";

    /** */
    static final String CMD_TRUSTSTORE = "--truststore";

    /** */
    static final String CMD_TRUSTSTORE_PASSWORD = "--truststore-password";

    /** */
    static final String CMD_TRUSTSTORE_TYPE = "--truststore-type";

    /** */
    static final String CMD_ENABLE_EXPERIMENTAL = "--enable-experimental";

    /** */
    static final String CMD_SSL_FACTORY = "--ssl-factory";

    /** */
    private static final BiConsumer<String, Integer> PORT_VALIDATOR = (name, val) -> {
        if (val <= 0 || val > 65535)
            throw new IllegalArgumentException("Invalid value for " + name + ": " + val);
    };

    /** */
    private final List<CLIArgument<?>> common = new ArrayList<>();

    /** Console instance */
    protected final GridConsole console;

    /**
     * @param log Logger.
     * @param registry Supported commands.
     * @param console Supported commands.
     */
    public ArgumentParser(IgniteLogger log, IgniteCommandRegistry registry, GridConsole console) {
        this.log = log;
        this.registry = registry;
        this.console = console;

        common.addAll(List.of(
            optionalArgument(CMD_HOST, String.class).withUsage("HOST_OR_IP").withDefault(DFLT_HOST).build(),
            optionalArgument(CMD_PORT, Integer.class).withUsage("PORT").withDefault(DFLT_PORT).withValidator(PORT_VALIDATOR).build(),
            optionalArgument(CMD_USER, String.class).withUsage("USER").build(),
            optionalArgument(CMD_PASSWORD, String.class).withUsage("PASSWORD").markSensitive().build(),
            optionalArgument(CMD_VERBOSE, boolean.class).withUsage(CMD_VERBOSE).withDefault(false).build(),
            optionalArgument(CMD_SSL_PROTOCOL, String[].class)
                .withUsage("SSL_PROTOCOL[, SSL_PROTOCOL_2, ..., SSL_PROTOCOL_N]")
                .withDefault(t -> new String[] {DFLT_SSL_PROTOCOL})
                .build(),
            optionalArgument(CMD_SSL_CIPHER_SUITES, String[].class).withUsage("SSL_CIPHER_1[, SSL_CIPHER_2, ..., SSL_CIPHER_N]").build(),
            optionalArgument(CMD_SSL_KEY_ALGORITHM, String.class).withUsage("SSL_KEY_ALGORITHM").withDefault(DFLT_KEY_ALGORITHM).build(),
            optionalArgument(CMD_SSL_FACTORY, String.class).withUsage("SSL_FACTORY_PATH").build(),
            optionalArgument(CMD_KEYSTORE_TYPE, String.class).withUsage("KEYSTORE_TYPE").withDefault(DFLT_STORE_TYPE).build(),
            optionalArgument(CMD_KEYSTORE, String.class).withUsage("KEYSTORE_PATH").build(),
            optionalArgument(CMD_KEYSTORE_PASSWORD, char[].class).withUsage("KEYSTORE_PASSWORD").markSensitive().build(),
            optionalArgument(CMD_TRUSTSTORE_TYPE, String.class).withUsage("TRUSTSTORE_TYPE").withDefault(DFLT_STORE_TYPE).build(),
            optionalArgument(CMD_TRUSTSTORE, String.class).withUsage("TRUSTSTORE_PATH").build(),
            optionalArgument(CMD_TRUSTSTORE_PASSWORD, char[].class).withUsage("TRUSTSTORE_PASSWORD").markSensitive().build(),
            optionalArgument(CMD_AUTO_CONFIRMATION, boolean.class).withUsage(CMD_AUTO_CONFIRMATION).withDefault(false).build(),
            optionalArgument(CMD_ENABLE_EXPERIMENTAL, Boolean.class)
                .withUsage(CMD_ENABLE_EXPERIMENTAL)
                .withDefault(t -> IgniteSystemProperties.getBoolean(IGNITE_ENABLE_EXPERIMENTAL_COMMAND))
                .build())
        );
    }

    /**
     * Creates list of common utility options.
     *
     * @return Array of common utility options.
     */
    public String[] getCommonOptions() {
        List<String> list = new ArrayList<>();

        for (CLIArgument<?> arg : common) {
            if (arg.name().equals(CMD_AUTO_CONFIRMATION))
                continue;

            if (isBoolean(arg.type()))
                list.add(asOptional(arg.name(), true));
            else
                list.add(asOptional(arg.name() + " " + arg.usage(), true));
        }

        return list.toArray(U.EMPTY_STRS);
    }

    /**
     * Parses and validates arguments.
     *
     * @param raw Raw arguments.
     * @return Arguments bean.
     * @throws IllegalArgumentException In case arguments aren't valid.
     */
    public <A extends IgniteDataTransferObject> ConnectionAndSslParameters<A> parseAndValidate(List<String> raw) {
        List<String> args = new ArrayList<>(raw);

        findCommand(args.iterator());

        CLIArgumentParser parser = createArgumentParser();

        parser.parse(args.listIterator());

        String argsToStr = convertCommandToString(raw.listIterator(), parser);

        A arg = (A)argument(
            cmdPath.peek().argClass(),
            (fld, pos) -> parser.get(pos),
            fld -> parser.get(toFormattedFieldName(fld).toLowerCase())
        );

        if (!parser.<Boolean>get(CMD_ENABLE_EXPERIMENTAL) && cmdPath.peek().getClass().isAnnotationPresent(IgniteExperimental.class)) {
            log.warning(
                String.format("To use experimental command add " + CMD_ENABLE_EXPERIMENTAL + " parameter for %s",
                    UTILITY_NAME)
            );

            throw new IllegalArgumentException("Experimental commands disabled");
        }

        return new ConnectionAndSslParameters<>(cmdPath, arg, parser, argsToStr);
    }

    /**
     * Searches command from hierarchical root.
     *
     * @param iter Iterator of CLI arguments.
     */
    protected void findCommand(Iterator<String> iter) {
        assert cmdPath.isEmpty();

        while (iter.hasNext() && cmdPath.isEmpty()) {
            String cmdName = iter.next();

            if (!cmdName.startsWith(NAME_PREFIX))
                continue;

            Command<?, ?> cmd = registry.command(fromFormattedCommandName(cmdName.substring(NAME_PREFIX.length()), CMD_WORDS_DELIM));

            if (cmd == null)
                continue;

            cmdPath.push(cmd);
        }

        if (cmdPath.isEmpty())
            throw new IllegalArgumentException("No action was specified");

        // Remove command name parameter to exclude it from ongoing parsing.
        iter.remove();

        while (cmdPath.peek() instanceof CommandsRegistry && iter.hasNext()) {
            String name = iter.next();

            char delim = PARAM_WORDS_DELIM;

            if (cmdPath.peek().getClass().isAnnotationPresent(CliSubcommandsWithPrefix.class)) {
                if (!name.startsWith(NAME_PREFIX))
                    break;

                name = name.substring(NAME_PREFIX.length());

                delim = CMD_WORDS_DELIM;
            }

            Command<?, ?> cmd1 = ((CommandsRegistry<?, ?>)cmdPath.peek()).command(fromFormattedCommandName(name, delim));

            if (cmd1 == null)
                break;

            cmdPath.push(cmd1);

            // Remove command name parameter to exclude it from ongoing parsing.
            iter.remove();
        }

        if (!executable(cmdPath.peek())) {
            throw new IllegalArgumentException(
                "Command " + toFormattedCommandName(cmdPath.peek().getClass()) + " can't be executed"
            );
        }
    }

    /** */
    private CLIArgumentParser createArgumentParser() {
        assert !cmdPath.isEmpty();

        List<CLIArgument<?>> positionalArgs = new ArrayList<>();
        List<CLIArgument<?>> namedArgs = new ArrayList<>();

        BiFunction<Field, Boolean, CLIArgument<?>> toArg =
            (fld, optional) -> argument(toFormattedFieldName(fld).toLowerCase(), fld.getType())
                .withOptional(optional)
                .withSensitive(fld.getAnnotation(Argument.class).sensitive())
                .build();

        List<Set<String>> grpdFlds = CommandUtils.argumentGroupsValues(cmdPath.peek().argClass());

        Consumer<Field> namedArgCb = fld -> namedArgs.add(
            toArg.apply(fld, CommandUtils.argumentGroupIdx(grpdFlds, fld.getName()) >= 0
                || fld.getAnnotation(Argument.class).optional())
        );

        Consumer<Field> positionalArgCb = fld -> positionalArgs.add(argument(fld.getName().toLowerCase(), fld.getType())
            .withOptional(fld.getAnnotation(Argument.class).optional())
            .build()
        );

        BiConsumer<ArgumentGroup, List<Field>> argGrpCb = (argGrp0, flds) -> flds.forEach(fld -> {
            if (fld.isAnnotationPresent(Positional.class))
                positionalArgCb.accept(fld);
            else
                namedArgCb.accept(fld);
        });

        visitCommandParams(cmdPath.peek().argClass(), positionalArgCb, namedArgCb, argGrpCb);

        namedArgs.addAll(common);

        return new CLIArgumentParser(positionalArgs, namedArgs, console);
    }

    /**
     * Create string of command arguments for logging arguments with hidden confidential values
     * @param rawIter raw command arguments iterator
     * @param parser CLIArgumentParser
     *
     * @return string of command arguments for logging with hidden confidential values
     */
    private String convertCommandToString(ListIterator<String> rawIter, CLIArgumentParser parser) {
        SB cmdToStr = new SB();

        while (rawIter.hasNext()) {
            String arg = rawIter.next();

            CLIArgument<?> cliArg = parser.getCliArg(arg.toLowerCase());

            cmdToStr.a(arg).a(' ');

            if (cliArg == null || cliArg.isFlag())
                continue;

            String argVal = readNextValueToken(rawIter);

            if (argVal != null) {
                if (cliArg.isSensitive()) {
                    cmdToStr.a("***** ");
                    log.info(String.format("Warning: %s is insecure. Whenever possible, use interactive " +
                            "prompt for password (just omit the argument value).", cliArg.name()));
                }
                else
                    cmdToStr.a(argVal).a(' ');
            }
        }

        return cmdToStr.toString();
    }
}
