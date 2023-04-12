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

package org.apache.ignite.internal.commands;

import java.lang.reflect.Field;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.commandline.GridConsole;
import org.apache.ignite.internal.commandline.argument.parser.CLIArgument;
import org.apache.ignite.internal.commandline.argument.parser.CLIArgumentParser;
import org.apache.ignite.internal.management.CommandsRegistry;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.CliPositionalSubcommands;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.management.api.CommandWithSubs;
import org.apache.ignite.internal.management.api.EnumDescription;
import org.apache.ignite.internal.management.api.PositionalArgument;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_EXPERIMENTAL_COMMAND;
import static org.apache.ignite.internal.IgniteVersionUtils.ACK_VER_STR;
import static org.apache.ignite.internal.IgniteVersionUtils.COPYRIGHT;
import static org.apache.ignite.internal.commandline.Command.EXPERIMENTAL_LABEL;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.TIME_PREFIX;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_ENABLE_EXPERIMENTAL;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_HOST;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_KEYSTORE;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_KEYSTORE_PASSWORD;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_KEYSTORE_TYPE;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_PASSWORD;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_PING_INTERVAL;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_PING_TIMEOUT;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_PORT;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_SSL_CIPHER_SUITES;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_SSL_KEY_ALGORITHM;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_SSL_PROTOCOL;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_TRUSTSTORE;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_TRUSTSTORE_PASSWORD;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_TRUSTSTORE_TYPE;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_USER;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_VERBOSE;
import static org.apache.ignite.internal.commandline.argument.parser.CLIArgument.optionalArg;
import static org.apache.ignite.internal.commands.CommandUtils.CMD_WORDS_DELIM;
import static org.apache.ignite.internal.commands.CommandUtils.PARAMETER_PREFIX;
import static org.apache.ignite.internal.commands.CommandUtils.PARAM_WORDS_DELIM;
import static org.apache.ignite.internal.commands.CommandUtils.commandName;
import static org.apache.ignite.internal.commands.CommandUtils.hasDescribedParameters;
import static org.apache.ignite.internal.commands.CommandUtils.isArgumentName;
import static org.apache.ignite.internal.commands.CommandUtils.parameterExample;
import static org.apache.ignite.internal.commands.CommandUtils.parameterName;
import static org.apache.ignite.internal.commands.CommandUtils.valueExample;
import static org.apache.ignite.internal.commands.CommandUtils.visitCommandParams;

/**
 *
 */
public class CLICommandFrontendImpl implements CLICommandFrontend {
    /** Indent for help output. */
    public static final String INDENT = "  ";

    /** Double indent for help output. */
    public static final String DOUBLE_INDENT = INDENT + INDENT;

    /** */
    public static final String CLI_NAME = "control";

    /** */
    public static final String FULL_CLI_NAME = CLI_NAME + ".(sh|bat)";

    /** */
    public static final List<String> ASCII_LOGO = Arrays.asList(
        "   __________  ________________  _______   ____",
        "  /  _/ ___/ |/ /  _/_  __/ __/ / ___/ /  /  _/",
        " _/ // (_ /    // /  / / / _/  / /__/ /___/ /  ",
        "/___/\\___/_/|_/___/ /_/ /___/  \\___/____/___/",
        ""
    );

    /** */
    private final IgniteLogger logger;

    /** */
    private final CLIArgumentParser commonArgsParser;

    /** */
    private final CommandWithSubs registry = new CommandsRegistry();

    /** */
    private boolean experimentalEnabled;

    /** */
    private boolean verbose;

    /** */
    @SuppressWarnings("deprecation")
    public CLICommandFrontendImpl(IgniteLogger logger) {
        this.logger = logger;

        commonArgsParser = new CLIArgumentParser(
            Collections.emptyList(),
            Arrays.asList(
                optionalArg(CMD_HOST, "HOST_OR_IP", String.class),
                optionalArg(CMD_PORT, "PORT", String.class),
                optionalArg(CMD_USER, "USER", String.class),
                optionalArg(CMD_PASSWORD, "PASSWORD", String.class),
                optionalArg(CMD_PING_INTERVAL, "PING_INTERVAL", Long.class),
                optionalArg(CMD_PING_TIMEOUT, "PING_TIMEOUT", Long.class),
                optionalArg(CMD_VERBOSE, "", Boolean.class),
                optionalArg(CMD_SSL_PROTOCOL, "SSL_PROTOCOL[, SSL_PROTOCOL_2, ..., SSL_PROTOCOL_N]", String.class),
                optionalArg(CMD_SSL_CIPHER_SUITES, "SSL_CIPHER_1[, SSL_CIPHER_2, ..., SSL_CIPHER_N]", String.class),
                optionalArg(CMD_SSL_KEY_ALGORITHM, "SSL_KEY_ALGORITHM", String.class),
                optionalArg(CMD_KEYSTORE_TYPE, "KEYSTORE_TYPE", String.class),
                optionalArg(CMD_KEYSTORE, "KEYSTORE_PATH", String.class),
                optionalArg(CMD_KEYSTORE_PASSWORD, "KEYSTORE_PASSWORD", String.class),
                optionalArg(CMD_TRUSTSTORE_TYPE, "TRUSTSTORE_TYPE", String.class),
                optionalArg(CMD_TRUSTSTORE, "TRUSTSTORE_PATH", String.class),
                optionalArg(CMD_TRUSTSTORE_PASSWORD, "TRUSTSTORE_PASSWORD", String.class),
                optionalArg(
                    CMD_ENABLE_EXPERIMENTAL,
                    "",
                    Boolean.class,
                    () -> IgniteSystemProperties.getBoolean(IGNITE_ENABLE_EXPERIMENTAL_COMMAND)
                )
            ),
            false
        );
    }

    /** */
    public static void main(String[] args) {
        CLICommandFrontendImpl hnd = new CLICommandFrontendImpl(
            CommandHandler.setupJavaLogger(CLI_NAME, CLICommandFrontendImpl.class)
        );

        System.exit(hnd.execute(Arrays.stream(args).filter(String::isEmpty).collect(Collectors.toList())));
    }

    /** {@inheritDoc} */
    @Override public int execute(List<String> args) {
        commonArgsParser.parse(args.iterator());

        experimentalEnabled = commonArgsParser.get(CMD_ENABLE_EXPERIMENTAL);
        verbose = commonArgsParser.get(CMD_VERBOSE);

        LocalDateTime startTime = LocalDateTime.now();

        logCommonInfo();

        if (CommandHandler.isHelp(args))
            printUsage();
        else
            parseCommand(args);

        LocalDateTime endTime = LocalDateTime.now();

        CommandHandler.printExecutionTime(logger, endTime, Duration.between(startTime, endTime));

        return EXIT_CODE_OK;
    }

    /** */
    private void parseCommand(List<String> args) {
        AtomicReference<Command<?, ?, ?>> cmd = new AtomicReference<>();

        AtomicInteger i = new AtomicInteger();

        while (i.get() < args.size()) {
            String arg = args.get(i.getAndIncrement());

            if (cmd.get() == null && !isArgumentName(arg))
                continue;

            Command<?, ?, ?> cmd0 = registry.command(arg.substring(PARAMETER_PREFIX.length()));

            if (cmd0 == null) {
                Object instance = cmd.get();
                if (instance instanceof CommandWithSubs) {
                    if (!(instance instanceof Command))
                        throw new IllegalArgumentException("Unknown argument " + arg);

                    i.decrementAndGet();

                    break;
                }

                continue;
            }

            cmd.set(cmd0);

            if (!(cmd.get() instanceof CommandWithSubs))
                break;
        }

        if (cmd.get() == null)
            throw new IllegalArgumentException("Unknown command");

        List<CLIArgument<?>> namedArgs = new ArrayList<>();
        List<CLIArgument<?>> positionalArgs = new ArrayList<>();
        List<IgniteBiTuple<Boolean, List<CLIArgument<?>>>> oneOfArgs = new ArrayList<>();

        BiFunction<Field, Boolean, CLIArgument<?>> toArg = (fld, optional) -> new CLIArgument<>(
            parameterName(fld),
            null,
            optional,
            fld.getType(),
            null
        );

        visitCommandParams(
            cmd.get().args(),
            fld -> positionalArgs.add(new CLIArgument<>(
                fld.getName(),
                null,
                fld.getAnnotation(PositionalArgument.class).optional(),
                fld.getType(),
                null
            )),
            fld -> namedArgs.add(toArg.apply(fld, fld.getAnnotation(Argument.class).optional())),
            (optionals, flds) -> {
                List<CLIArgument<?>> oneOfArg = flds.stream().map(
                    fld -> toArg.apply(fld, fld.getAnnotation(Argument.class).optional())
                ).collect(Collectors.toList());

                oneOfArgs.add(F.t(optionals, oneOfArg));

                flds.forEach(fld -> namedArgs.add(toArg.apply(fld, true)));
            }
        );

        CLIArgumentParser parser = new CLIArgumentParser(positionalArgs, namedArgs, true);

        parser.parse(args.listIterator(i.get()));

        AtomicInteger position = new AtomicInteger();

        BiConsumer<Field, Object> fldSetter = (fld, val) -> {
            if (val == null)
                return;

            try {
                // TODO: use setters here.
                fld.setAccessible(true);
                fld.set(cmd.get(), val);
            }
            catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        };

        // TODO: check OneOf invariant here.
        // TODO: support OneOf for PositionalArgument.
        visitCommandParams(
            cmd.get().args(),
            fld -> fldSetter.accept(fld, parser.get(position.getAndIncrement())),
            fld -> fldSetter.accept(fld, parser.get(parameterName(fld))),
            (optionals, flds) ->
                flds.forEach(fld -> fldSetter.accept(fld, parser.get(parameterName(fld))))
        );

        logger.info(cmd.get().toString());
    }

    /** */
    private void printUsage() {
        logger.info("Control utility script is used to execute admin commands on cluster or get common cluster info. " +
            "The command has the following syntax:");
        logger.info("");
        logger.info(commonArgsParser.usage(false, INDENT + FULL_CLI_NAME, "[command] <command_parameters>"));
        logger.info("");
        logger.info("");
        logger.info("This utility can do the following commands:");

        registry.subcommands().forEach(cmdSupplier -> {
            Command<?, ?, ?> cmd = cmdSupplier.get();

            if (cmd.experimental() && !experimentalEnabled)
                return;

            usage(cmd, Collections.emptyList());
        });

        CommandHandler.printCommonInfo(logger);
    }

    /** */
    private void logCommonInfo() {
        for (String str : ASCII_LOGO)
            logger.info(str);

        logger.info("Control utility [ver. " + ACK_VER_STR + "]");
        logger.info(COPYRIGHT);
        logger.info("User: " + System.getProperty("user.name"));
        logger.info(TIME_PREFIX + LocalDateTime.now().format(U.CLI_FORMAT));
    }

    /** */
    private void usage(Command<?, ?, ?> cmd, List<CommandWithSubs> parents) {
        boolean skip = (cmd instanceof CommandWithSubs) && !(cmd instanceof Command);

        if (!skip) {
            logger.info("");

            printExample(cmd, parents);

            if (hasDescribedParameters(cmd)) {
                logger.info("");
                logger.info(DOUBLE_INDENT + "Parameters:");

                AtomicInteger maxParamLen = new AtomicInteger();

                Consumer<Field> lenCalc = fld -> {
                    maxParamLen.set(Math.max(maxParamLen.get(), parameterExample(fld, false).length()));

                    if (fld.isAnnotationPresent(EnumDescription.class)) {
                        EnumDescription enumDesc = fld.getAnnotation(EnumDescription.class);

                        for (String name : enumDesc.names())
                            maxParamLen.set(Math.max(maxParamLen.get(), name.length()));
                    }
                };

                visitCommandParams(cmd.args(), lenCalc, lenCalc, (optional, flds) -> flds.forEach(lenCalc));

                Consumer<Field> printer = fld -> {
                    BiConsumer<String, String> logParam = (name, description) -> logger.info(
                        DOUBLE_INDENT + INDENT + U.extendToLen(name, maxParamLen.get()) + "  - " + description + "."
                    );

                    if (!fld.isAnnotationPresent(EnumDescription.class)) {
                        Argument desc = fld.getAnnotation(Argument.class);
                        PositionalArgument posDesc = fld.getAnnotation(PositionalArgument.class);

                        if (desc != null && desc.excludeFromDescription())
                            return;

                        logParam.accept(
                            parameterExample(fld, false),
                            (desc != null ? desc.description() : posDesc.description())
                        );
                    }
                    else {
                        EnumDescription enumDesc = fld.getAnnotation(EnumDescription.class);

                        String[] names = enumDesc.names();
                        String[] descriptions = enumDesc.descriptions();

                        for (int i = 0; i < names.length; i++)
                            logParam.accept(names[i], descriptions[i]);
                    }
                };

                visitCommandParams(cmd.args(), printer, printer, (optional, flds) -> flds.forEach(printer));
            }
        }

        if (cmd instanceof CommandWithSubs) {
            List<CommandWithSubs> parents0 = new ArrayList<>(parents);

            parents0.add((CommandWithSubs)cmd);

            ((CommandWithSubs)cmd).subcommands().forEach(cmdSupplier -> {
                Command<?, ?, ?> cmd0 = cmdSupplier.get();

                if (cmd0.experimental() && !experimentalEnabled)
                    return;

                usage(cmd0, parents0);
            });
        }
    }

    /** */
    private void printExample(Command<?, ?, ?> cmd, List<CommandWithSubs> parents) {
        if (cmd.experimental())
            logger.info(INDENT + EXPERIMENTAL_LABEL);

        logger.info(INDENT + cmd.description() + ":");

        StringBuilder bldr =
            new StringBuilder(DOUBLE_INDENT + FULL_CLI_NAME);

        AtomicBoolean prefixInclude = new AtomicBoolean(true);
        StringBuilder parentPrefix = new StringBuilder();

        Consumer<Object> namePrinter = cmd0 -> {
            bldr.append(' ');

            if (prefixInclude.get())
                bldr.append(PARAMETER_PREFIX);

            String cmdName = commandName(cmd0.getClass(), CMD_WORDS_DELIM);

            if (parentPrefix.length() > 0) {
                cmdName = cmdName.replaceFirst(parentPrefix.toString(), "");

                if (!prefixInclude.get())
                    cmdName = cmdName.replaceAll(CMD_WORDS_DELIM + "", PARAM_WORDS_DELIM + "");
            }

            bldr.append(cmdName);

            parentPrefix.append(cmdName).append(CMD_WORDS_DELIM);

            if (cmd0 instanceof CommandWithSubs)
                prefixInclude.set(!(cmd0.getClass().isAnnotationPresent(CliPositionalSubcommands.class)));
        };

        parents.forEach(namePrinter);
        namePrinter.accept(cmd);

        BiConsumer<Boolean, Field> paramPrinter = (spaceReq, fld) -> {
            if (spaceReq)
                bldr.append(' ');

            bldr.append(parameterExample(fld, true));
        };

        visitCommandParams(
            cmd.args(),
            fld -> bldr.append(' ').append(valueExample(fld)),
            fld -> paramPrinter.accept(true, fld),
            (optional, flds) -> {
                bldr.append(' ');

                for (int i = 0; i < flds.size(); i++) {
                    if (i != 0)
                        bldr.append('|');

                    paramPrinter.accept(false, flds.get(i));
                }
            }
        );

        logger.info(bldr.toString());
    }

    /** {@inheritDoc} */
    @Override public <T> T getLastOperationResult() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void console(GridConsole console) {
        throw new UnsupportedOperationException();
    }
}
