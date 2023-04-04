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

package org.apache.ignite.internal.commands.impl;

import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.commandline.GridConsole;
import org.apache.ignite.internal.commandline.argument.parser.CLIArgumentParser;
import org.apache.ignite.internal.commands.api.CLICommandFrontend;
import org.apache.ignite.internal.commands.api.Command;
import org.apache.ignite.internal.commands.api.CommandWithSubs;
import org.apache.ignite.internal.commands.api.Parameter;
import org.apache.ignite.internal.commands.api.PositionalParameter;
import org.apache.ignite.internal.util.typedef.internal.U;
import static org.apache.ignite.internal.IgniteVersionUtils.ACK_VER_STR;
import static org.apache.ignite.internal.IgniteVersionUtils.COPYRIGHT;
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
import static org.apache.ignite.internal.commands.impl.CommandUtils.CMD_WORDS_DELIM;
import static org.apache.ignite.internal.commands.impl.CommandUtils.PARAM_WORDS_DELIM;
import static org.apache.ignite.internal.commands.impl.CommandUtils.commandName;

/**
 *
 */
public class CLICommandFrontendImpl implements CLICommandFrontend {
    /** Indent for help output. */
    public static final String INDENT = "  ";

    /** Double indent for help output. */
    public static final String DOUBLE_INDENT = INDENT + INDENT;

    /** */
    public static final String PREFIX = "--";

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
    private final CommandsRegistry registry = new CommandsRegistry();

    /** */
    public CLICommandFrontendImpl(IgniteLogger logger) {
        this.logger = logger;

        commonArgsParser = new CLIArgumentParser(Arrays.asList(
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
            optionalArg(CMD_ENABLE_EXPERIMENTAL, "", Boolean.class)
        ));
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
        logCommonInfo();

        if (CommandHandler.isHelp(args)) {
            printUsage();

            return EXIT_CODE_OK;
        }

        return EXIT_CODE_OK;
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

        registry.iterator().forEachRemaining(cmd -> usage(cmd, Collections.emptyList()));

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
    private void usage(Command cmd, List<CommandWithSubs> parents) {
        boolean skip = (cmd instanceof CommandWithSubs) && !((CommandWithSubs)cmd).canBeExecuted();

        if (!skip) {
            logger.info("");

            printExample(cmd, parents);

            if (hasDescribedParameters(cmd)) {
                logger.info("");
                logger.info(DOUBLE_INDENT + "Parameters:");

                AtomicInteger maxParamLen = new AtomicInteger();

                Consumer<Field> fldCalc = fld -> maxParamLen.set(
                    Math.max(
                        maxParamLen.get(),
                        CommandUtils.parameterName(fld).length() + CommandUtils.examples(fld).length() + 3
                    )
                );

                CommandUtils.forEachField(
                    cmd.getClass(),
                    fld -> maxParamLen.set(
                        Math.max(maxParamLen.get(), CommandUtils.parameterName(fld.getName()).length())
                    ),
                    fldCalc,
                    (optional, flds) -> flds.forEach(fldCalc)
                );

                Consumer<Field> fldPrinter = fld -> {
                    Parameter desc = fld.getAnnotation(Parameter.class);

                    if (desc.excludeFromDescription())
                        return;

                    String prefix = desc.withoutPrefix() ? "" : PREFIX;
                    String example = CommandUtils.examples(fld);

                    logger.info(
                        DOUBLE_INDENT + INDENT +
                        U.extendToLen(prefix +
                        CommandUtils.parameterName(fld) + " " + example, maxParamLen.get()) +
                        "  - " + desc.description() + "."
                    );
                };

                CommandUtils.forEachField(
                    cmd.getClass(),
                    fld -> {
                        PositionalParameter desc = fld.getAnnotation(PositionalParameter.class);

                        logger.info(
                            DOUBLE_INDENT + INDENT +
                                U.extendToLen(CommandUtils.parameterName(fld.getName()), maxParamLen.get()) + "  - " +
                                desc.description() + "."
                        );
                    },
                    fldPrinter,
                    (optional, flds) -> flds.forEach(fldPrinter)
                );
            }
        }

        if (cmd instanceof CommandWithSubs) {
            List<CommandWithSubs> parents0 = new ArrayList<>(parents);

            parents0.add((CommandWithSubs)cmd);

            ((CommandWithSubs)cmd).subcommands().forEach(cmd0 -> usage(cmd0, parents0));
        }
    }

    /** */
    private boolean hasDescribedParameters(Command cmd) {
        AtomicBoolean res = new AtomicBoolean();

        CommandUtils.forEachField(
            cmd.getClass(),
            fld -> res.compareAndSet(false, !fld.getAnnotation(PositionalParameter.class).description().isEmpty()),
            fld -> res.compareAndSet(
                false,
                !fld.getAnnotation(Parameter.class).description().isEmpty()
                    && !fld.getAnnotation(Parameter.class).excludeFromDescription()
            ),
            (spaceReq, flds) -> flds.forEach(fld -> res.compareAndSet(
                false,
                !(fld.isAnnotationPresent(Parameter.class)
                    ? fld.getAnnotation(Parameter.class).description()
                    : fld.getAnnotation(PositionalParameter.class).description()
                ).isEmpty()
            ))
        );

        return res.get();
    }

    /** */
    private void printExample(Command cmd, List<CommandWithSubs> parents) {
        if (cmd.experimental())
            logger.info(INDENT + "[EXPERIMENTAL]");
        logger.info(INDENT + cmd.description() + ":");

        StringBuilder bldr =
            new StringBuilder(DOUBLE_INDENT + FULL_CLI_NAME);

        AtomicBoolean prefixInclude = new AtomicBoolean(true);
        StringBuilder parentPrefix = new StringBuilder();

        Consumer<Command> namePrinter = cmd0 -> {
            bldr.append(' ');

            if (prefixInclude.get())
                bldr.append(PREFIX);

            String cmdName = commandName(cmd0.getClass());

            if (parentPrefix.length() > 0) {
                cmdName = cmdName
                    .replaceFirst(parentPrefix.toString(), "")
                    .replaceAll(CMD_WORDS_DELIM + "", PARAM_WORDS_DELIM + "");
            }

            bldr.append(cmdName);

            parentPrefix.append(cmdName).append(CMD_WORDS_DELIM);

            if (cmd0 instanceof CommandWithSubs)
                prefixInclude.set(!((CommandWithSubs)cmd0).positionalSubsName());
        };

        parents.forEach(namePrinter);
        namePrinter.accept(cmd);

        BiConsumer<Boolean, Field> paramPrinter = (spaceReq, fld) -> {
            Parameter desc = fld.getAnnotation(Parameter.class);
            PositionalParameter posDesc = fld.getAnnotation(PositionalParameter.class);

            assert desc != null || posDesc != null;

            boolean optional = (desc != null && desc.optional()) || (posDesc != null && posDesc.optional());
            boolean withoutPrefix = desc == null || desc.withoutPrefix();

            if (spaceReq)
                bldr.append(' ');

            if (optional)
                bldr.append('[');

            if (!withoutPrefix)
                bldr.append(PREFIX);

            if (desc != null)
                bldr.append(CommandUtils.parameterName(fld));

            String examples = CommandUtils.examples(fld);

            if (!examples.isEmpty()) {
                if (desc != null)
                    bldr.append(' ');

                bldr.append(examples);
            }

            if (optional)
                bldr.append(']');
        };

        CommandUtils.forEachField(
            cmd.getClass(),
            fld -> bldr.append(' ').append(CommandUtils.examples(fld)),
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
