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

package org.apache.ignite.internal.commandline;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientBeforeNodeStart;
import org.apache.ignite.internal.client.GridClientCompute;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientDisconnectedException;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.commandline.argument.parser.CLIArgument;
import org.apache.ignite.internal.commandline.argument.parser.CLIArgumentParser;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.AbstractCommandInvoker;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.ArgumentGroup;
import org.apache.ignite.internal.management.api.BeforeNodeStartCommand;
import org.apache.ignite.internal.management.api.CliPositionalSubcommands;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.management.api.CommandsRegistry;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.management.api.EnumDescription;
import org.apache.ignite.internal.management.api.HelpCommand;
import org.apache.ignite.internal.management.api.LocalCommand;
import org.apache.ignite.internal.management.api.Positional;
import org.apache.ignite.internal.management.api.WithCliConfirmParameter;
import org.apache.ignite.internal.management.cache.CacheCommand;
import org.apache.ignite.internal.util.lang.PeekableIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.commandline.CommandHandler.UTILITY_NAME;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_AUTO_CONFIRMATION;
import static org.apache.ignite.internal.commandline.CommonArgParser.getCommonOptions;
import static org.apache.ignite.internal.commandline.argument.parser.CLIArgument.optionalArg;
import static org.apache.ignite.internal.management.api.CommandUtils.CMD_WORDS_DELIM;
import static org.apache.ignite.internal.management.api.CommandUtils.DOUBLE_INDENT;
import static org.apache.ignite.internal.management.api.CommandUtils.INDENT;
import static org.apache.ignite.internal.management.api.CommandUtils.PARAMETER_PREFIX;
import static org.apache.ignite.internal.management.api.CommandUtils.PARAM_WORDS_DELIM;
import static org.apache.ignite.internal.management.api.CommandUtils.join;
import static org.apache.ignite.internal.management.api.CommandUtils.parameterExample;
import static org.apache.ignite.internal.management.api.CommandUtils.toFormattedCommandName;
import static org.apache.ignite.internal.management.api.CommandUtils.toFormattedFieldName;
import static org.apache.ignite.internal.management.api.CommandUtils.valueExample;

/**
 * Adapter of new management API command for legacy {@code control.sh} execution flow.
 */
public class DeclarativeCommandAdapter<A extends IgniteDataTransferObject> extends AbstractCommandInvoker {
    /** Root command to start parsing from. */
    private final Command<?, ?> baseCmd;

    /** Command to execute. */
    private Command<A, ?> cmd;

    /** Parsed argument. */
    private A arg;

    /** Confirmed flag value. */
    private boolean confirmed = true;

    /** Prepared flag value. */
    private boolean prepared;

    /**
     * If {@code true} then command prepared successfully, {@code false} otherwise.
     * @see ComputeCommand#prepare(GridClient, IgniteDataTransferObject, Consumer)
     */
    private boolean executeAfterPreparation = true;

    /** Message. */
    private String confirmMsg;

    /** @param baseCmd Base command. */
    public DeclarativeCommandAdapter(Command<?, ?> baseCmd) {
        this.baseCmd = baseCmd;

        assert baseCmd != null;
    }

    /**
     * Parse command-specific arguments.
     *
     * @param argIter Argument iterator.
     */
    public void parseArguments(CommandArgIterator argIter) {
        PeekableIterator<String> cliArgs = argIter.raw();

        Command<A, ?> cmd0 = baseCmd instanceof CommandsRegistry
                ? command((CommandsRegistry<?, ?>)baseCmd, cliArgs, true)
                : (Command<A, ?>)baseCmd;

        if (cmd0 instanceof HelpCommand) {
            if (cliArgs.hasNext() && cliArgs.peek().equals("help"))
                cliArgs.next();

            state(cmd0, null, true);

            return;
        }

        if (!(cmd0 instanceof ComputeCommand) && !(cmd0 instanceof LocalCommand) && !(cmd0 instanceof BeforeNodeStartCommand)) {
            throw new IllegalArgumentException(
                "Command " + toFormattedCommandName(cmd0.getClass()) + " can't be executed"
            );
        }

        List<CLIArgument<?>> namedArgs = new ArrayList<>();
        List<CLIArgument<?>> positionalArgs = new ArrayList<>();

        BiFunction<Field, Boolean, CLIArgument<?>> toArg = (fld, optional) -> new CLIArgument<>(
            toFormattedFieldName(fld),
            null,
            optional,
            fld.getType(),
            null,
            (name, val) -> {}
        );

        ArgumentGroup argGrp = cmd0.argClass().getAnnotation(ArgumentGroup.class);
        Set<String> grpdFlds = argGrp == null
            ? Collections.emptySet()
            : new HashSet<>(Arrays.asList(argGrp.value()));

        Consumer<Field> namedArgCb = fld -> namedArgs.add(
            toArg.apply(fld, grpdFlds.contains(fld.getName()) || fld.getAnnotation(Argument.class).optional())
        );

        Consumer<Field> positionalArgCb = fld -> positionalArgs.add(new CLIArgument<>(
            fld.getName(),
            null,
            fld.getAnnotation(Argument.class).optional(),
            fld.getType(),
            null,
            (name, val) -> {}
        ));

        BiConsumer<ArgumentGroup, List<Field>> argGrpCb = (argGrp0, flds) -> flds.forEach(fld -> {
            if (fld.isAnnotationPresent(Positional.class))
                positionalArgCb.accept(fld);
            else
                namedArgCb.accept(fld);
        });

        visitCommandParams(cmd0.argClass(), positionalArgCb, namedArgCb, argGrpCb);

        namedArgs.add(optionalArg(CMD_AUTO_CONFIRMATION, "Confirm without prompt", boolean.class, () -> false));

        CLIArgumentParser parser = new CLIArgumentParser(positionalArgs, namedArgs);

        parser.parse(cliArgs);

        try {
            state(
                cmd0,
                argument(
                    cmd0.argClass(),
                    (fld, pos) -> parser.get(pos),
                    fld -> parser.get(toFormattedFieldName(fld))
                ),
                parser.get(CMD_AUTO_CONFIRMATION)
            );
        }
        catch (InstantiationException | IllegalAccessException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Actual command execution with verbose mode if needed.
     * Implement it if your command supports verbose mode.
     *
     * @param clientCfg Thin client configuration if connection to cluster is necessary.
     * @param logger Logger to use.
     * @param verbose Use verbose mode or not
     * @return Result of operation (mostly usable for tests).
     * @throws Exception If error occur.
     */
    public Object execute(GridClientConfiguration clientCfg, IgniteLogger logger, boolean verbose) throws Exception {
        if (cmd instanceof BeforeNodeStartCommand)
            return executeBeforeNodeStart(clientCfg, logger);

        return execute0(clientCfg, logger);
    }

    /**
     * @param clientCfg Client configuration.
     * @param logger Logger to print result.
     * @return Command result.
     * @param <R> Result type
     * @throws Exception If failed.
     */
    private <R> R execute0(GridClientConfiguration clientCfg, IgniteLogger logger) throws Exception {
        try (GridClient client = startClient(clientCfg)) {
            String deprecationMsg = cmd.deprecationMessage(arg);

            if (deprecationMsg != null)
                logger.warning(deprecationMsg);

            R res;

            if (cmd instanceof LocalCommand)
                res = ((LocalCommand<A, R>)cmd).execute(client, arg, logger::info);
            else if (cmd instanceof ComputeCommand) {
                GridClientCompute compute = client.compute();

                Map<UUID, GridClientNode> clusterNodes = compute.nodes().stream()
                    .collect(toMap(GridClientNode::nodeId, n -> n));

                ComputeCommand<A, R> cmd = (ComputeCommand<A, R>)this.cmd;

                if (!prepared)
                    executeAfterPreparation = cmd.prepare(client, arg, logger::info);

                if (!executeAfterPreparation)
                    return null;

                Collection<UUID> nodeIds = commandNodes(
                    cmd,
                    arg,
                    clusterNodes.values()
                        .stream()
                        .collect(toMap(GridClientNode::nodeId, n -> new T3<>(n.isClient(), n.consistentId(), n.order()))),
                    TaskExecutor.defaultNode(client, clientCfg).nodeId()
                );

                Collection<GridClientNode> connectable = F.viewReadOnly(
                    nodeIds,
                    clusterNodes::get,
                    id -> clusterNodes.get(id).connectable()
                );

                if (!F.isEmpty(connectable))
                    compute = compute.projection(connectable);

                res = compute.execute(cmd.taskClass().getName(), new VisorTaskArgument<>(nodeIds, arg, false));

                cmd.printResult(arg, res, logger::info);
            }
            else
                throw new IllegalArgumentException("Unknown command type: " + cmd);

            return res;
        }
        catch (Throwable e) {
            logger.error("Failed to perform operation.");
            logger.error(CommandLogger.errorMessage(e));

            throw e;
        }
        finally {
            state(null, null, true);
        }
    }

    /** */
    private <R> R executeBeforeNodeStart(GridClientConfiguration clientCfg, IgniteLogger logger) throws Exception {
        try (GridClientBeforeNodeStart client = startClientBeforeNodeStart(clientCfg)) {
            return ((BeforeNodeStartCommand<A, R>)cmd).execute(client, arg, logger::info);
        }
        catch (GridClientDisconnectedException e) {
            throw new GridClientException(e.getCause());
        }
        finally {
            state(null, null, true);
        }
    }

    /**
     * Print info for user about command (parameters, use cases and so on).
     *
     * @param logger Logger to use.
     */
    public void printUsage(IgniteLogger logger) {
        if (baseCmd instanceof CacheCommand || baseCmd instanceof CacheCommand.CacheHelpCommand)
            printCacheHelpHeader(logger);

        usage(baseCmd, Collections.emptyList(), logger);

        if (baseCmd instanceof CacheCommand || baseCmd instanceof CacheCommand.CacheHelpCommand)
            logger.info("");
    }

    /**
     * Generates usage for base command and all of its children, if any.
     *
     * @param cmd Base command.
     * @param parents Collection of parent commands.
     * @param logger Logger to print help to.
     */
    private void usage(Command<?, ?> cmd, List<Command<?, ?>> parents, IgniteLogger logger) {
        if (cmd instanceof LocalCommand
            || cmd instanceof ComputeCommand
            || cmd instanceof HelpCommand
            || cmd instanceof BeforeNodeStartCommand) {
            logger.info("");

            if (cmd.experimental())
                logger.info(INDENT + "[EXPERIMENTAL]");

            printExample(cmd, parents, logger);

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

                visitCommandParams(cmd.argClass(), lenCalc, lenCalc, (argGrp, flds) -> flds.forEach(lenCalc));

                Consumer<Field> printer = fld -> {
                    BiConsumer<String, String> logParam = (name, description) -> {
                        if (F.isEmpty(description))
                            return;

                        logger.info(
                            DOUBLE_INDENT + INDENT + U.extendToLen(name, maxParamLen.get()) + "  - " + description + "."
                        );
                    };

                    if (!fld.isAnnotationPresent(EnumDescription.class)) {
                        logParam.accept(
                            parameterExample(fld, false),
                            fld.getAnnotation(Argument.class).description()
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

                visitCommandParams(cmd.argClass(), printer, printer, (argGrp, flds) -> {
                    flds.stream().filter(fld -> fld.isAnnotationPresent(Positional.class)).forEach(printer);
                    flds.stream().filter(fld -> !fld.isAnnotationPresent(Positional.class)).forEach(printer);
                });
            }
        }

        if (cmd instanceof CommandsRegistry) {
            List<Command<?, ?>> parents0 = new ArrayList<>(parents);

            parents0.add(cmd);

            ((CommandsRegistry<?, ?>)cmd).commands().forEachRemaining(cmd0 -> usage(cmd0.getValue(), parents0, logger));
        }
    }

    /**
     * Generates and prints example of command.
     *
     * @param cmd Command.
     * @param parents Collection of parent commands.
     * @param logger Logger to print help to.
     */
    private void printExample(Command<?, ?> cmd, List<Command<?, ?>> parents, IgniteLogger logger) {
        logger.info(INDENT + cmd.description() + ":");

        StringBuilder bldr = new StringBuilder(DOUBLE_INDENT + UTILITY_NAME);

        AtomicBoolean prefixInclude = new AtomicBoolean(true);

        AtomicReference<String> parentPrefix = new AtomicReference<>();

        Consumer<Object> namePrinter = cmd0 -> {
            bldr.append(' ');

            if (prefixInclude.get())
                bldr.append(PARAMETER_PREFIX);

            String cmdName = toFormattedCommandName(cmd0.getClass());

            String parentPrefix0 = parentPrefix.get();

            parentPrefix.set(cmdName);

            if (!F.isEmpty(parentPrefix0)) {
                cmdName = cmdName.replaceFirst(parentPrefix0 + CMD_WORDS_DELIM, "");

                if (!prefixInclude.get())
                    cmdName = cmdName.replaceAll(CMD_WORDS_DELIM + "", PARAM_WORDS_DELIM + "");
            }

            bldr.append(cmdName);

            if (cmd0 instanceof CommandsRegistry)
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
            cmd.argClass(),
            fld -> bldr.append(' ').append(valueExample(fld)),
            fld -> paramPrinter.accept(true, fld),
            (argGrp, flds) -> {
                if (argGrp.onlyOneOf()) {
                    bldr.append(' ');

                    if (argGrp.optional())
                        bldr.append('[');

                    for (int i = 0; i < flds.size(); i++) {
                        if (i != 0)
                            bldr.append('|');

                        paramPrinter.accept(false, flds.get(i));
                    }

                    if (argGrp.optional())
                        bldr.append(']');
                }
                else {
                    flds.stream()
                        .filter(fld -> fld.isAnnotationPresent(Positional.class))
                        .forEach(fld -> bldr.append(' ').append(valueExample(fld)));
                    flds.stream()
                        .filter(fld -> !fld.isAnnotationPresent(Positional.class))
                        .forEach(fld -> paramPrinter.accept(true, fld));
                }
            }
        );

        if (cmd.argClass().isAnnotationPresent(WithCliConfirmParameter.class))
            bldr.append(' ').append(CommandUtils.asOptional(CMD_AUTO_CONFIRMATION, true));

        logger.info(bldr.toString());
    }

    /**
     * Prepares confirmation for the command.
     *
     * @param clientCfg Thin client configuration.
     * @param logger Logger.
     * @throws Exception If error occur.
     */
    public void prepareConfirmation(GridClientConfiguration clientCfg, IgniteLogger logger) throws Exception {
        if (confirmed)
            return;

        try (GridClient client = startClient(clientCfg)) {
            if (cmd instanceof ComputeCommand) {
                prepared = true;

                executeAfterPreparation = ((ComputeCommand<A, ?>)cmd).prepare(client, arg, logger::info);

                if (!executeAfterPreparation)
                    return;
            }

            confirmMsg = cmd.confirmationPrompt(client, arg);
        }
    }

    /**
     * @return Message text to show user for. If null it means that confirmantion is not needed.
     */
    public String confirmationPrompt() {
        return confirmMsg;
    }

    /**
     * @return Command arguments which were parsed during {@link #parseArguments(CommandArgIterator)} call.
     */
    public A arg() {
        return arg;
    }

    /**
     * Return {@code true} if the command is experimental or {@code false}
     * otherwise.
     *
     * @return {@code true} if the command is experimental or {@code false}
     *      otherwise.
     */
    public boolean experimental() {
        return cmd == null ? baseCmd.experimental() : cmd.experimental();
    }

    /** */
    private void state(Command<A, ?> cmd, A arg, boolean confirmed) {
        this.cmd = cmd;
        this.arg = arg;
        this.confirmed = confirmed;
        confirmMsg = null;
        prepared = false;
        executeAfterPreparation = true;
    }

    /**
     * @return command name.
     */
    public String name() {
        return toFormattedCommandName(baseCmd.getClass()).toUpperCase();
    }

    /** {@inheritDoc} */
    @Override public IgniteEx grid() {
        return null;
    }

    /** @return {@code True} if help for parsed command must be printer. */
    public boolean isHelp() {
        return cmd instanceof HelpCommand;
    }

    /**
     * @param cmd Command.
     * @return {@code True} if command has described parameters.
     */
    private boolean hasDescribedParameters(Command<?, ?> cmd) {
        AtomicBoolean res = new AtomicBoolean();

        visitCommandParams(
            cmd.argClass(),
            fld -> res.compareAndSet(false,
                !fld.getAnnotation(Argument.class).description().isEmpty() ||
                    fld.isAnnotationPresent(EnumDescription.class)
            ),
            fld -> res.compareAndSet(false,
                !fld.getAnnotation(Argument.class).description().isEmpty() ||
                    fld.isAnnotationPresent(EnumDescription.class)
            ),
            (argGrp, flds) -> flds.forEach(fld -> res.compareAndSet(false,
                !fld.getAnnotation(Argument.class).description().isEmpty() ||
                    fld.isAnnotationPresent(EnumDescription.class)
            ))
        );

        return res.get();
    }

    /** */
    public Command<?, ?> command() {
        return baseCmd;
    }

    /** */
    private void printCacheHelpHeader(IgniteLogger logger) {
        logger.info(INDENT + "The '--cache subcommand' is used to get information about and perform actions" +
            " with caches. The command has the following syntax:");
        logger.info("");
        logger.info(INDENT + join(" ", UTILITY_NAME, join(" ", getCommonOptions())) + " " +
            "--cache [subcommand] <subcommand_parameters>");
        logger.info("");
        logger.info(INDENT + "The subcommands that take [nodeId] as an argument ('list', 'find_garbage', " +
            "'contention' and 'validate_indexes') will be executed on the given node or on all server nodes" +
            " if the option is not specified. Other commands will run on a random server node.");
        logger.info("");
        logger.info("");
        logger.info(INDENT + "Subcommands:");
    }

    /**
     * Method to create thin client for communication with cluster.
     *
     * @param clientCfg Thin client configuration.
     * @return Grid thin client instance which is already connected to cluster.
     * @throws Exception If error occur.
     */
    public static GridClient startClient(GridClientConfiguration clientCfg) throws Exception {
        GridClient client = GridClientFactory.start(clientCfg);

        // If connection is unsuccessful, fail before doing any operations:
        if (!client.connected()) {
            GridClientException lastErr = client.checkLastError();

            try {
                client.close();
            }
            catch (Throwable e) {
                lastErr.addSuppressed(e);
            }

            throw lastErr;
        }

        return client;
    }

    /**
     * Method to create thin client for communication with node before it starts.
     * If node has already started, there will be an error.
     *
     * @param clientCfg Thin client configuration.
     * @return Grid thin client instance which is already connected to node before it starts.
     * @throws Exception If error occur.
     */
    public static GridClientBeforeNodeStart startClientBeforeNodeStart(
        GridClientConfiguration clientCfg
    ) throws Exception {
        GridClientBeforeNodeStart client = GridClientFactory.startBeforeNodeStart(clientCfg);

        // If connection is unsuccessful, fail before doing any operations:
        if (!client.connected()) {
            GridClientException lastErr = client.checkLastError();

            try {
                client.close();
            }
            catch (Throwable e) {
                lastErr.addSuppressed(e);
            }

            throw lastErr;
        }

        return client;
    }
}
