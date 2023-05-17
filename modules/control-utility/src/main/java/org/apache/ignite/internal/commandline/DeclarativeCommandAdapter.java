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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.commandline.argument.parser.CLIArgument;
import org.apache.ignite.internal.commandline.argument.parser.CLIArgumentParser;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.AbstractCommandInvoker;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.ArgumentGroup;
import org.apache.ignite.internal.management.api.BeforeNodeStartCommand;
import org.apache.ignite.internal.management.api.CliPositionalSubcommands;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.management.api.CommandsRegistry;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.management.api.EnumDescription;
import org.apache.ignite.internal.management.api.HelpCommand;
import org.apache.ignite.internal.management.api.LocalCommand;
import org.apache.ignite.internal.management.api.Positional;
import org.apache.ignite.internal.management.api.WithCliConfirmParameter;
import org.apache.ignite.internal.util.lang.PeekableIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.commandline.CommandHandler.UTILITY_NAME;
import static org.apache.ignite.internal.commandline.CommandLogger.DOUBLE_INDENT;
import static org.apache.ignite.internal.commandline.CommandLogger.INDENT;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_AUTO_CONFIRMATION;
import static org.apache.ignite.internal.commandline.argument.parser.CLIArgument.optionalArg;
import static org.apache.ignite.internal.management.api.CommandUtils.CMD_WORDS_DELIM;
import static org.apache.ignite.internal.management.api.CommandUtils.PARAMETER_PREFIX;
import static org.apache.ignite.internal.management.api.CommandUtils.PARAM_WORDS_DELIM;
import static org.apache.ignite.internal.management.api.CommandUtils.parameterExample;
import static org.apache.ignite.internal.management.api.CommandUtils.toFormattedCommandName;
import static org.apache.ignite.internal.management.api.CommandUtils.toFormattedFieldName;
import static org.apache.ignite.internal.management.api.CommandUtils.valueExample;

/**
 * Adapter of new management API command for legacy {@code control.sh} execution flow.
 */
public class DeclarativeCommandAdapter<A extends IgniteDataTransferObject> extends AbstractCommandInvoker implements Command<A> {
    /** Root command to start parsing from. */
    private final org.apache.ignite.internal.management.api.Command<?, ?> baseCmd;

    /** Command to execute. */
    private org.apache.ignite.internal.management.api.Command<A, ?> cmd;

    /** Parsed argument. */
    private A arg;

    /** Confirmed flag value. */
    private boolean confirmed = true;

    /** Message. */
    private String confirmMsg;

    /** @param baseCmd Base command. */
    public DeclarativeCommandAdapter(org.apache.ignite.internal.management.api.Command<?, ?> baseCmd) {
        this.baseCmd = baseCmd;

        assert baseCmd != null;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        PeekableIterator<String> cliArgs = argIter.raw();

        org.apache.ignite.internal.management.api.Command<A, ?> cmd0 = baseCmd instanceof CommandsRegistry
                ? command((CommandsRegistry<?, ?>)baseCmd, cliArgs, true)
                : (org.apache.ignite.internal.management.api.Command<A, ?>)baseCmd;

        if (cmd0 instanceof HelpCommand) {
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
            null
        );

        Consumer<Field> namedArgCb =
            fld -> namedArgs.add(toArg.apply(fld, fld.getAnnotation(Argument.class).optional()));

        Consumer<Field> positionalArgCb = fld -> positionalArgs.add(new CLIArgument<>(
            fld.getName(),
            null,
            fld.getAnnotation(Argument.class).optional(),
            fld.getType(),
            null
        ));

        BiConsumer<ArgumentGroup, List<Field>> argGrpCb = (argGrp, flds) -> flds.forEach(fld -> {
            if (fld.isAnnotationPresent(Positional.class))
                positionalArgCb.accept(fld);
            else
                namedArgs.add(toArg.apply(fld, true));
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

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, IgniteLogger logger) throws Exception {
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
        try (GridClient client = Command.startClient(clientCfg)) {
            if (cmd.getClass().isAnnotationPresent(Deprecated.class) &&
                cmd.deprecationMessage() != null) {
                logger.warning(cmd.deprecationMessage());
            }

            R res;

            if (cmd instanceof LocalCommand)
                res = ((LocalCommand<A, R>)cmd).execute(client, arg, logger::info);
            else if (cmd instanceof ComputeCommand) {
                GridClientCompute compute = client.compute();

                Map<UUID, GridClientNode> clusterNodes = compute.nodes().stream()
                    .collect(toMap(GridClientNode::nodeId, n -> n));

                ComputeCommand<A, R> cmd = (ComputeCommand<A, R>)this.cmd;

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
        try (GridClientBeforeNodeStart client = Command.startClientBeforeNodeStart(clientCfg)) {
            return ((BeforeNodeStartCommand<A, R>)cmd).execute(client, arg, logger::info);
        }
        catch (GridClientDisconnectedException e) {
            throw new GridClientException(e.getCause());
        }
        finally {
            state(null, null, true);
        }
    }

    /** {@inheritDoc} */
    @Override public void printUsage(IgniteLogger logger) {
        usage(baseCmd, Collections.emptyList(), logger);
    }

    /**
     * Generates usage for base command and all of its children, if any.
     *
     * @param cmd Base command.
     * @param parents Collection of parent commands.
     * @param logger Logger to print help to.
     */
    private void usage(
        org.apache.ignite.internal.management.api.Command<?, ?> cmd,
        List<org.apache.ignite.internal.management.api.Command<?, ?>> parents,
        IgniteLogger logger
    ) {
        if (cmd instanceof LocalCommand || cmd instanceof ComputeCommand || cmd instanceof HelpCommand) {
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
            List<org.apache.ignite.internal.management.api.Command<?, ?>> parents0 = new ArrayList<>(parents);

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
    private void printExample(
        org.apache.ignite.internal.management.api.Command<?, ?> cmd,
        List<org.apache.ignite.internal.management.api.Command<?, ?>> parents,
        IgniteLogger logger
    ) {
        logger.info(INDENT + cmd.description() + ":");

        StringBuilder bldr = new StringBuilder(DOUBLE_INDENT + UTILITY_NAME);

        AtomicBoolean prefixInclude = new AtomicBoolean(true);
        StringBuilder parentPrefix = new StringBuilder();

        Consumer<Object> namePrinter = cmd0 -> {
            bldr.append(' ');

            if (prefixInclude.get())
                bldr.append(PARAMETER_PREFIX);

            String cmdName = toFormattedCommandName(cmd0.getClass());

            if (parentPrefix.length() > 0) {
                cmdName = cmdName.replaceFirst(parentPrefix.toString(), "");

                if (!prefixInclude.get())
                    cmdName = cmdName.replaceAll(CMD_WORDS_DELIM + "", PARAM_WORDS_DELIM + "");
            }

            bldr.append(cmdName);

            parentPrefix.append(cmdName).append(CMD_WORDS_DELIM);

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

                    for (int i = 0; i < flds.size(); i++) {
                        if (i != 0)
                            bldr.append('|');

                        paramPrinter.accept(false, flds.get(i));
                    }
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

    /** {@inheritDoc} */
    @Override public void prepareConfirmation(GridClientConfiguration clientCfg) throws Exception {
        if (confirmed)
            return;

        try (GridClient client = Command.startClient(clientCfg)) {
            confirmMsg = cmd.confirmationPrompt(client, arg);
        }
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        return confirmMsg;
    }

    /** {@inheritDoc} */
    @Override public A arg() {
        return arg;
    }

    /** */
    private void state(
        org.apache.ignite.internal.management.api.Command<A, ?> cmd,
        A arg,
        boolean confirmed
    ) {
        this.cmd = cmd;
        this.arg = arg;
        this.confirmed = confirmed;
        this.confirmMsg = null;
    }

    /** {@inheritDoc} */
    @Override public String name() {
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
    private boolean hasDescribedParameters(org.apache.ignite.internal.management.api.Command<?, ?> cmd) {
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
    public org.apache.ignite.internal.management.api.Command<?, ?> command() {
        return baseCmd;
    }
}
