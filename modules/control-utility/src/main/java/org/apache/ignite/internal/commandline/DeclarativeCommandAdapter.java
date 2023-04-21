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
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientCompute;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.commandline.argument.parser.CLIArgument;
import org.apache.ignite.internal.commandline.argument.parser.CLIArgumentParser;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.AbstractCommandInvoker;
import org.apache.ignite.internal.management.IgniteCommandRegistry;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.CliPositionalSubcommands;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.management.api.CommandsRegistry;
import org.apache.ignite.internal.management.api.EnumDescription;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.lang.IgniteBiTuple;
import static java.util.Collections.singleton;
import static org.apache.ignite.internal.commandline.CommandHandler.UTILITY_NAME;
import static org.apache.ignite.internal.commandline.CommandLogger.DOUBLE_INDENT;
import static org.apache.ignite.internal.commandline.CommandLogger.INDENT;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_AUTO_CONFIRMATION;
import static org.apache.ignite.internal.commandline.TaskExecutor.getBalancedNode;
import static org.apache.ignite.internal.management.api.CommandUtils.CMD_WORDS_DELIM;
import static org.apache.ignite.internal.management.api.CommandUtils.PARAMETER_PREFIX;
import static org.apache.ignite.internal.management.api.CommandUtils.PARAM_WORDS_DELIM;
import static org.apache.ignite.internal.management.api.CommandUtils.commandName;
import static org.apache.ignite.internal.management.api.CommandUtils.hasDescribedParameters;
import static org.apache.ignite.internal.management.api.CommandUtils.parameterExample;
import static org.apache.ignite.internal.management.api.CommandUtils.parameterName;
import static org.apache.ignite.internal.management.api.CommandUtils.valueExample;

/**
 *
 */
public class DeclarativeCommandAdapter<A extends IgniteDataTransferObject> extends AbstractCommandInvoker implements Command<A> {
    /** */
    private final IgniteCommandRegistry standaloneRegistry = new IgniteCommandRegistry();

    /** */
    private final org.apache.ignite.internal.management.api.Command<A, ?, ?> baseCmd;

    /** */
    private IgniteBiTuple<org.apache.ignite.internal.management.api.Command<A, ?, ?>, A> parsed;

    /** */
    public DeclarativeCommandAdapter(String name) {
        baseCmd = standaloneRegistry.command(name);

        assert baseCmd != null;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIterator) {
        org.apache.ignite.internal.management.api.Command<A, ?, ?> cmd0 =
            baseCmd instanceof CommandsRegistry
                ? command(
                    (CommandsRegistry)baseCmd,
                    argIterator.raw(),
                    true)
                : baseCmd;

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
            cmd0.args(),
            fld -> positionalArgs.add(new CLIArgument<>(
                fld.getName(),
                null,
                fld.getAnnotation(Argument.class).optional(),
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

        namedArgs.add(CLIArgument.optionalArg(CMD_AUTO_CONFIRMATION, "Confirm without prompt", boolean.class));

        CLIArgumentParser parser = new CLIArgumentParser(positionalArgs, namedArgs);

        parser.parse(argIterator.raw());

        try {
            parsed = F.t(
                cmd0,
                argument(
                    cmd0.args(),
                    (fld, pos) -> parser.get(pos),
                    fld -> parser.get(parameterName(fld))
                )
            );
        }
        catch (InstantiationException | IllegalAccessException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, IgniteLogger logger) throws Exception {
        return execute0(clientCfg, logger);
    }

    /** */
    private <R> R execute0(GridClientConfiguration clientCfg, IgniteLogger logger) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            GridClientCompute compute = client.compute();

            Map<UUID, GridClientNode> clusterNodes = compute.nodes().stream()
                .collect(Collectors.toMap(GridClientNode::nodeId, n -> n));

            Collection<UUID> nodeIds = parsed.get1().nodes(clusterNodes.keySet(), parsed.get2());

            for (UUID id : nodeIds) {
                if (!clusterNodes.containsKey(id))
                    throw new IllegalArgumentException("Node with id=" + id + " not found.");
            }

            if (nodeIds.isEmpty())
                nodeIds = singleton(getBalancedNode(compute).nodeId());

            Collection<GridClientNode> connectable = F.viewReadOnly(
                nodeIds,
                clusterNodes::get,
                id -> clusterNodes.get(id).connectable()
            );

            if (!F.isEmpty(connectable))
                compute = compute.projection(connectable);

            org.apache.ignite.internal.management.api.Command<A, R, ComputeTask<VisorTaskArgument<A>, R>> cmd =
                (org.apache.ignite.internal.management.api.Command<A, R, ComputeTask<VisorTaskArgument<A>, R>>)parsed.get1();

            R res = compute.execute(cmd.task().getName(), new VisorTaskArgument<>(nodeIds, parsed.get2(), false));

            cmd.printResult(parsed.get2(), res, logger::info);

            return res;
        }
        catch (Throwable e) {
            logger.error("Failed to perform operation.");
            logger.error(CommandLogger.errorMessage(e));

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public void printUsage(IgniteLogger logger) {
        usage(baseCmd, Collections.emptyList(), logger);
    }

    /** */
    private void usage(
        org.apache.ignite.internal.management.api.Command<?, ?, ?> cmd,
        List<CommandsRegistry> parents,
        IgniteLogger logger
    ) {
        boolean skip = (cmd instanceof CommandsRegistry) && !((CommandsRegistry)cmd).canBeExecuted();

        if (!skip) {
            logger.info("");

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

                visitCommandParams(cmd.args(), lenCalc, lenCalc, (optional, flds) -> flds.forEach(lenCalc));

                Consumer<Field> printer = fld -> {
                    BiConsumer<String, String> logParam = (name, description) -> logger.info(
                        DOUBLE_INDENT + INDENT + U.extendToLen(name, maxParamLen.get()) + "  - " + description + "."
                    );

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

                visitCommandParams(cmd.args(), printer, printer, (optional, flds) -> flds.forEach(printer));
            }
        }

        if (cmd instanceof CommandsRegistry) {
            List<CommandsRegistry> parents0 = new ArrayList<>(parents);

            parents0.add((CommandsRegistry)cmd);

            ((CommandsRegistry)cmd).forEach(cmd0 -> usage(cmd0.getValue(), parents0, logger));
        }
    }

    /** */
    private void printExample(
        org.apache.ignite.internal.management.api.Command<?, ?, ?> cmd,
        List<CommandsRegistry> parents,
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

            String cmdName = commandName(cmd0.getClass(), CMD_WORDS_DELIM);

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
    @Override public A arg() {
        return parsed.get2();
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return CommandUtils.commandName(baseCmd.getClass(), CMD_WORDS_DELIM).toUpperCase();
    }

    /** {@inheritDoc} */
    @Override public IgniteEx grid() {
        return null;
    }
}
