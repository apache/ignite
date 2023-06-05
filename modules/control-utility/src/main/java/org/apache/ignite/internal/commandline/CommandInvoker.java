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

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientBeforeNodeStart;
import org.apache.ignite.internal.client.GridClientCompute;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientDisconnectedException;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
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
import org.apache.ignite.internal.management.api.PreparableCommand;
import org.apache.ignite.internal.management.api.WithCliConfirmParameter;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.lang.IgniteBiTuple;

import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.commandline.CommandHandler.DFLT_HOST;
import static org.apache.ignite.internal.commandline.CommandHandler.UTILITY_NAME;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_AUTO_CONFIRMATION;
import static org.apache.ignite.internal.management.api.CommandUtils.CMD_WORDS_DELIM;
import static org.apache.ignite.internal.management.api.CommandUtils.DOUBLE_INDENT;
import static org.apache.ignite.internal.management.api.CommandUtils.INDENT;
import static org.apache.ignite.internal.management.api.CommandUtils.PARAMETER_PREFIX;
import static org.apache.ignite.internal.management.api.CommandUtils.PARAM_WORDS_DELIM;
import static org.apache.ignite.internal.management.api.CommandUtils.parameterExample;
import static org.apache.ignite.internal.management.api.CommandUtils.toFormattedCommandName;
import static org.apache.ignite.internal.management.api.CommandUtils.valueExample;
import static org.apache.ignite.internal.management.api.CommandUtils.visitCommandParams;

/**
 * Adapter of new management API command for legacy {@code control.sh} execution flow.
 */
public class CommandInvoker<A extends IgniteDataTransferObject> {
    /** Command to execute. */
    private final Command<A, ?> cmd;

    /** Parsed argument. */
    private final A arg;

    /** Client configuration. */
    private GridClientConfiguration clientCfg;

    /** @param cmd Command to execute. */
    public CommandInvoker(Command<A, ?> cmd, A arg, GridClientConfiguration clientCfg) {
        this.cmd = cmd;
        this.arg = arg;
        this.clientCfg = clientCfg;
    }

    /**
     * Actual command execution with verbose mode if needed.
     * Implement it if your command supports verbose mode.
     *
     * @param logger Logger to use.
     * @param verbose Use verbose mode or not
     * @return Result of operation (mostly usable for tests).
     * @throws Exception If error occur.
     */
    public <R> R invoke(IgniteLogger logger, boolean verbose) throws Exception {
        try (GridClient client = startClient(clientCfg)) {
            String deprecationMsg = cmd.deprecationMessage(arg);

            if (deprecationMsg != null)
                logger.warning(deprecationMsg);

            R res;

            if (cmd instanceof LocalCommand)
                res = ((LocalCommand<A, R>)cmd).execute(client, arg, logger::info);
            else if (cmd instanceof ComputeCommand) {
                GridClientCompute compute = client.compute();

                Map<UUID, GridClientNode> nodes = compute.nodes().stream()
                    .collect(toMap(GridClientNode::nodeId, n -> n));

                ComputeCommand<A, R> cmd = (ComputeCommand<A, R>)this.cmd;

                Collection<UUID> cmdNodes = cmd.nodes(
                    nodes.values()
                        .stream()
                        .collect(toMap(GridClientNode::nodeId, n -> new T3<>(n.isClient(), n.consistentId(), n.order()))),
                    arg
                );

                if (cmdNodes == null)
                    cmdNodes = singleton(defaultNode(client, clientCfg).nodeId());

                for (UUID id : cmdNodes) {
                    if (!nodes.containsKey(id))
                        throw new IllegalArgumentException("Node with id=" + id + " not found.");
                }

                Collection<GridClientNode> connectable = F.viewReadOnly(
                    cmdNodes,
                    nodes::get,
                    id -> nodes.get(id).connectable()
                );

                if (!F.isEmpty(connectable))
                    compute = compute.projection(connectable);

                res = compute.execute(cmd.taskClass().getName(), new VisorTaskArgument<>(cmdNodes, arg, false));

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
    }

    /** */
    public boolean prepare(IgniteLogger logger) throws Exception {
        if (!(cmd instanceof PreparableCommand))
            return true;

        try (GridClient client = startClient(clientCfg)) {
            return ((PreparableCommand<A, ?>)cmd).prepare(client, arg, logger::info);
        }
    }

    /**
     * @return Message text to show user for. If null it means that confirmantion is not needed.
     * @throws Exception If error occur.
     */
    public String confirmationPrompt() {
        return cmd.confirmationPrompt(arg);
    }

    /** */
    public static void usage(Command<?, ?> cmd, IgniteLogger logger) {
        usage(cmd, Collections.emptyList(), logger);
    }

    /**
     * Generates usage for base command and all of its children, if any.
     *
     * @param cmd Base command.
     * @param parents Collection of parent commands.
     * @param logger Logger to print help to.
     */
    private static void usage(Command<?, ?> cmd, List<Command<?, ?>> parents, IgniteLogger logger) {
        if (cmd instanceof LocalCommand
            || cmd instanceof ComputeCommand
            || cmd instanceof HelpCommand
            || cmd instanceof BeforeNodeStartCommand) {
            logger.info("");

            if (cmd.experimental())
                logger.info(INDENT + "[EXPERIMENTAL]");

            printExample(cmd, parents, logger);

            if (CommandUtils.hasDescribedParameters(cmd)) {
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
    private static void printExample(Command<?, ?> cmd, List<Command<?, ?>> parents, IgniteLogger logger) {
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

    /** */
    public <R> R invokeBeforeNodeStart(IgniteLogger logger) throws Exception {
        try (GridClientBeforeNodeStart client = startClientBeforeNodeStart(clientCfg)) {
            return ((BeforeNodeStartCommand<A, R>)cmd).execute(client, arg, logger::info);
        }
        catch (GridClientDisconnectedException e) {
            throw new GridClientException(e.getCause());
        }
    }

    /**
     * Method to create thin client for communication with cluster.
     *
     * @param clientCfg Thin client configuration.
     * @return Grid thin client instance which is already connected to cluster.
     * @throws Exception If error occur.
     */
    private static GridClient startClient(GridClientConfiguration clientCfg) throws Exception {
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
    private static GridClientBeforeNodeStart startClientBeforeNodeStart(
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

    /** */
    public static GridClientNode defaultNode(GridClient client, GridClientConfiguration clientCfg) throws GridClientException {
        GridClientNode node;

        // Prefer node from connect string.
        final String cfgAddr = clientCfg.getServers().iterator().next();

        String[] parts = cfgAddr.split(":");

        if (DFLT_HOST.equals(parts[0])) {
            InetAddress addr;

            try {
                addr = IgniteUtils.getLocalHost();
            }
            catch (IOException e) {
                throw new GridClientException("Can't get localhost name.", e);
            }

            if (addr.isLoopbackAddress())
                throw new GridClientException("Can't find localhost name.");

            String origAddr = addr.getHostName() + ":" + parts[1];

            node = listHosts(client).filter(tuple -> origAddr.equals(tuple.get2())).findFirst().map(IgniteBiTuple::get1).orElse(null);

            if (node == null)
                node = listHostsByClientNode(client).filter(tuple -> tuple.get2().size() == 1 && cfgAddr.equals(tuple.get2().get(0))).
                    findFirst().map(IgniteBiTuple::get1).orElse(null);
        }
        else
            node = listHosts(client).filter(tuple -> cfgAddr.equals(tuple.get2())).findFirst().map(IgniteBiTuple::get1).orElse(null);

        // Otherwise choose random node.
        if (node == null)
            node = balancedNode(client.compute());

        return node;
    }

    /**
     * @param client Client.
     * @return List of hosts.
     */
    private static Stream<IgniteBiTuple<GridClientNode, String>> listHosts(GridClient client) throws GridClientException {
        return client.compute()
            .nodes(GridClientNode::connectable)
            .stream()
            .flatMap(node -> Stream.concat(
                node.tcpAddresses() == null ? Stream.empty() : node.tcpAddresses().stream(),
                node.tcpHostNames() == null ? Stream.empty() : node.tcpHostNames().stream()
            ).map(addr -> new IgniteBiTuple<>(node, addr + ":" + node.tcpPort())));
    }

    /**
     * @param client Client.
     * @return List of hosts.
     */
    private static Stream<IgniteBiTuple<GridClientNode, List<String>>> listHostsByClientNode(
        GridClient client
    ) throws GridClientException {
        return client.compute().nodes(GridClientNode::connectable).stream()
            .map(
                node -> new IgniteBiTuple<>(
                    node,
                    Stream.concat(
                            node.tcpAddresses() == null ? Stream.empty() : node.tcpAddresses().stream(),
                            node.tcpHostNames() == null ? Stream.empty() : node.tcpHostNames().stream()
                        )
                        .map(addr -> addr + ":" + node.tcpPort()).collect(Collectors.toList())
                )
            );
    }

    /**
     * @param compute instance
     * @return balanced node
     */
    private static GridClientNode balancedNode(GridClientCompute compute) throws GridClientException {
        Collection<GridClientNode> nodes = compute.nodes(GridClientNode::connectable);

        if (F.isEmpty(nodes))
            throw new GridClientDisconnectedException("Connectable node not found", null);

        return compute.balancer().balancedNode(nodes);
    }

    /** */
    public void clientConfiguration(GridClientConfiguration clientCfg) {
        this.clientCfg = clientCfg;
    }

    /** */
    public GridClientConfiguration clientConfiguration() {
        return clientCfg;
    }
}
