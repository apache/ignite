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
import java.net.InetAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
import org.apache.ignite.internal.management.api.BeforeNodeStartCommand;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.management.api.LocalCommand;
import org.apache.ignite.internal.management.api.PreparableCommand;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.lang.IgniteBiTuple;

import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.commandline.CommandHandler.DFLT_HOST;

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
