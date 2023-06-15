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
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientBeforeNodeStart;
import org.apache.ignite.internal.client.GridClientCompute;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientDisconnectedException;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.AbstractCommandInvoker;
import org.apache.ignite.internal.management.api.BeforeNodeStartCommand;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.management.api.PreparableCommand;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.lang.IgniteBiTuple;

import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.commandline.CommandHandler.DFLT_HOST;

/**
 * Adapter of new management API command for legacy {@code control.sh} execution flow.
 */
public class CommandInvoker<A extends IgniteDataTransferObject> extends AbstractCommandInvoker<A> {

    /** Client configuration. */
    private GridClientConfiguration clientCfg;

    /** Client. */
    private GridClient client;

    /** @param cmd Command to execute. */
    public CommandInvoker(Command<A, ?> cmd, A arg, GridClientConfiguration clientCfg) {
        super(cmd, arg);
        this.clientCfg = clientCfg;
    }

    /** */
    public boolean prepare(Consumer<String> printer) throws Exception {
        if (!(cmd instanceof PreparableCommand))
            return true;

        return ((PreparableCommand<A, ?>)cmd).prepare(client(), arg, printer);
    }

    /**
     * @return Message text to show user for. {@code null} means that confirmantion is not required.
     */
    public String confirmationPrompt() {
        return cmd.confirmationPrompt(arg);
    }

    /** */
    public <R> R invokeBeforeNodeStart(Consumer<String> printer) throws Exception {
        try (GridClientBeforeNodeStart client = startClientBeforeNodeStart(clientCfg)) {
            return ((BeforeNodeStartCommand<A, R>)cmd).execute(client, arg, printer);
        }
        catch (GridClientDisconnectedException e) {
            throw new GridClientException(e.getCause());
        }
    }

    /** {@inheritDoc} */
    @Override protected Map<UUID, GridClientNode> nodes() throws GridClientException {
        return client().compute().nodes().stream()
            .collect(toMap(GridClientNode::nodeId, n -> n));
    }

    /** {@inheritDoc} */
    @Override protected <R> R execute(ComputeCommand<A, R> cmd, A arg, Collection<UUID> nodes) throws GridClientException {
        GridClientCompute compute = client().compute();

        Collection<GridClientNode> connectable = compute.nodes().stream()
            .filter(n -> nodes.contains(n.nodeId()))
            .filter(GridClientNode::connectable)
            .collect(Collectors.toList());

        if (!F.isEmpty(nodes))
            compute = compute.projection(connectable);

        return compute.execute(cmd.taskClass().getName(), new VisorTaskArgument<>(nodes, arg, false));
    }

    /** {@inheritDoc} */
    @Override protected GridClientNode defaultNode() throws GridClientException {
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

            node = listHosts(client()).filter(tuple -> origAddr.equals(tuple.get2())).findFirst().map(IgniteBiTuple::get1).orElse(null);

            if (node == null)
                node = listHostsByClientNode(client()).filter(tuple -> tuple.get2().size() == 1 && cfgAddr.equals(tuple.get2().get(0))).
                    findFirst().map(IgniteBiTuple::get1).orElse(null);
        }
        else
            node = listHosts(client()).filter(tuple -> cfgAddr.equals(tuple.get2())).findFirst().map(IgniteBiTuple::get1).orElse(null);

        // Otherwise choose random node.
        if (node == null)
            node = balancedNode(client().compute());

        return node;
    }

    /** */
    @Override protected GridClient client() throws GridClientException {
        if (client != null && client.connected())
            return client;

        client = GridClientFactory.start(clientCfg);

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
    public void clientConfiguration(GridClientConfiguration clientCfg) {
        this.clientCfg = clientCfg;
    }

    /** */
    public GridClientConfiguration clientConfiguration() {
        return clientCfg;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        if (client != null)
            client.close();
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
}
