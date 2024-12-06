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
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCheckedException;
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
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityCredentialsBasicProvider;
import org.apache.ignite.plugin.security.SecurityCredentialsProvider;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.commandline.CommandHandler.DFLT_HOST;

/**
 * Adapter of new management API command for legacy {@code control.sh} execution flow.
 */
public class CliGridClientCommandInvoker<A extends IgniteDataTransferObject> extends AbstractCliCommandInvoker<A> {
    /** */
    private final GridClientConfiguration clientCfg;

    /** Client. */
    private final GridClient client;

    /** @param cmd Command to execute. */
    public CliGridClientCommandInvoker(
        Command<A, ?> cmd,
        ConnectionAndSslParameters<A> args,
        Function<String, char[]> pwdReader
    ) throws Exception {
        super(cmd, args, pwdReader);

        clientCfg = clientConfiguration(args);

        if (args.command() instanceof BeforeNodeStartCommand) {
            client = null;

            return;
        }

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
    }

    /** */
    /** {@inheritDoc} */
    @Override public <R> R invokeBeforeNodeStart(Consumer<String> printer) throws Exception {
        try (GridClientBeforeNodeStart client = startClientBeforeNodeStart(clientCfg)) {
            return ((BeforeNodeStartCommand<A, R>)cmd).execute(client.beforeStartState(), arg, printer);
        }
        catch (GridClientDisconnectedException e) {
            throw new GridClientException(e.getCause());
        }
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

            node = listHosts(gridClient()).filter(tuple -> origAddr.equals(tuple.get2())).findFirst().map(IgniteBiTuple::get1).orElse(null);

            if (node == null)
                node = listHostsByClientNode(gridClient()).filter(tuple -> tuple.get2().size() == 1 && cfgAddr.equals(tuple.get2().get(0))).
                    findFirst().map(IgniteBiTuple::get1).orElse(null);
        }
        else
            node = listHosts(gridClient()).filter(tuple -> cfgAddr.equals(tuple.get2())).findFirst().map(IgniteBiTuple::get1).orElse(null);

        // Otherwise choose random node.
        if (node == null)
            node = balancedNode(gridClient().compute());

        return node;
    }

    /** {@inheritDoc} */
    @Override protected GridClient gridClient() {
        return client;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        IgniteUtils.closeQuiet(client);
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

    /**
     * @param args Common arguments.
     * @return Thin client configuration to connect to cluster.
     * @throws IgniteCheckedException If error occur.
     */
    private GridClientConfiguration clientConfiguration(
        ConnectionAndSslParameters args
    ) throws IgniteCheckedException {
        GridClientConfiguration clientCfg = new GridClientConfiguration();

        clientCfg.setPingInterval(args.pingInterval());

        clientCfg.setPingTimeout(args.pingTimeout());

        clientCfg.setServers(Collections.singletonList(args.host() + ":" + args.port()));

        if (!F.isEmpty(args.userName()))
            clientCfg.setSecurityCredentialsProvider(getSecurityCredentialsProvider(args.userName(), args.password(), clientCfg));

        if (!F.isEmpty(args.sslKeyStorePath()) || !F.isEmpty(args.sslFactoryConfigPath()))
            clientCfg.setSslContextFactory(createSslSupportFactory(args));

        return clientCfg;
    }

    /**
     * @param userName User name for authorization.
     * @param password Password for authorization.
     * @param clientCfg Thin client configuration to connect to cluster.
     * @return Security credentials provider with usage of given user name and password.
     * @throws IgniteCheckedException If error occur.
     */
    @NotNull private SecurityCredentialsProvider getSecurityCredentialsProvider(
        String userName,
        String password,
        GridClientConfiguration clientCfg
    ) throws IgniteCheckedException {
        SecurityCredentialsProvider securityCredential = clientCfg.getSecurityCredentialsProvider();

        if (securityCredential == null)
            return new SecurityCredentialsBasicProvider(new SecurityCredentials(userName, password));

        final SecurityCredentials credential = securityCredential.credentials();
        credential.setLogin(userName);
        credential.setPassword(password);

        return securityCredential;
    }
}
