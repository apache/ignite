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

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientCompute;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientDisconnectedException;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Visor task executor.
 */
public class TaskExecutor {
    /** */
    public static final String DFLT_HOST = "127.0.0.1";

    /** */
    public static final String DFLT_PORT = "11211";

    /** Broadcast uuid. */
    public static final UUID BROADCAST_UUID = UUID.randomUUID();

    /**
     * @param client Client
     * @param taskClsName Task class name.
     * @param taskArgs Task args.
     * @param nodeId Node ID to execute task at (if null, random node will be chosen by balancer).
     * @param clientCfg
     * @return Task result.
     * @throws GridClientException If failed to execute task.
     */
    public static <R> R executeTaskByNameOnNode(
        GridClient client,
        String taskClsName,
        Object taskArgs,
        UUID nodeId,
        GridClientConfiguration clientCfg
    ) throws GridClientException {
        GridClientCompute compute = client.compute();

        if (nodeId == BROADCAST_UUID) {
            Collection<GridClientNode> nodes = compute.nodes(GridClientNode::connectable);

            if (F.isEmpty(nodes))
                throw new GridClientDisconnectedException("Connectable nodes not found", null);

            List<UUID> nodeIds = nodes.stream()
                .map(GridClientNode::nodeId)
                .collect(Collectors.toList());

            return client.compute().execute(taskClsName, new VisorTaskArgument<>(nodeIds, taskArgs, false));
        }

        GridClientNode node = null;

        if (nodeId == null) {
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
                node = getBalancedNode(compute);
        }
        else {
            for (GridClientNode n : compute.nodes()) {
                if (n.connectable() && nodeId.equals(n.nodeId())) {
                    node = n;

                    break;
                }
            }

            if (node == null)
                throw new IllegalArgumentException("Node with id=" + nodeId + " not found");
        }

        return compute.projection(node).execute(taskClsName, new VisorTaskArgument<>(node.nodeId(), taskArgs, false));
    }

    /**
     * @param client Client.
     * @param taskCls Task class.
     * @param taskArgs Task arguments.
     * @param clientCfg Client configuration.
     * @return Task result.
     * @throws GridClientException If failed to execute task.
     */
    public static <R> R executeTask(
        GridClient client,
        Class<? extends ComputeTask<?, R>> taskCls,
        Object taskArgs,
        GridClientConfiguration clientCfg
    ) throws GridClientException {
        return executeTaskByNameOnNode(client, taskCls.getName(), taskArgs, null, clientCfg);
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
    private static GridClientNode getBalancedNode(GridClientCompute compute) throws GridClientException {
        Collection<GridClientNode> nodes = compute.nodes(GridClientNode::connectable);

        if (F.isEmpty(nodes))
            throw new GridClientDisconnectedException("Connectable node not found", null);

        return compute.balancer().balancedNode(nodes);
    }
}
