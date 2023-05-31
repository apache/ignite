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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientCompute;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientDisconnectedException;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import static org.apache.ignite.internal.commandline.CommandHandler.DFLT_HOST;

/**
 * Visor task executor.
 */
public class TaskExecutor {
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
            node = getBalancedNode(client.compute());

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
    public static GridClientNode getBalancedNode(GridClientCompute compute) throws GridClientException {
        Collection<GridClientNode> nodes = compute.nodes(GridClientNode::connectable);

        if (F.isEmpty(nodes))
            throw new GridClientDisconnectedException("Connectable node not found", null);

        return compute.balancer().balancedNode(nodes);
    }
}
