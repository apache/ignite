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

package org.apache.ignite.internal.client.thin;

import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.processors.odbc.ClientListenerProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Abstract thin client test.
 */
public abstract class AbstractThinClientTest extends GridCommonAbstractTest {
    /**
     * Gets default client configuration.
     */
    protected ClientConfiguration getClientConfiguration() {
        return new ClientConfiguration();
    }

    /**
     * Return thin client port for given node.
     *
     * @param node Node.
     */
    protected int clientPort(ClusterNode node) {
        return node.attribute(ClientListenerProcessor.CLIENT_LISTENER_PORT);
    }

    /**
     * Return host for given node.
     *
     * @param node Node.
     */
    protected String clientHost(ClusterNode node) {
        return F.first(node.addresses());
    }

    /**
     * Start thin client with configured endpoints to specified nodes.
     *
     * @param nodes Nodes to connect.
     * @return Thin client.
     */
    protected IgniteClient startClient(ClusterNode... nodes) {
        String[] addrs = new String[nodes.length];

        for (int i = 0; i < nodes.length; i++) {
            ClusterNode node = nodes[i];

            addrs[i] = clientHost(node) + ":" + clientPort(node);
        }

        return Ignition.startClient(getClientConfiguration().setAddresses(addrs));
    }

    /**
     * Start thin client with configured endpoints to specified ignite instances.
     *
     * @param ignites Ignite instances to connect.
     * @return Thin client.
     */
    protected IgniteClient startClient(Ignite... ignites) {
        return startClient(Arrays.stream(ignites).map(ignite -> ignite.cluster().localNode()).toArray(ClusterNode[]::new));
    }

    /**
     * Start thin client with configured endpoints to specified ignite instance indexes.
     *
     * @param igniteIdxs Ignite instance indexes to connect.
     * @return Thin client.
     */
    protected IgniteClient startClient(int... igniteIdxs) {
        return startClient(Arrays.stream(igniteIdxs).mapToObj(igniteIdx -> grid(igniteIdx).cluster().localNode())
            .toArray(ClusterNode[]::new));
    }
}
