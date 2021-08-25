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

package org.apache.ignite.internal.processors.security.client;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.security.GridSecurityProcessor;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.TestReconnectSecurityPluginProvider;
import org.apache.ignite.spi.discovery.tcp.TestReconnectProcessor;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_RECONNECTED;

/**
 * Tests client node can reconnect cluster based only on node id which changed on disconnect.
 * @see TcpDiscoveryNode#onClientDisconnected(UUID)
 */
public class ClientReconnectTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setPluginProviders(new TestReconnectSecurityPluginProvider() {
            @Override protected GridSecurityProcessor securityProcessor(GridKernalContext ctx) {
                return new TestReconnectProcessor(ctx) {
                    @Override public SecurityContext securityContext(UUID subjId) {
                        if (ctx.localNodeId().equals(subjId))
                            return ctx.security().securityContext();

                        fail("Unexpected subjId[subjId=" + subjId + ", localNodeId=" + ctx.localNodeId() + ']');

                        return null;
                    }

                    @Override public SecurityContext authenticateNode(ClusterNode node, SecurityCredentials cred) {
                        return new TestSecurityContext(new TestSecuritySubject(node.id()));
                    }
                };
            }
        });
    }

    /** */
    @Test
    public void testClientNodeReconnected() throws Exception {
        startGrids(2);

        IgniteEx cli = startClientGrid(2);

        CountDownLatch latch = new CountDownLatch(1);

        cli.events().localListen(evt -> {
            latch.countDown();

            return true;
        }, EVT_CLIENT_NODE_RECONNECTED);

        DiscoverySpi discoverySpi = ignite(0).configuration().getDiscoverySpi();

        discoverySpi.failNode(nodeId(2), null);

        assertTrue(latch.await(getTestTimeout(), TimeUnit.MILLISECONDS));
    }
}
