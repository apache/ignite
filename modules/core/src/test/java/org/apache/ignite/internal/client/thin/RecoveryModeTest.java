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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.spi.systemview.view.ClientConnectionView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.GridKernalState.STARTED;
import static org.apache.ignite.internal.IgnitionEx.gridx;
import static org.apache.ignite.internal.processors.odbc.ClientListenerNioListener.MANAGEMENT_CLIENT_ATTR;
import static org.apache.ignite.internal.processors.odbc.ClientListenerNioListener.MANAGEMENT_CONNECTION_SHIFTED_ID;
import static org.apache.ignite.internal.processors.odbc.ClientListenerProcessor.CLI_CONN_VIEW;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.stopThreads;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Tests connection to a node in recovery mode.
 */
public class RecoveryModeTest extends AbstractThinClientTest {
    /** */
    private final CountDownLatch recoveryLatch = new CountDownLatch(1);

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopThreads(log);
        stopAllGrids();
    }

    /** */
    @Test
    public void testNodeInRecovery() throws Exception {
        startGridInRecovery(0);

        assertThrowsWithCause(() -> startClient(0), ClientConnectionException.class);
    }

    /** */
    @Test
    public void testFirstNodeInRecovery() throws Exception {
        startGridInRecovery(0);
        startGrid(1);

        try (IgniteClient client = startClient(0, 1)) {
            assertEquals(1, client.cluster().nodes().size());

            assertEquals(0, clientConnections(0).size());

            List<Long> conns1 = clientConnections(1);

            assertEquals(1, conns1.size());
            assertFalse(managementClientConnection(conns1.iterator().next()));

            recoveryLatch.countDown();

            assertTrue(waitForCondition(() -> 2 == client.cluster().nodes().size(), getTestTimeout()));

            assertTrue(waitForCondition(() -> {
                List<Long> conn0 = clientConnections(0);

                return conn0.size() == 1 && conn0.stream().allMatch(id -> id != -1); // Connection completed.
            }, getTestTimeout()));

            List<Long> conn0 = clientConnections(0);

            assertEquals(1, conn0.size());
            assertFalse(managementClientConnection(conn0.iterator().next()));

            assertEquals(conns1, clientConnections(1));
        }
    }

    /** */
    @Test
    public void testConnectToNodeInRecovery() throws Exception {
        startGridInRecovery(0);

        try (IgniteClient ignored1 = startClientRecoveryEnabled(0)) {
            try (IgniteClient ignored2 = startClientRecoveryEnabled(0)) {
                List<Long> conns = clientConnections(0);

                assertEquals(2, conns.stream().distinct().count());
                assertTrue(conns.stream().allMatch(this::managementClientConnection));
            }
        }
    }

    /** */
    private List<Long> clientConnections(int nodeIdx) {
        SystemView<ClientConnectionView> conns = gridx(getTestIgniteInstanceName(nodeIdx)).context()
            .systemView().view(CLI_CONN_VIEW);

        return StreamSupport.stream(conns.spliterator(), false)
            .map(ClientConnectionView::connectionId).collect(Collectors.toList());
    }

    /** */
    private boolean managementClientConnection(long connIdx) {
        return connIdx >> 32 == MANAGEMENT_CONNECTION_SHIFTED_ID;
    }

    /** */
    private ClientConfiguration getClientConfiguration(int... igniteIdxs) {
        ClusterNode[] nodes = Arrays.stream(igniteIdxs).mapToObj(igniteIdx -> {
            IgniteKernal srv = gridx(getTestIgniteInstanceName(igniteIdx));

            if (srv.context().gateway().getState() == STARTED)
                return srv.cluster().localNode();

            GridTestNode node = new GridTestNode(srv.localNodeId());

            node.setAttributes(gridx(getTestIgniteInstanceName(igniteIdx)).context().nodeAttributes());
            node.setPhysicalAddress("127.0.0.1");

            return node;
        }).toArray(ClusterNode[]::new);

        return getClientConfiguration(nodes)
            .setAutoBinaryConfigurationEnabled(false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteClient startClient(int... igniteIdxs) {
        return Ignition.startClient(getClientConfiguration(igniteIdxs));
    }

    /** */
    private IgniteClient startClientRecoveryEnabled(int... igniteIdxs) {
        return Ignition.startClient(getClientConfiguration(igniteIdxs)
            .setUserAttributes(F.asMap(MANAGEMENT_CLIENT_ATTR, Boolean.TRUE.toString())));
    }

    /** Starts grid in recovery mode. */
    protected void startGridInRecovery(int idx) throws Exception {
        CountDownLatch kernalStart = new CountDownLatch(1);

        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(idx))
            .setPluginProviders(new AbstractTestPluginProvider() {
                @Override public String name() {
                    return "recovery-test";
                }

                @Override public void start(PluginContext ctx) throws IgniteCheckedException {
                    super.start(ctx);

                    kernalStart.countDown();

                    try {
                        recoveryLatch.await();
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });

        GridTestUtils.runAsync(() -> startGrid(cfg));

        kernalStart.await();
    }
}
