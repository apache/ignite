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
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.GridKernalState.STARTED;
import static org.apache.ignite.internal.IgnitionEx.gridx;
import static org.apache.ignite.internal.processors.odbc.ClientListenerNioListener.RECOVERY_ATTR;
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

            recoveryLatch.countDown();

            assertTrue(waitForCondition(() -> 2 == client.cluster().nodes().size(), getTestTimeout()));
        }
    }

    /** */
    @Test
    public void testConnectToNodeInRecovery() throws Exception {
        startGridInRecovery(0);

        try (IgniteClient ignored1 = startClientRecoveryEnabled(0)) {
            try (IgniteClient ignored2 = startClientRecoveryEnabled(0)) {
                List<String> conns = gridx(getTestIgniteInstanceName(0)).context().clientListener().mxBean().getConnections();

                assertEquals(2, conns.size());
            }
        }
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

        return getClientConfiguration(nodes);
    }

    /** {@inheritDoc} */
    @Override protected IgniteClient startClient(int... igniteIdxs) {
        return Ignition.startClient(getClientConfiguration(igniteIdxs));
    }

    /** */
    private IgniteClient startClientRecoveryEnabled(int... igniteIdxs) {
        return Ignition.startClient(getClientConfiguration(igniteIdxs)
            .setUserAttributes(F.asMap(RECOVERY_ATTR, Boolean.TRUE.toString())));
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
