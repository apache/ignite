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

package org.apache.ignite.spi.communication.tcp;

import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.internal.ConnectionClientPool;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_FORCIBLE_NODE_KILL;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Tests that freezing due to JVM STW client will be failed if connection can't be established.
 */
@WithSystemProperty(key=IGNITE_ENABLE_FORCIBLE_NODE_KILL, value = "true")
@SuppressWarnings("ThrowableNotThrown")
public class TcpCommunicationSpiFreezingClientTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setFailureDetectionTimeout(getTestTimeout());
        cfg.setClientFailureDetectionTimeout(getTestTimeout());

        TcpCommunicationSpi spi = new TcpCommunicationSpi();

        spi.setConnectTimeout(1000);
        spi.setMaxConnectTimeout(1000);
        spi.setIdleConnectionTimeout(100);
        spi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(spi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** @throws Exception If failed. */
    @Test
    public void testFreezingClient() throws Exception {
        Ignite srv = startGrid(0);
        Ignite client = startClientGrid("client");

        // Close communication connections by idle and trigger STW on the client.
        srv.compute(srv.cluster().forNode(client.cluster().localNode())).withNoFailover().runAsync(() -> {
            waitConnectionsClosed(Ignition.localIgnite());

            triggerSTW();
        });

        // Make sure connections closed on the server.
        waitConnectionsClosed(srv);

        // Make sure that the client is freezed by STW.
        doSleep(1000);

        // Open new connection to the freezed client.
        assertThrowsWithCause(() -> {
            srv.compute(srv.cluster().forNode(client.cluster().localNode())).withNoFailover().run(() -> {
                // No-op.
            });
        }, ClusterTopologyException.class);

        assertEquals(1, srv.cluster().nodes().size());
    }

    /** Triggers STW. */
    private void triggerSTW() {
        long end = System.currentTimeMillis() + getTestTimeout();

        while (!Thread.interrupted() && (System.currentTimeMillis() < end)) {
            IgniteInternalFuture<?> fut = GridTestUtils.runAsync(this::simulateLoad);

            while (!fut.isDone())
                GridTestUtils.runGC();
        }
    }

    /** Simulate load without safepoints to block GC. */
    public double simulateLoad() {
        double d = 0;

        for (int i = 0; i < Integer.MAX_VALUE; i++)
            d += Math.log(Math.PI * i);

        return d;
    }

    /** Waits for all communication connections closed by idle. */
    private void waitConnectionsClosed(Ignite node) {
        TcpCommunicationSpi spi = (TcpCommunicationSpi)node.configuration().getCommunicationSpi();
        ConnectionClientPool pool = U.field(spi, "clientPool");
        ConcurrentMap<UUID, GridCommunicationClient[]> clientsMap = U.field(pool, "clients");

        try {
            GridTestUtils.waitForCondition(() -> {
                for (GridCommunicationClient[] clients : clientsMap.values()) {
                    if (clients == null)
                        continue;

                    for (GridCommunicationClient client : clients) {
                        if (client != null)
                            return false;
                    }
                }

                return true;
            }, getTestTimeout());
        }
        catch (IgniteInterruptedCheckedException e) {
            throw F.wrap(e);
        }
    }
}
