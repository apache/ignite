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

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_FORCIBLE_NODE_KILL;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Tests that freezing due to JVM STW client will be failed if connection can't be established.
 */
@WithSystemProperty(key = IGNITE_ENABLE_FORCIBLE_NODE_KILL, value = "true")
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
    @SuppressWarnings("ThrowableNotThrown")
    public void testFreezingClient() throws Exception {
        Assume.assumeTrue("The test reqires the 'kill' command.", U.isUnix() || U.isMacOs());

        Ignite srv = startGrid(0);
        IgniteProcessProxy client = (IgniteProcessProxy)startClientGrid("client");

        // Close communication connections by idle.
        waitConnectionsClosed(srv);

        // Simulate freeze/STW on the client.
        client.getProcess().suspend();

        // Open new communication connection to the freezing client.
        GridTestUtils.assertThrowsWithCause(
            () -> srv.compute(srv.cluster().forClients()).withNoFailover().run(() -> {}),
            ClusterTopologyException.class);

        assertEquals(1, srv.cluster().nodes().size());
    }

    /** Waits for all communication connections closed by idle. */
    private void waitConnectionsClosed(Ignite node) {
        TcpCommunicationSpi spi = (TcpCommunicationSpi)node.configuration().getCommunicationSpi();
        Map<UUID, GridCommunicationClient[]> clientsMap = GridTestUtils.getFieldValue(spi, "clientPool", "clients");

        try {
            assertTrue(waitForCondition(() -> {
                for (GridCommunicationClient[] clients : clientsMap.values()) {
                    if (clients == null)
                        continue;

                    for (GridCommunicationClient client : clients) {
                        if (client != null)
                            return false;
                    }
                }

                return true;
            }, getTestTimeout()));
        }
        catch (IgniteInterruptedCheckedException e) {
            throw U.convertException(e);
        }
    }
}
