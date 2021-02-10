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

package org.apache.ignite.client;

import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.client.thin.AbstractThinClientTest;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * Checks that connection with starting node will be established correctly.
 */
public class ConnectToStartingNodeTest extends AbstractThinClientTest {
    /** Barrier to suspend discovery SPI start. */
    private final CyclicBarrier barrier = new CyclicBarrier(2);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setDiscoverySpi(new TcpDiscoverySpi() {
            @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
                try {
                    barrier.await();
                }
                catch (Exception ignore) {
                    // No-op.
                }

                super.spiStart(igniteInstanceName);

                try {
                    barrier.await();
                }
                catch (Exception ignore) {
                    // No-op.
                }
            }
        });
    }

    /**
     * Test that client can't connect to server before discovery SPI start.
     */
    @Test
    public void testClientConnectBeforeDiscoveryStart() throws Exception {
        IgniteInternalFuture<Ignite> futStartGrid = GridTestUtils.runAsync((Callable<Ignite>)this::startGrid);

        barrier.await();

        IgniteInternalFuture<IgniteClient> futStartClient = GridTestUtils.runAsync(
            () -> startClient(grid()));

        // Server doesn't accept connection before discovery SPI started.
        assertFalse(GridTestUtils.waitForCondition(futStartClient::isDone, 500L));

        barrier.await();

        futStartGrid.get();

        // Server accept connection after discovery SPI started.
        assertTrue(GridTestUtils.waitForCondition(futStartClient::isDone, 500L));
    }
}
