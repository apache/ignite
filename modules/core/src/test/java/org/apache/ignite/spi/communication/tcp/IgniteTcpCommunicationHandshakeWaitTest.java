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

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddFinishedMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Testing {@link TcpCommunicationSpi} that will send the wait handshake message on received connections until SPI
 * context initialized.
 */
public class IgniteTcpCommunicationHandshakeWaitTest extends GridCommonAbstractTest {
    /** */
    private static final long COMMUNICATION_TIMEOUT = 1000;

    /** */
    private static final long DISCOVERY_MESSAGE_DELAY = 500;

    /** */
    private final AtomicBoolean slowNet = new AtomicBoolean();

    /** */
    private final CountDownLatch latch = new CountDownLatch(1);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(false);

        TcpDiscoverySpi discoSpi = new SlowTcpDiscoverySpi();

        discoSpi.setIpFinder(sharedStaticIpFinder);

        cfg.setDiscoverySpi(discoSpi);

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

        commSpi.setConnectTimeout(COMMUNICATION_TIMEOUT);
        commSpi.setMaxConnectTimeout(4 * COMMUNICATION_TIMEOUT);
        commSpi.setReconnectCount(3);

        cfg.setCommunicationSpi(commSpi);

        return cfg;
    }

    /**
     * Test that joining node will send the wait handshake message on received connections until SPI context
     * initialized.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testHandshakeOnNodeJoining() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_ENABLE_FORCIBLE_NODE_KILL, "true");

        IgniteEx ignite = startGrid("srv1");

        startGrid("srv2");

        slowNet.set(true);

        IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
            latch.await(expectedTimeout(), TimeUnit.MILLISECONDS);

            Collection<ClusterNode> nodes = ignite.context().discovery().aliveServerNodes();

            assertEquals(3, nodes.size());

            return ignite.context().io().sendIoTest(new ArrayList<>(nodes), null, true).get();
        });

        startGrid("srv3");

        fut.get();
    }

    /** */
    private long expectedTimeout() {
        long maxBackoffTimeout = COMMUNICATION_TIMEOUT;

        for (int i = 1; i < 3 && maxBackoffTimeout < 3 * COMMUNICATION_TIMEOUT; i++)
            maxBackoffTimeout += Math.min(2 * maxBackoffTimeout, 3 * COMMUNICATION_TIMEOUT);

        return maxBackoffTimeout;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        System.clearProperty(IgniteSystemProperties.IGNITE_ENABLE_FORCIBLE_NODE_KILL);
    }

    /** */
    private class SlowTcpDiscoverySpi extends TcpDiscoverySpi {
        /** {@inheritDoc} */
        @Override protected boolean ensured(TcpDiscoveryAbstractMessage msg) {
            if (slowNet.get() && msg instanceof TcpDiscoveryNodeAddFinishedMessage) {
                try {
                    if (igniteInstanceName.contains("srv2") && msg.verified())
                        latch.countDown();

                    U.sleep(DISCOVERY_MESSAGE_DELAY);
                }
                catch (IgniteInterruptedCheckedException e) {
                    throw new IgniteSpiException("Thread has been interrupted.", e);
                }
            }

            return super.ensured(msg);
        }
    }
}
