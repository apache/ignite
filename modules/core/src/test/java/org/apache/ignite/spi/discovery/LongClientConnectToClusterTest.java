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

package org.apache.ignite.spi.discovery;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddFinishedMessage;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Collections;

/**
 * Test client connects to two nodes cluster during time more than the {@link org.apache.ignite.configuration.IgniteConfiguration#clientFailureDetectionTimeout}.
 */
public class LongClientConnectToClusterTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        TcpDiscoverySpi discoSpi = getTestIgniteInstanceName(0).equals(igniteInstanceName)
            ? new DelayedTcpDiscoverySpi()
            : new TcpDiscoverySpi();

        return super.getConfiguration(igniteInstanceName)
            .setClientMode(igniteInstanceName.startsWith("client"))
            .setClientFailureDetectionTimeout(1_000)
            .setMetricsUpdateFrequency(500)
            .setDiscoverySpi(discoSpi
                .setReconnectCount(1)
                .setLocalAddress("127.0.0.1")
                .setIpFinder(new TcpDiscoveryVmIpFinder()
                    .setAddresses(Collections.singletonList(igniteInstanceName.startsWith("client")
                        ? "127.0.0.1:47501"
                        : "127.0.0.1:47500..47502"))));
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration optimize(IgniteConfiguration cfg) throws IgniteCheckedException {
        super.optimize(cfg);

        ((TcpDiscoverySpi)super.optimize(cfg).getDiscoverySpi()).setJoinTimeout(0);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Test method.
     * @throws Exception If failed.
     */
    public void testClientConnectToCluster() throws Exception {
        IgniteEx client = startGrid("client");

        assertTrue(client.localNode().isClient());

        assertEquals(client.cluster().nodes().size(), 3);
    }

    /** Discovery SPI delayed TcpDiscoveryNodeAddFinishedMessage. */
    private static class DelayedTcpDiscoverySpi extends TcpDiscoverySpi {

        /** {@inheritDoc} */
        @Override protected void writeToSocket(ClusterNode node, Socket sock, OutputStream out, TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            if (msg instanceof TcpDiscoveryNodeAddFinishedMessage && msg.topologyVersion() == 3) {
                log.info("Catched discovery message: " + msg);

                try {
                    Thread.sleep(2_000);
                }
                catch (InterruptedException e) {
                    log.error("Interrupt on DelayedTcpDiscoverySpi.", e);
                }
            }

            super.writeToSocket(node, sock, out, msg, timeout);
        }
    }
}
