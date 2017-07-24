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

package org.apache.ignite.internal.managers.communication;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Test for behavior when CommunicationSpi throws runtime exception
 */
public class MessageSendExceptionTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new ChaosCommunicationSpi(client));

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);

        client = true;

        startGrid(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    public void testSendException() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(1).createCache("test");

        cache.put(1, 1);

        try {
            ChaosCommunicationSpi.FAIL.set(true);

            new Thread() {
                @Override public void run() {
                    try {
                        Thread.sleep(1000);
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace(System.out);
                    }

                    ChaosCommunicationSpi.FAIL.set(false);
                }
            }.start();

            cache.put(2, 2);
        }
        catch (Exception e) {
            error("Exception: " + e.getMessage());
        }

        cache.put(3, 3);

        assertEquals(Integer.valueOf(1), cache.get(1));
        assertEquals(Integer.valueOf(2), cache.get(2));
        assertEquals(Integer.valueOf(3), cache.get(3));
    }

    /**
     * Implementation of TcpCommunicationSpi, which throws runtime exception in sendMessage method.
     */
    private static class ChaosCommunicationSpi extends TcpCommunicationSpi {
        /** */
        private final boolean client;

        /** */
        public static final AtomicBoolean FAIL = new AtomicBoolean(false);

        /** */
        public ChaosCommunicationSpi(boolean client) {
            this.client = client;
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg) throws IgniteSpiException {
            if (!this.client && FAIL.get())
                throw new RuntimeException("Chaos");

            super.sendMessage(node, msg);
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
            throws IgniteSpiException {
            if (!this.client && FAIL.get())
                throw new RuntimeException("Chaos");

            super.sendMessage(node, msg, ackC);
        }
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 60 * 1000; // 1 min.
    }
}
