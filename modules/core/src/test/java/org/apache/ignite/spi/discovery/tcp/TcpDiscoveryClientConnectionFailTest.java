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

package org.apache.ignite.spi.discovery.tcp;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientHeartbeatMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddFinishedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddedMessage;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class TcpDiscoveryClientConnectionFailTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);
    /** */
    private static volatile boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (gridName.endsWith("0"))
            cfg.setFailureDetectionTimeout(100);

        TestDiscoverySpi spi = new TestDiscoverySpi(gridName);
        spi.setClientReconnectDisabled(false);
        spi.setIpFinder(ipFinder);

        spi.setJoinTimeout(20000);
        spi.setMaxMissedClientHeartbeats(2);
        spi.failureDetectionTimeoutEnabled(true);

        cfg.setDiscoverySpi(spi);

        cfg.setClientMode(client);

        return cfg;
    }

    /** */
    public void testClientFailsIfCantConnect() throws Exception {
        client = false;

        startGrid(0);

        client = true;

        IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                try {
                    startGrid(1);
                }
                catch (Exception ignored) {
                }

            }
        }, 1, "client start thread");

        try {
            fut.get(10000);
        }
        catch (IgniteFutureTimeoutCheckedException e) {
            fail("client node start hangs (possible deadlock detected)");
        }

    }

    /**
     *
     */
    private static class TestDiscoverySpi extends TcpDiscoverySpi {
        /** */
        private final String name;
        /** */
        private boolean block;

        /** */
        TestDiscoverySpi(String name) {
            this.name = name;
        }

        /**
         * @param msg Message.
         * @return {@code False} if should not further process message.
         * @throws IOException If failed.
         */
        private boolean onMessage(TcpDiscoveryAbstractMessage msg) throws IOException {
            if (msg == null)
                return true;

            boolean blocked = false;

            // we block first connection attempt only
            if (msg instanceof TcpDiscoveryNodeAddedMessage)
                block = !block;

            if (msg instanceof TcpDiscoveryClientHeartbeatMessage && block)
                blocked = true;

            if (msg instanceof TcpDiscoveryNodeAddFinishedMessage && block)
                blocked = true;

            return !blocked;
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg, byte[] data,
            long timeout) throws IOException {

            if (onMessage(msg))
                super.writeToSocket(sock, msg, data, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(TcpDiscoveryAbstractMessage msg, Socket sock, int res,
            long timeout) throws IOException {
            if (onMessage(msg))
                super.writeToSocket(msg, sock, res, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            if (onMessage(msg))
                super.writeToSocket(sock, msg, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, OutputStream out, TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            if (onMessage(msg))
                super.writeToSocket(sock, out, msg, timeout);
        }
    }
}
