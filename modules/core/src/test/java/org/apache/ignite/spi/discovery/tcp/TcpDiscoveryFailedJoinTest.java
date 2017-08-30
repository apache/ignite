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
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.Collections;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutException;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutHelper;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test checks that if node cannot accept incoming connections it will be
 * kicked off and stopped.
 *
 * Older versions will infinitely retry to connect, but this will not lead
 * to node join and, as a consequence, to start exchange process.
 */
public class TcpDiscoveryFailedJoinTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        FailTcpDiscoverySpi discoSpi = new FailTcpDiscoverySpi();

        discoSpi.setLocalPort(Integer.parseInt(gridName.split("-")[1]));

        TcpDiscoveryVmIpFinder finder = new TcpDiscoveryVmIpFinder(true);

        finder.setAddresses(Collections.singleton("127.0.0.1:47500..47503"));

        discoSpi.setIpFinder(finder);

        cfg.setDiscoverySpi(discoSpi);

        if (gridName.contains("client")) {
            cfg.setClientMode(true);

            discoSpi.setForceServerMode(gridName.contains("server"));
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testDiscovery() throws Exception {
        startGrid("server-47500");
        startGrid("server-47501");
        startGrid("server-47502");

        assertStartFailed("server-47503");

        // Client in server mode.
        assertStartFailed("client_server-47503");

        // Regular client start normally.
        startGrid("client-47503");
    }

    /**
     * @param name Name.
     */
    private void assertStartFailed(final String name) throws Exception {
        //noinspection ThrowableNotThrown
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                startGrid(name);

                return null;
            }
        }, IgniteCheckedException.class, null);
    }

    /**
     *
     */
    private static class FailTcpDiscoverySpi extends TcpDiscoverySpi {
        /** {@inheritDoc} */
        @Override protected Socket openSocket(InetSocketAddress sockAddr,
            IgniteSpiOperationTimeoutHelper timeoutHelper) throws IOException, IgniteSpiOperationTimeoutException {
            if (sockAddr.getPort() == 47503)
                throw new SocketException("Connection refused");

            return super.openSocket(sockAddr, timeoutHelper);
        }

        /** {@inheritDoc} */
        @Override protected Socket openSocket(Socket sock, InetSocketAddress remAddr,
            IgniteSpiOperationTimeoutHelper timeoutHelper) throws IOException, IgniteSpiOperationTimeoutException {
            if (remAddr.getPort() == 47503)
                throw new SocketException("Connection refused");

            return super.openSocket(sock, remAddr, timeoutHelper);
        }
    }


}
