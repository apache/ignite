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
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Collections;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.DummyQueryIndexing;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutException;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutHelper;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test checks that if node cannot accept incoming connections it will be
 * kicked off and stopped.
 *
 * Older versions will infinitely retry to connect, but this will not lead
 * to node join and, as a consequence, to start exchange process.
 */
public class TcpDiscoveryFailedJoinTest extends GridCommonAbstractTest {
    /** */
    private static final int FAIL_PORT = 47503;

    /** */
    private static final int BIND_PORT = 47511;

    /** */
    private SpiFailType failType = SpiFailType.REFUSE;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = failType == SpiFailType.REFUSE ? new FailTcpDiscoverySpi() : new DropTcpDiscoverySpi();

        discoSpi.setLocalPort(Integer.parseInt(gridName.split("-")[1]));

        TcpDiscoveryVmIpFinder finder = new TcpDiscoveryVmIpFinder(true);

        finder.setAddresses(Collections.singleton("127.0.0.1:47500..47503"));

        discoSpi.setIpFinder(finder);
        discoSpi.setNetworkTimeout(2_000);
        discoSpi.setForceServerMode(gridName.contains("client") && gridName.contains("server"));

        cfg.setDiscoverySpi(discoSpi);

        if (gridName.contains("failingNode")) {
            GridQueryProcessor.idxCls = FailingIndexing.class;

            cfg.setLocalHost("127.0.0.1");
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        GridQueryProcessor.idxCls = null;
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testPortReleasedAfterFailure() throws Exception {
        try {
            startGrid("failingNode-" + BIND_PORT);

            fail("Node start should fail");
        }
        catch (Exception e) {
            // Expected exception. Check that BIND_PORT can be re-bound.
            ServerSocket sock = new ServerSocket();

            try {
                sock.setReuseAddress(true);

                sock.bind(new InetSocketAddress("127.0.0.1", BIND_PORT));
            }
            finally {
                U.close(sock, log);
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDiscoveryRefuse() throws Exception {
        failType = SpiFailType.REFUSE;

        startGrid("server-47500");
        startGrid("server-47501");
        startGrid("server-47502");

        assertStartFailed("server-47503");

        // Client in server mode.
        assertStartFailed("client_server-47503");

        // Regular client starts normally.
        startClientGrid("client-47503");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDiscoveryDrop() throws Exception {
        failType = SpiFailType.DROP;

        startGrid("server-47500");
        startGrid("server-47501");
        startGrid("server-47502");

        assertStartFailed("server-47503");

        // Client in server mode.
        assertStartFailed("client_server-47503");

        // Regular client starts normally.
        startClientGrid("client-47503");
    }

    /**
     * @param name Name.
     */
    private void assertStartFailed(final String name) {
        //noinspection ThrowableNotThrown
        GridTestUtils.assertThrows(log, () -> {
            if (name.contains("client"))
                startClientGrid(name);
            else
                startGrid(name);

            return null;
        }, IgniteCheckedException.class, null);
    }

    /**
     *
     */
    private static class FailTcpDiscoverySpi extends TcpDiscoverySpi {
        /** {@inheritDoc} */
        @Override protected Socket openSocket(InetSocketAddress sockAddr,
            IgniteSpiOperationTimeoutHelper timeoutHelper) throws IOException, IgniteSpiOperationTimeoutException {
            if (sockAddr.getPort() == FAIL_PORT)
                throw new SocketException("Connection refused");

            return super.openSocket(sockAddr, timeoutHelper);
        }

        /** {@inheritDoc} */
        @Override protected Socket openSocket(Socket sock, InetSocketAddress remAddr,
            IgniteSpiOperationTimeoutHelper timeoutHelper) throws IOException, IgniteSpiOperationTimeoutException {
            if (remAddr.getPort() == FAIL_PORT)
                throw new SocketException("Connection refused");

            return super.openSocket(sock, remAddr, timeoutHelper);
        }
    }

    /**
     * Emulates situation when network drops packages.
     */
    private static class DropTcpDiscoverySpi extends TcpDiscoverySpi {
        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg, byte[] data,
            long timeout) throws IOException {
            if (sock.getPort() != FAIL_PORT)
                super.writeToSocket(sock, msg, data, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            if (sock.getPort() != FAIL_PORT)
                super.writeToSocket(sock, msg, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(ClusterNode node, Socket sock, OutputStream out,
            TcpDiscoveryAbstractMessage msg, long timeout) throws IOException, IgniteCheckedException {
            if (sock.getPort() != FAIL_PORT)
                super.writeToSocket(node, sock, out, msg, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, OutputStream out, TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            if (sock.getPort() != FAIL_PORT)
                super.writeToSocket(sock, out, msg, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(TcpDiscoveryAbstractMessage msg, Socket sock, int res,
            long timeout) throws IOException {
            if (sock.getPort() != FAIL_PORT)
                super.writeToSocket(msg, sock, res, timeout);
        }
    }

    /**
     *
     */
    private static class FailingIndexing extends DummyQueryIndexing {
        /** {@inheritDoc} */
        @Override public void start(GridKernalContext ctx, GridSpinBusyLock busyLock) {
            ctx.discovery().consistentId();

            throw new IgniteException("Failed to start");
        }
    }

    /**
     *
     */
    private enum SpiFailType {
        /** */
        REFUSE,

        /** */
        DROP
    }
}
