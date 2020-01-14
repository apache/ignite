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
import java.net.Socket;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutException;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutHelper;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddFinishedMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Test;

/**
 *
 */
@SuppressWarnings("deprecation")
public class TcpDiscoveryCoordinatorFailureTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(false)
        .setAddresses(Collections.singleton("127.0.0.1:47500..47504"));

    /** */
    private Map<String, TcpDiscoverySpi> discoSpis;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi spi = null;

        if (discoSpis != null)
            spi = discoSpis.get(igniteInstanceName);

        if (spi == null)
            spi = new TcpDiscoverySpi();

        spi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(spi);

        if (getTestIgniteInstanceName(3).equals(igniteInstanceName)) {
            UUID oldId = cfg.getNodeId();

            // Make sure most significant bits is a positive number.
            cfg.setNodeId(new UUID(oldId.getMostSignificantBits() & 0x7FFFFFFFFFFFFFFFL, oldId.getLeastSignificantBits()));
        }

        if (getTestIgniteInstanceName(4).equals(igniteInstanceName)) {
            UUID oldId = cfg.getNodeId();

            // Zero the most significant bits of node ID so that 4th node can win
            // discovery concurrent node startup race (must be 1 in order to not break communication).
            cfg.setNodeId(new UUID(1, oldId.getLeastSignificantBits()));
        }

        return cfg;
    }

    /**
     *
     */
    @After
    public void testCleanup() {
        discoSpis = null;
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testCoordinatorFailedNoAddFinishedMessageStartOneNode() throws Exception {
        checkCoordinatorFailedNoAddFinishedMessage(false);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testCoordinatorFailedNoAddFinishedMessageStartTwoNodes() throws Exception {
        checkCoordinatorFailedNoAddFinishedMessage(true);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testClusterFailedNewCoordinatorInitialized() throws Exception {
        StallingJoinDiscoverySpi stallSpi = new StallingJoinDiscoverySpi();

        discoSpis = F.asMap(
            getTestIgniteInstanceName(0), new FailingDiscoverySpi(3),
            getTestIgniteInstanceName(1), new FailingDiscoverySpi(3),
            getTestIgniteInstanceName(2), new FailingDiscoverySpi(3),
            getTestIgniteInstanceName(3), stallSpi
        );

        try {
            startGrids(3);

            IgniteInternalFuture<IgniteEx> fut3 = GridTestUtils.runAsync(() -> startGrid(3), "starter-3");

            ((FailingDiscoverySpi)grid(0).configuration().getDiscoverySpi()).awaitDrop();

            IgniteInternalFuture<IgniteEx> fut4 = GridTestUtils.runAsync(() -> startGrid(4), "starter-4");

            stopGrid(0, true);

            ((FailingDiscoverySpi)grid(1).configuration().getDiscoverySpi()).awaitDrop();
            stopGrid(1, true);

            ((FailingDiscoverySpi)grid(2).configuration().getDiscoverySpi()).awaitDrop();
            stopGrid(2, true);

            stallSpi.startStall();

            // At this point startGrid(3) cannot proceed as well because openSocket() is blocked.
            assertFalse(fut3.isDone());

            fut4.get();

            IgniteEx newCrd = grid(4);

            stallSpi.stopStall();

            assertEquals("New node did not become a coordinator",
                1, newCrd.cluster().localNode().order());

            fut3.get();

            assertEquals("Second node did not join the grid",
                2, grid(3).cluster().localNode().order());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception if failed.
     */
    private void checkCoordinatorFailedNoAddFinishedMessage(boolean startAnother) throws Exception {
        discoSpis = F.asMap(getTestIgniteInstanceName(0), new FailingDiscoverySpi(3));

        try {
            startGrids(3);

            IgniteInternalFuture<IgniteEx> fut = GridTestUtils.runAsync(() -> startGrid(3));

            ((FailingDiscoverySpi)grid(0).configuration().getDiscoverySpi()).awaitDrop();

            stopGrid(0, true);

            if (startAnother)
                startGrid(4);

            fut.get(getTestTimeout());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    private static class StallingJoinDiscoverySpi extends TcpDiscoverySpi {
        /** */
        private volatile CountDownLatch stallLatch;

        /** {@inheritDoc} */
        @Override protected Socket openSocket(InetSocketAddress sockAddr, IgniteSpiOperationTimeoutHelper timeoutHelper) throws IOException, IgniteSpiOperationTimeoutException {
            checkStall();

            return super.openSocket(sockAddr, timeoutHelper);
        }

        /** {@inheritDoc} */
        @Override protected Socket openSocket(Socket sock, InetSocketAddress remAddr, IgniteSpiOperationTimeoutHelper timeoutHelper) throws IOException, IgniteSpiOperationTimeoutException {
            checkStall();

            return super.openSocket(sock, remAddr, timeoutHelper);
        }

        /**
         */
        private void checkStall() {
            CountDownLatch latch = stallLatch;

            try {
                if (latch != null)
                    latch.await();
            }
            catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }

        /**
         */
        private void startStall() {
            if (stallLatch == null)
                stallLatch = new CountDownLatch(1);
        }

        /**
         */
        private void stopStall() {
            if (stallLatch != null) {
                stallLatch.countDown();

                stallLatch = null;
            }
        }
    }

    /**
     *
     */
    private static class FailingDiscoverySpi extends TcpDiscoverySpi {
        /** */
        private CountDownLatch dropLatch = new CountDownLatch(1);

        /** */
        private int dropNodeIdx;

        /** */
        private boolean drop;

        /**
         * @param dropNodeIdx Node test index to drop.
         */
        private FailingDiscoverySpi(int dropNodeIdx) {
            this.dropNodeIdx = dropNodeIdx;
        }

        /**
         * @throws InterruptedException If current thread was interrupted.
         */
        public void awaitDrop() throws InterruptedException, IgniteCheckedException {
            if (!dropLatch.await(15, TimeUnit.SECONDS))
                throw new IgniteCheckedException("Failed to wait for NodeAddFinishedMessage");
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(
            Socket sock,
            TcpDiscoveryAbstractMessage msg,
            byte[] data,
            long timeout
        ) throws IOException {
            if (isDrop(msg))
                return;

            super.writeToSocket(sock, msg, data, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(
            Socket sock,
            TcpDiscoveryAbstractMessage msg,
            long timeout
        ) throws IOException, IgniteCheckedException {
            if (isDrop(msg))
                return;

            super.writeToSocket(sock, msg, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(
            ClusterNode node,
            Socket sock,
            OutputStream out,
            TcpDiscoveryAbstractMessage msg,
            long timeout
        ) throws IOException, IgniteCheckedException {
            if (isDrop(msg))
                return;

            super.writeToSocket(node, sock, out, msg, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(
            Socket sock,
            OutputStream out,
            TcpDiscoveryAbstractMessage msg,
            long timeout
        ) throws IOException, IgniteCheckedException {
            if (isDrop(msg))
                return;

            super.writeToSocket(sock, out, msg, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(
            TcpDiscoveryAbstractMessage msg,
            Socket sock,
            int res,
            long timeout
        ) throws IOException {
            if (isDrop(msg))
                return;

            super.writeToSocket(msg, sock, res, timeout);
        }

        /**
         *
         */
        private boolean isDrop(TcpDiscoveryAbstractMessage msg) {
            if (!drop) {
                if (msg instanceof TcpDiscoveryNodeAddFinishedMessage) {
                    TcpDiscoveryNodeAddFinishedMessage finishMsg = (TcpDiscoveryNodeAddFinishedMessage)msg;

                    if ((finishMsg.nodeId().getLeastSignificantBits() & 0xFFFF) == dropNodeIdx) {
                        drop = true;

                        dropLatch.countDown();
                    }
                }
            }

            if (drop)
                ignite.log().info(">> Drop message " + msg);

            return drop;
        }
    }
}
