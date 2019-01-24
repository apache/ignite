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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddFinishedMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
public class TcpDiscoveryCoordinatorFailureTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (getTestIgniteInstanceName(0).equals(igniteInstanceName))
            cfg.setDiscoverySpi(new FailingDiscoverySpi().setIpFinder(IP_FINDER));
        else
            cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        return cfg;
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
    private void checkCoordinatorFailedNoAddFinishedMessage(boolean startAnother) throws Exception {
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
    private static class FailingDiscoverySpi extends TcpDiscoverySpi {
        /** */
        private CountDownLatch dropLatch = new CountDownLatch(1);

        /** */
        private boolean drop;

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

                    if ((finishMsg.nodeId().getLeastSignificantBits() & 0xFFFF) == 3) {
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
