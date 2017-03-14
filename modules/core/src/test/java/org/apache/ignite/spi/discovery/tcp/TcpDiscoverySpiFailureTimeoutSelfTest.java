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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutException;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutHelper;
import org.apache.ignite.spi.discovery.AbstractDiscoverySelfTest;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryConnectionCheckMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryPingRequest;

/**
 *
 */
public class TcpDiscoverySpiFailureTimeoutSelfTest extends AbstractDiscoverySelfTest {
    /** */
    private static final int SPI_COUNT = 6;

    /** */
    private TcpDiscoveryIpFinder ipFinder =  new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected int getSpiCount() {
        return SPI_COUNT;
    }

    /** {@inheritDoc} */
    @Override protected DiscoverySpi getSpi(int idx) {
        TestTcpDiscoverySpi spi = new TestTcpDiscoverySpi();

        spi.setMetricsProvider(createMetricsProvider());
        spi.setIpFinder(ipFinder);

        switch (idx) {
            case 0:
            case 1:
                // Ignore
                break;
            case 2:
                spi.setAckTimeout(3000);
                break;
            case 3:
                spi.setSocketTimeout(4000);
                break;
            case 4:
                spi.setReconnectCount(4);
                break;
            case 5:
                spi.setMaxAckTimeout(10000);
                break;
            default:
                assert false;
        }

        return spi;
    }

    /**
     * @throws Exception In case of error.
     */
    public void testFailureDetectionTimeoutEnabled() throws Exception {
        assertTrue(firstSpi().failureDetectionTimeoutEnabled());
        assertTrue(secondSpi().failureDetectionTimeoutEnabled());

        assertEquals(IgniteConfiguration.DFLT_FAILURE_DETECTION_TIMEOUT.longValue(),
                firstSpi().failureDetectionTimeout());
        assertEquals(IgniteConfiguration.DFLT_FAILURE_DETECTION_TIMEOUT.longValue(),
                secondSpi().failureDetectionTimeout());
    }

    /**
     * @throws Exception In case of error.
     */
    public void testFailureDetectionTimeoutDisabled() throws Exception {
        for (int i = 2; i < spis.size(); i++) {
            assertFalse(((TcpDiscoverySpi)spis.get(i)).failureDetectionTimeoutEnabled());
            assertEquals(0, ((TcpDiscoverySpi)spis.get(i)).failureDetectionTimeout());
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void testFailureDetectionOnSocketOpen() throws Exception {
        try {
            ClusterNode node = secondSpi().getLocalNode();

            firstSpi().openSocketTimeout = true;

            assertFalse(firstSpi().pingNode(node.id()));
            assertTrue(firstSpi().validTimeout);
            assertTrue(firstSpi().err.getMessage().equals("Timeout: openSocketTimeout"));

            firstSpi().openSocketTimeout = false;
            firstSpi().openSocketTimeoutWait = true;

            assertFalse(firstSpi().pingNode(node.id()));
            assertTrue(firstSpi().validTimeout);
            assertTrue(firstSpi().err.getMessage().equals("Timeout: openSocketTimeoutWait"));
        }
        finally {
            firstSpi().resetState();
        }
    }


    /**
     * @throws Exception In case of error.
     */
    public void testFailureDetectionOnSocketWrite() throws Exception {
        try {
            ClusterNode node = secondSpi().getLocalNode();

            firstSpi().writeToSocketTimeoutWait = true;

            assertFalse(firstSpi().pingNode(node.id()));
            assertTrue(firstSpi().validTimeout);

            firstSpi().writeToSocketTimeoutWait = false;

            assertTrue(firstSpi().pingNode(node.id()));
            assertTrue(firstSpi().validTimeout);
        }
        finally {
            firstSpi().resetState();
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void testConnectionCheckMessage() throws Exception {
        TestTcpDiscoverySpi nextSpi = null;

        try {
            assert firstSpi().connCheckStatusMsgCntSent == 0;

            TcpDiscoveryNode nextNode = ((ServerImpl)(firstSpi().impl)).ring().nextNode();

            assertNotNull(nextNode);

            nextSpi = null;

            for (int i = 1; i < spis.size(); i++)
                if (spis.get(i).getLocalNode().id().equals(nextNode.id())) {
                    nextSpi = (TestTcpDiscoverySpi)spis.get(i);
                    break;
                }

            assertNotNull(nextSpi);

            assert nextSpi.connCheckStatusMsgCntReceived == 0;

            firstSpi().countConnCheckMsg = true;
            nextSpi.countConnCheckMsg = true;

            Thread.sleep(firstSpi().failureDetectionTimeout());

            firstSpi().countConnCheckMsg = false;
            nextSpi.countConnCheckMsg = false;

            int sent = firstSpi().connCheckStatusMsgCntSent;
            int received = nextSpi.connCheckStatusMsgCntReceived;

            assert sent >= 3 && sent < 7 : "messages sent: " + sent;
            assert received >= 3 && received < 7 : "messages received: " + received;
        }
        finally {
            firstSpi().resetState();

            if (nextSpi != null)
                nextSpi.resetState();
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void testConnectionCheckMessageBackwardCompatibility() throws Exception {
        TestTcpDiscoverySpi nextSpi = null;
        TcpDiscoveryNode nextNode = null;

        IgniteProductVersion nextNodeVer = null;

        try {
            assert firstSpi().connCheckStatusMsgCntSent == 0;

            nextNode = ((ServerImpl)(firstSpi().impl)).ring().nextNode();

            assertNotNull(nextNode);

            nextSpi = null;

            for (int i = 1; i < spis.size(); i++)
                if (spis.get(i).getLocalNode().id().equals(nextNode.id())) {
                    nextSpi = (TestTcpDiscoverySpi)spis.get(i);
                    break;
                }

            assertNotNull(nextSpi);

            assert nextSpi.connCheckStatusMsgCntReceived == 0;

            nextNodeVer = nextNode.version();

            // Overriding the version of the next node. Connection check message must not been sent to it.
            nextNode.version(new IgniteProductVersion(TcpDiscoverySpi.FAILURE_DETECTION_MAJOR_VER,
                (byte)(TcpDiscoverySpi.FAILURE_DETECTION_MINOR_VER - 1), TcpDiscoverySpi.FAILURE_DETECTION_MAINT_VER,
                0l, null));

            firstSpi().countConnCheckMsg = true;
            nextSpi.countConnCheckMsg = true;

            Thread.sleep(firstSpi().failureDetectionTimeout() / 2);

            firstSpi().countConnCheckMsg = false;
            nextSpi.countConnCheckMsg = false;

            int sent = firstSpi().connCheckStatusMsgCntSent;
            int received = nextSpi.connCheckStatusMsgCntReceived;

            assert sent == 0 : "messages sent: " + sent;
            assert received == 0 : "messages received: " + received;
        }
        finally {
            firstSpi().resetState();

            if (nextSpi != null)
                nextSpi.resetState();

            if (nextNode != null && nextNodeVer != null)
                nextNode.version(nextNodeVer);
        }
    }

    /**
     * Returns the first spi with failure detection timeout enabled.
     *
     * @return SPI.
     */
    private TestTcpDiscoverySpi firstSpi() {
        return (TestTcpDiscoverySpi)spis.get(0);
    }


    /**
     * Returns the second spi with failure detection timeout enabled.
     *
     * @return SPI.
     */
    private TestTcpDiscoverySpi secondSpi() {
        return (TestTcpDiscoverySpi)spis.get(1);
    }

    /**
     *
     */
    private static class TestTcpDiscoverySpi extends TcpDiscoverySpi {
        /** */
        private volatile boolean openSocketTimeout;

        /** */
        private volatile boolean openSocketTimeoutWait;

        /** */
        private volatile boolean writeToSocketTimeoutWait;

        /** */
        private volatile boolean countConnCheckMsg;

        /** */
        private volatile int connCheckStatusMsgCntSent;

        /** */
        private volatile int connCheckStatusMsgCntReceived;

        /** */
        private volatile boolean validTimeout = true;

        /** */
        private volatile IgniteSpiOperationTimeoutException err;


        /** {@inheritDoc} */
        @Override protected Socket openSocket(Socket sock, InetSocketAddress sockAddr,
            IgniteSpiOperationTimeoutHelper timeoutHelper)
            throws IOException, IgniteSpiOperationTimeoutException {

            if (openSocketTimeout) {
                err = new IgniteSpiOperationTimeoutException("Timeout: openSocketTimeout");
                throw err;
            }
            else if (openSocketTimeoutWait) {
                long timeout = timeoutHelper.nextTimeoutChunk(0);

                try {
                    Thread.sleep(timeout + 1000);
                }
                catch (InterruptedException ignored) {
                    // No-op.
                }

                try {
                    timeoutHelper.nextTimeoutChunk(0);
                }
                catch (IgniteSpiOperationTimeoutException ignored) {
                    throw (err = new IgniteSpiOperationTimeoutException("Timeout: openSocketTimeoutWait"));
                }
            }

            super.openSocket(sock, sockAddr, timeoutHelper);

            try {
                Thread.sleep(1500);
            }
            catch (InterruptedException ignored) {
                // No-op.
            }

            return sock;
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, OutputStream out, TcpDiscoveryAbstractMessage msg, long timeout)
            throws IOException, IgniteCheckedException {
            if (!(msg instanceof TcpDiscoveryPingRequest)) {
                if (countConnCheckMsg && msg instanceof TcpDiscoveryConnectionCheckMessage)
                    connCheckStatusMsgCntSent++;

                super.writeToSocket(sock, out, msg, timeout);

                return;
            }

            if (timeout >= IgniteConfiguration.DFLT_FAILURE_DETECTION_TIMEOUT) {
                validTimeout = false;

                throw new IgniteCheckedException("Invalid timeout: " + timeout);
            }

            if (writeToSocketTimeoutWait) {
                try {
                    Thread.sleep(timeout);
                }
                catch (InterruptedException ignored) {
                    // No-op.
                }
            }
            else
                super.writeToSocket(sock, out, msg, timeout);
        }

        /** {@inheritDoc} */
        protected void writeToSocket(TcpDiscoveryAbstractMessage msg, Socket sock, int res, long timeout)
            throws IOException {
            if (countConnCheckMsg && msg instanceof TcpDiscoveryConnectionCheckMessage)
                connCheckStatusMsgCntReceived++;

            super.writeToSocket(msg, sock, res, timeout);
        }

        /**
         *
         */
        private void resetState() {
            openSocketTimeout = false;
            openSocketTimeoutWait = false;
            writeToSocketTimeoutWait = false;
            err = null;
            validTimeout = true;
            connCheckStatusMsgCntSent = 0;
            connCheckStatusMsgCntReceived = 0;
            countConnCheckMsg = false;
        }
    }
}
