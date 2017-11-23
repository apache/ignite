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

import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_FAILURE_DETECTION_TIMEOUT;

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

        assertEquals(DFLT_FAILURE_DETECTION_TIMEOUT.longValue(),
                firstSpi().failureDetectionTimeout());
        assertEquals(DFLT_FAILURE_DETECTION_TIMEOUT.longValue(),
                secondSpi().failureDetectionTimeout());

        assertEquals(IgniteConfiguration.DFLT_CLIENT_FAILURE_DETECTION_TIMEOUT.longValue(),
            firstSpi().clientFailureDetectionTimeout());
        assertEquals(IgniteConfiguration.DFLT_CLIENT_FAILURE_DETECTION_TIMEOUT.longValue(),
            secondSpi().clientFailureDetectionTimeout());
    }

    /**
     * @throws Exception In case of error.
     */
    public void testFailureDetectionTimeoutDisabled() throws Exception {
        for (int i = 2; i < spis.size(); i++) {
            assertFalse(((TcpDiscoverySpi)spis.get(i)).failureDetectionTimeoutEnabled());
            assertEquals(0, ((TcpDiscoverySpi)spis.get(i)).failureDetectionTimeout());
            assertFalse(0 == ((TcpDiscoverySpi)spis.get(i)).clientFailureDetectionTimeout());
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void testFailureDetectionOnSocketOpen() throws Exception {
        try {
            ClusterNode node = secondSpi().getLocalNode();

            firstSpi().openSockTimeout = true;

            assertFalse(firstSpi().pingNode(node.id()));
            assertTrue(firstSpi().validTimeout);
            assertTrue(firstSpi().err.getMessage().equals("Timeout: openSocketTimeout"));

            firstSpi().openSockTimeout = false;
            firstSpi().openSockTimeoutWait = true;

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

            firstSpi().writeToSockTimeoutWait = true;

            assertFalse(firstSpi().pingNode(node.id()));
            assertTrue(firstSpi().validTimeout);

            firstSpi().writeToSockTimeoutWait = false;

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
        TestTcpDiscoverySpi prevSpi = (TestTcpDiscoverySpi)spis.get(3);

        assert prevSpi != null;

        TestTcpDiscoverySpi nextSpi = null;

        try {
            assert prevSpi.connCheckStatusMsgCntSent == 0;

            TcpDiscoveryNode nextNode = ((ServerImpl)(prevSpi.impl)).ring().nextNode();

            assertNotNull(nextNode);

            nextSpi = null;

            for (int i = 1; i < spis.size(); i++)
                if (spis.get(i).getLocalNode().id().equals(nextNode.id())) {
                    nextSpi = (TestTcpDiscoverySpi)spis.get(i);
                    break;
                }

            assertNotNull(nextSpi);

            if (log.isInfoEnabled())
                log.info("Start connection check counting");

            assert nextSpi.connCheckStatusMsgCntReceived == 0;

            prevSpi.cntConnCheckMsg = true;
            nextSpi.cntConnCheckMsg = true;

            Thread.sleep(firstSpi().failureDetectionTimeout());

            if (log.isInfoEnabled())
                log.info("Stop connection check counting");

            prevSpi.cntConnCheckMsg = false;
            nextSpi.cntConnCheckMsg = false;

            int sent = prevSpi.connCheckStatusMsgCntSent;
            int received = nextSpi.connCheckStatusMsgCntReceived;

            int expected = (int)(firstSpi().failureDetectionTimeout() / firstSpi().metricsUpdateFreq);

            assert sent >= expected - 1 && sent <= expected : "messages sent: " + sent;
            assert received >= expected - 1 && received <= expected : "messages received: " + received;
        }
        finally {
            prevSpi.resetState();

            if (nextSpi != null)
                nextSpi.resetState();
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
        private volatile boolean openSockTimeout;

        /** */
        private volatile boolean openSockTimeoutWait;

        /** */
        private volatile boolean writeToSockTimeoutWait;

        /** */
        private volatile boolean cntConnCheckMsg;

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

            if (openSockTimeout) {
                err = new IgniteSpiOperationTimeoutException("Timeout: openSocketTimeout");
                throw err;
            }
            else if (openSockTimeoutWait) {
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

            if (log.isTraceEnabled())
                log.trace("$> send [sock=" + sock + ", msg=" + msg + ']');

            if (!(msg instanceof TcpDiscoveryPingRequest)) {
                if (cntConnCheckMsg && msg instanceof TcpDiscoveryConnectionCheckMessage)
                    connCheckStatusMsgCntSent++;

                super.writeToSocket(sock, out, msg, timeout);

                return;
            }

            if (timeout >= DFLT_FAILURE_DETECTION_TIMEOUT) {
                validTimeout = false;

                throw new IgniteCheckedException("Invalid timeout: " + timeout);
            }

            if (writeToSockTimeoutWait) {
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

            if (log.isTraceEnabled())
                log.trace("$> respond [sock=" + sock + ", msg=" + msg + ']');

            if (cntConnCheckMsg && msg instanceof TcpDiscoveryConnectionCheckMessage)
                connCheckStatusMsgCntReceived++;

            super.writeToSocket(msg, sock, res, timeout);
        }

        /**
         *
         */
        private void resetState() {
            openSockTimeout = false;
            openSockTimeoutWait = false;
            writeToSockTimeoutWait = false;
            err = null;
            validTimeout = true;
            connCheckStatusMsgCntSent = 0;
            connCheckStatusMsgCntReceived = 0;
            cntConnCheckMsg = false;
        }
    }
}
