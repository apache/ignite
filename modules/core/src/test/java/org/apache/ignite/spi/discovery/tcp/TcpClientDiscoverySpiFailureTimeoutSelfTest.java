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
import java.io.InputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryPingRequest;
import org.jetbrains.annotations.Nullable;

/**
 * Client-based discovery SPI test with failure detection timeout enabled.
 */
public class TcpClientDiscoverySpiFailureTimeoutSelfTest extends TcpClientDiscoverySpiSelfTest {
    /** */
    private final static int FAILURE_AWAIT_TIME = 7_000;

    /** */
    private final static long FAILURE_THRESHOLD = 10_000;

    /** */
    private static long failureThreshold = FAILURE_THRESHOLD;

    /** */
    private static boolean useTestSpi;

    /** {@inheritDoc} */
    @Override protected boolean useFailureDetectionTimeout() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected long failureDetectionTimeout() {
        return failureThreshold;
    }

    /** {@inheritDoc} */
    @Override protected long awaitTime() {
        return failureDetectionTimeout() + FAILURE_AWAIT_TIME;
    }

    /** {@inheritDoc} */
    @Override protected TcpDiscoverySpi getDiscoverySpi() {
        return useTestSpi ? new TestTcpDiscoverySpi() : super.getDiscoverySpi();
    }

    /**
     * @throws Exception in case of error.
     */
    public void testFailureDetectionTimeoutEnabled() throws Exception {
        startServerNodes(1);
        startClientNodes(1);

        checkNodes(1, 1);

        assertTrue(((TcpDiscoverySpi)(G.ignite("server-0").configuration().getDiscoverySpi())).
                failureDetectionTimeoutEnabled());
        assertEquals(failureDetectionTimeout(),
            ((TcpDiscoverySpi)(G.ignite("server-0").configuration().getDiscoverySpi())).failureDetectionTimeout());

        assertTrue(((TcpDiscoverySpi)(G.ignite("client-0").configuration().getDiscoverySpi())).
                failureDetectionTimeoutEnabled());
        assertEquals(failureDetectionTimeout(),
            ((TcpDiscoverySpi)(G.ignite("client-0").configuration().getDiscoverySpi())).failureDetectionTimeout());
    }

    /**
     * @throws Exception in case of error.
     */
    public void testFailureTimeoutWorkabilityAvgTimeout() throws Exception {
        failureThreshold = 3000;

        try {
            checkFailureThresholdWorkability();
        }
        finally {
            failureThreshold = FAILURE_THRESHOLD;
        }
    }

    /**
     * @throws Exception in case of error.
     */
    public void testFailureTimeoutWorkabilitySmallTimeout() throws Exception {
        failureThreshold = 500;

        try {
            checkFailureThresholdWorkability();
        }
        finally {
            failureThreshold = FAILURE_THRESHOLD;
        }
    }

    /**
     * @throws Exception in case of error.
     */
    private void checkFailureThresholdWorkability() throws Exception {
        useTestSpi = true;

        TestTcpDiscoverySpi firstSpi = null;
        TestTcpDiscoverySpi secondSpi = null;

        try {
            startServerNodes(2);

            checkNodes(2, 0);

            firstSpi = (TestTcpDiscoverySpi)(G.ignite("server-0").configuration().getDiscoverySpi());
            secondSpi = (TestTcpDiscoverySpi)(G.ignite("server-1").configuration().getDiscoverySpi());

            assert firstSpi.err == null;

            secondSpi.readDelay = failureDetectionTimeout() + 5000;

            assertFalse(firstSpi.pingNode(secondSpi.getLocalNodeId()));

            Thread.sleep(failureDetectionTimeout());

            assertTrue(firstSpi.err != null && X.hasCause(firstSpi.err, SocketTimeoutException.class));

            firstSpi.reset();
            secondSpi.reset();

            assertTrue(firstSpi.pingNode(secondSpi.getLocalNodeId()));

            assertTrue(firstSpi.err == null);
        }
        finally {
            useTestSpi = false;

            if (firstSpi != null)
                firstSpi.reset();

            if (secondSpi != null)
                secondSpi.reset();
        }
    }

    /**
     *
     */
    private static class TestTcpDiscoverySpi extends TcpDiscoverySpi {
        /** */
        private long readDelay;

        /** */
        private Exception err;

        /** {@inheritDoc} */
        @Override protected <T> T readMessage(Socket sock, @Nullable InputStream in, long timeout)
            throws IOException, IgniteCheckedException {

            if (readDelay < failureDetectionTimeout()) {
                try {
                    return super.readMessage(sock, in, timeout);
                }
                catch (Exception e) {
                    err = e;

                    throw e;
                }
            }
            else {
                T msg = super.readMessage(sock, in, timeout);

                if (msg instanceof TcpDiscoveryPingRequest) {
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        // Ignore
                    }
                    throw new SocketTimeoutException("Forced timeout");
                }

                return msg;
            }
        }

        /**
         * Resets testing state.
         */
        private void reset() {
            readDelay = 0;
            err = null;
        }
    }
}