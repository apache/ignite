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
import java.util.concurrent.Exchanger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryConnectionCheckMessage;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Checks pinging next node in the ring relies on configured timeouts.
 */
public class ConnectionCheckingTest extends GridCommonAbstractTest {
    /**
     * Maximal additional delay before sending the ping message. 10 ms is the granulation in {@link
     * IgniteUtils#currentTimeMillis()} and other 10ms is for code.
     */
    private static final int ACCEPTABLE_CODE_DELAYS = 10 + 10;

    /** Number of the ping messages to watch to ensure node pinging works well. */
    private static final int PING_MESSAGES_CNT_TO_ENSURE = 10;

    /** Checks connection to next node is checked depending on configured failure detection timeout. */
    @Test
    public void testWithFailureDetectionTimeout() throws Exception {
        for (long failureDetectionTimeout = 200; failureDetectionTimeout <= 600; failureDetectionTimeout += 100) {
            IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(1));

            cfg.setFailureDetectionTimeout(failureDetectionTimeout);

            launchTest(cfg);
        }
    }

    /** Checks connection to next node is checked depending on configured socket and acknowledgement timeouts. */
    @Test
    public void testWithSocketAndAckTimeouts() throws Exception {
        for (long sockTimeout = 200; sockTimeout <= 600; sockTimeout += 200) {
            for (long ackTimeout = 200; ackTimeout <= 600; ackTimeout += 200) {
                IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(1));

                cfg.setFailureDetectionTimeout(sockTimeout);

                ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setSocketTimeout(sockTimeout);

                ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setAckTimeout(sockTimeout);

                ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setReconnectCount(1);

                launchTest(cfg);
            }
        }
    }

    /** */
    private void launchTest(IgniteConfiguration cfg) throws Exception {
        startGrid(0);

        Exchanger<String> errHolder = new Exchanger<>();

        TcpDiscoverySpi prevSpi = (TcpDiscoverySpi)cfg.getDiscoverySpi();

        TcpDiscoverySpi spi = tcpDiscoverySpi(errHolder);

        spi.setIpFinder(LOCAL_IP_FINDER);

        cfg.setDiscoverySpi(spi);

        if (!prevSpi.failureDetectionTimeoutEnabled()) {
            spi.setReconnectCount(prevSpi.getReconnectCount());

            spi.setSocketTimeout(prevSpi.getSocketTimeout());

            spi.setAckTimeout(prevSpi.getAckTimeout());
        }

        startGrid(cfg);

        String errMsg = errHolder.exchange(null);

        assertNull(errMsg, errMsg);

        stopAllGrids(true);
    }

    /**
     * @return Testing tcp discovery which monitors message traffic.
     */
    private TcpDiscoverySpi tcpDiscoverySpi(Exchanger<String> errHolder) {
        return new TcpDiscoverySpi() {
            /** Last sent message. */
            private final AtomicReference<TcpDiscoveryAbstractMessage> lastMsg = new AtomicReference<>();

            /** Time of the last sent message. */
            private long lastSentMsgTime;

            /** Cycles counter. */
            private long cycles;

            /** Stop flag. */
            private boolean stop;

            /** */
            @Override protected void writeToSocket(Socket sock, OutputStream out, TcpDiscoveryAbstractMessage msg,
                long timeout) throws IOException, IgniteCheckedException {
                super.writeToSocket(sock, out, msg, timeout);

                TcpDiscoveryAbstractMessage prevMsg = lastMsg.getAndSet(msg);

                if (msg instanceof TcpDiscoveryConnectionCheckMessage) {
                    synchronized (lastMsg) {
                        if (!stop && prevMsg instanceof TcpDiscoveryConnectionCheckMessage) {
                            long period = System.currentTimeMillis() - lastSentMsgTime;

                            lastSentMsgTime = System.currentTimeMillis();

                            long properTimeout = failureDetectionTimeoutEnabled() ? failureDetectionTimeout() :
                                getSocketTimeout();

                            if (period > properTimeout / 2 + ACCEPTABLE_CODE_DELAYS
                                || period < properTimeout / 2 - 10) {
                                stop("Invalid node ping interval: " + period + ". Expected value is half of actual " +
                                    "message exchange timeout which is: " + properTimeout);
                            }
                            else if (failureDetectionTimeoutEnabled() && timeout > properTimeout / 2) {
                                stop("Invalid timeout on writting TcpDiscoveryConnectionCheckMessage: " + timeout +
                                    ". Expected value is half of IgniteConfiguration.getFailureDetectionTimeout()" +
                                    " which is: " + properTimeout);
                            }
                            else if (!failureDetectionTimeoutEnabled() && timeout > properTimeout / 2) {
                                stop("Invalid timeout on writting TcpDiscoveryConnectionCheckMessage: " + timeout +
                                    ". Expected value is half of TcpDiscoverySpi.getSocketTimeout() which is: " +
                                    properTimeout);
                            }
                            else if (++cycles == PING_MESSAGES_CNT_TO_ENSURE)
                                stop(null);
                        }
                    }
                }
            }

            /** */
            @Override protected int readReceipt(Socket sock, long timeout) throws IOException {
                int res = super.readReceipt(sock, timeout);

                synchronized (lastMsg) {
                    lastSentMsgTime = System.currentTimeMillis();

                    if (lastMsg.get() instanceof TcpDiscoveryConnectionCheckMessage) {
                        if (failureDetectionTimeoutEnabled() && timeout > failureDetectionTimeout() / 2) {
                            stop("Invalid timeout on reading acknowledgement for TcpDiscoveryConnectionCheckMessage: " +
                                timeout + ". Expected value is half of IgniteConfiguration.failureDetectionTimeout " +
                                "which is: " + failureDetectionTimeout());
                        }
                        else if (!failureDetectionTimeoutEnabled() && timeout > getAckTimeout() / 2) {
                            stop("Invalid timeout on reading acknowledgement for TcpDiscoveryConnectionCheckMessage: " +
                                timeout + ". Expected value is half of TcpDiscoverySpi.ackTimeout " +
                                "which is: " + getAckTimeout());
                        }
                    }
                }

                return res;
            }

            /** Stops watching messages and notifies the exchanger. */
            private void stop(String errMsg) {
                stop = true;

                try {
                    errHolder.exchange(errMsg);
                }
                catch (InterruptedException e) {
                    // No-op.
                }
            }
        };
    }

    /** */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setSystemWorkerBlockedTimeout(20_000);

        //Prevent other discovery messages.
        cfg.setMetricsUpdateFrequency(24 * 60 * 60_000);

        cfg.setClientFailureDetectionTimeout(cfg.getMetricsUpdateFrequency());

        return cfg;
    }

    /** */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids(true);
    }
}
