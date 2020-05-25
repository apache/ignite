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

import static com.sun.tools.javac.util.Assert.check;

/**
 * Checks pinging next node in the ring relies on configured timeouts.
 */
public class ConnectionCheckingTest extends GridCommonAbstractTest {
    /** Number of the ping messages to watch to ensure node pinging works well. */
    private static final int PING_MESSAGES_CNT_TO_ENSURE = 10;

    /** Timer granulation in milliseconds. See {@link IgniteUtils#currentTimeMillis()}. */
    private static final int TIMER_GRANULATION = 10;

    /**
     * Maximal additional delay before sending the ping message including timer granulation in and other 10ms in code.
     */
    private static final int ACCEPTABLE_ADDITIONAL_DELAY = TIMER_GRANULATION + 10;

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
        for (long sockTimeout = 200; sockTimeout <= 400; sockTimeout += 100) {
            for (long ackTimeout = 200; ackTimeout <= 400; ackTimeout += 100) {
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

        check(errMsg == null, errMsg);

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

            /** {@inheritDoc} */
            @Override protected void writeToSocket(Socket sock, OutputStream out, TcpDiscoveryAbstractMessage msg,
                long timeout) throws IOException, IgniteCheckedException {
                super.writeToSocket(sock, out, msg, timeout);

                TcpDiscoveryAbstractMessage prevMsg = lastMsg.getAndSet(msg);

                if (msg instanceof TcpDiscoveryConnectionCheckMessage) {
                    synchronized (lastMsg) {
                        if (!stop && prevMsg instanceof TcpDiscoveryConnectionCheckMessage) {
                            long period = System.currentTimeMillis() - lastSentMsgTime;

                            lastSentMsgTime = System.currentTimeMillis();

                            long msgExchangeTimeout = failureDetectionTimeoutEnabled() ? failureDetectionTimeout() :
                                getSocketTimeout() + getAckTimeout();

                            if (period > msgExchangeTimeout / 2 + ACCEPTABLE_ADDITIONAL_DELAY ||
                                period < msgExchangeTimeout / 2 - TIMER_GRANULATION) {
                                stop("Invalid interval of sending TcpDiscoveryConnectionCheckMessage: " + period +
                                    "ms. Expected value is near " + msgExchangeTimeout / 2 + "ms, half of message " +
                                    "exchange timeout (" + msgExchangeTimeout + "ms).");
                            }
                            else if (failureDetectionTimeoutEnabled() &&
                                timeout > msgExchangeTimeout / 2 + TIMER_GRANULATION) {
                                stop("Invalid timeout on sending TcpDiscoveryConnectionCheckMessage: " + timeout +
                                    "ms. Expected value is near " + failureDetectionTimeout() / 2 + "ms, half of " +
                                    "IgniteConfiguration.failureDetectionTimeout (" + msgExchangeTimeout + "ms).");
                            }
                            else if (++cycles == PING_MESSAGES_CNT_TO_ENSURE)
                                stop(null);
                        }
                    }
                }
            }

            /** {@inheritDoc} */
            @Override protected int readReceipt(Socket sock, long timeout) throws IOException {
                int res = super.readReceipt(sock, timeout);

                synchronized (lastMsg) {
                    lastSentMsgTime = System.currentTimeMillis();

                    if ((lastMsg.get() instanceof TcpDiscoveryConnectionCheckMessage) &&
                        failureDetectionTimeoutEnabled() &&
                        timeout > failureDetectionTimeout() / 2 + TIMER_GRANULATION) {
                        stop("Invalid timeout set on reading acknowledgement for TcpDiscoveryConnectionCheckMessage: " +
                            timeout + "ms. Expected value is up to " + failureDetectionTimeout() / 2 + "ms, half of " +
                            "IgniteConfiguration.failureDetectionTimeout (" + failureDetectionTimeout() + "ms).");
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

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setSystemWorkerBlockedTimeout(20_000);

        //Prevent other discovery messages.
        cfg.setMetricsUpdateFrequency(24 * 60 * 60_000);

        cfg.setClientFailureDetectionTimeout(cfg.getMetricsUpdateFrequency());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids(true);
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 3 * 60 * 1000;
    }
}
