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
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryMetricsUpdateMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** */
public class IgniteMetricsOverflowTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES_NUM = 6;

    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Released when first discovery message gets delayed. */
    private CountDownLatch slowDownLatch;

    /**
     * Period of time, for which {@link TcpDiscoverySpi#readReceipt(Socket, long)} execution is delayed on the node
     * with a slow {@link DiscoverySpi}.
     */
    private volatile int readReceiptDelay;

    /** If {@code true}, then {@code TestTcpDiscoverySpi} will be used. */
    private boolean slowDiscovery;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration configuration = super.getConfiguration(igniteInstanceName);

        configuration.setFailureDetectionTimeout(60_000);
        configuration.setMetricsUpdateFrequency(500);

        TcpDiscoverySpi discoverySpi;

        discoverySpi = slowDiscovery
            ? new TestTcpDiscoverySpi()
            : new TcpDiscoverySpi();

        discoverySpi.setIpFinder(IP_FINDER);

        configuration.setDiscoverySpi(discoverySpi);

        return configuration;
    }

    /** */
    @Before
    public void before() {
        readReceiptDelay = 0;
        slowDiscovery = false;
    }

    /** */
    @After
    public void after() {
        stopAllGrids();
    }

    /**
     * Checks a case when one discovery link processes messages slower than {@link TcpDiscoveryMetricsUpdateMessage}s
     * are generated. In such situation metrics updates shouldn't block processing of other discovery messages.
     *
     * This test doesn't have any asserts, since it will time out if discovery SPI is blocked.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMetricOverflow() throws Exception {
        slowDownLatch = new CountDownLatch(1);

        startGrids(NODES_NUM - 2);

        slowDiscovery = true;
        startGrid(NODES_NUM - 2);

        slowDiscovery = false;
        startGrid(NODES_NUM - 1);

        awaitPartitionMapExchange();

        IgniteInternalFuture<?> statsFut = GridTestUtils.runAsync(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                StringBuilder sb = new StringBuilder();
                sb.append(">>>>>> Queue sizes:");

                for (int i = 0; i < NODES_NUM; i++) {
                    Ignite ignite = grid(i);

                    TcpDiscoverySpi spi = (TcpDiscoverySpi)ignite.configuration().getDiscoverySpi();

                    sb.append(" ").append(spi.getMessageWorkerQueueSize());
                }

                log.info(sb.toString());

                try {
                    Thread.sleep(2_000);
                }
                catch (InterruptedException e) {
                    return;
                }
            }
        });

        try {
            readReceiptDelay = 1_000;

            slowDownLatch.await();

            IgniteInternalFuture<?> cacheCreateFut =
                GridTestUtils.runAsync(() -> grid(0).createCache("foo"));

            cacheCreateFut.get(30, TimeUnit.SECONDS);
        }
        finally {
            statsFut.cancel();
            statsFut.get();
        }
    }

    /** */
    private class TestTcpDiscoverySpi extends TcpDiscoverySpi {
        /** {@inheritDoc} */
        @Override protected int readReceipt(Socket sock, long timeout) throws IOException {
            if (readReceiptDelay > 0) {
                slowDownLatch.countDown();

                try {
                    Thread.sleep(readReceiptDelay);
                }
                catch (InterruptedException e) {
                    throw new IgniteException(e);
                }
            }

            return super.readReceipt(sock, timeout);
        }
    }
}
