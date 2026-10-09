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

import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryMetricsUpdateMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test verifies there is no data race in ServerImpl code between handling {@link TcpDiscoveryMetricsUpdateMessage} messages
 * and sending them to client nodes.
 */
public class TestMetricUpdateFailure extends GridCommonAbstractTest {
    /** */
    private static final int MESSAGES = 10_000;

    /** */
    private static final int METRICS_FREQ = 10;

    /** */
    public static final int SRV_CNT = 1;

    /** */
    public static final int CLN_CNT = 20;

    /** */
    private static final AtomicLong msgCnt = new AtomicLong();

    /** */
    private LogListener concLsnr = LogListener.matches("ConcurrentModificationException").build();

    /** */
    private LogListener invalidLsnr = LogListener.matches("Invalid message type").build();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ListeningTestLogger log = new ListeningTestLogger(log());

        log.registerAllListeners(concLsnr, invalidLsnr);

        return cfg.setMetricsUpdateFrequency(METRICS_FREQ)
            .setGridLogger(log)
            .setDiscoverySpi(new TestDiscoverySpi());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        msgCnt.set(MESSAGES);

        assertFalse(concLsnr.check());
        assertFalse(invalidLsnr.check());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        concLsnr.reset();
        invalidLsnr.reset();

        stopAllGrids(true);
    }

    /** */
    @Test
    public void test() throws Exception {
        startGrids(SRV_CNT);
        startClientGridsMultiThreaded(SRV_CNT, CLN_CNT);

        waitForTopology(SRV_CNT + CLN_CNT);

        IgniteCache<Integer, Integer> clnCache = grid(SRV_CNT).createCache(
            new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
                .setStatisticsEnabled(true)
                .setBackups(1));

        IntStream.range(0, 10000)
            .forEach(i -> clnCache.put(i, i));

        GridTestUtils.waitForCondition(() -> msgCnt.get() <= 0 || concLsnr.check() || invalidLsnr.check(),
            getTestTimeout());

        assertFalse("Concurrent modification occured", concLsnr.check());
        assertFalse("Invalid message type found", invalidLsnr.check());

        checkTopology(SRV_CNT + CLN_CNT);
    }

    /** */
    private static class TestDiscoverySpi extends TcpDiscoverySpi {
        /** {@inheritDoc} */
        @Override protected void startMessageProcess(TcpDiscoveryAbstractMessage msg) {
            super.startMessageProcess(msg);

            if (msg instanceof TcpDiscoveryMetricsUpdateMessage)
                msgCnt.decrementAndGet();
        }
    }
}
