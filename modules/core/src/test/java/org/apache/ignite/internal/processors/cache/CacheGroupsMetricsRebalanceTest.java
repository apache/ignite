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

package org.apache.ignite.internal.processors.cache;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheRebalancingEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_REBALANCE_STATISTICS_TIME_INTERVAL;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 *
 */
public class CacheGroupsMetricsRebalanceTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String CACHE1 = "cache1";

    /** */
    private static final String CACHE2 = "cache2";

    /** */
    private static final String CACHE3 = "cache3";

    /** */
    private static final long REBALANCE_DELAY = 5_000;

    /** */
    private static final String GROUP = "group1";

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 10 * 60 * 1000;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        CacheConfiguration cfg1 = new CacheConfiguration()
            .setName(CACHE1)
            .setGroupName(GROUP)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setRebalanceMode(CacheRebalanceMode.ASYNC)
            .setRebalanceBatchSize(100)
            .setStatisticsEnabled(true);

        CacheConfiguration cfg2 = new CacheConfiguration(cfg1)
            .setName(CACHE2);

        CacheConfiguration cfg3 = new CacheConfiguration()
            .setName(CACHE3)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setRebalanceMode(CacheRebalanceMode.ASYNC)
            .setRebalanceBatchSize(100)
            .setStatisticsEnabled(true)
            .setRebalanceDelay(REBALANCE_DELAY);

        cfg.setCacheConfiguration(cfg1, cfg2, cfg3);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testRebalance() throws Exception {
        Ignite ignite = startGrids(4);

        IgniteCache<Object, Object> cache1 = ignite.cache(CACHE1);
        IgniteCache<Object, Object> cache2 = ignite.cache(CACHE2);

        for (int i = 0; i < 10000; i++) {
            cache1.put(i, CACHE1 + "-" + i);

            if (i % 2 == 0)
                cache2.put(i, CACHE2 + "-" + i);
        }

        final CountDownLatch l1 = new CountDownLatch(1);
        final CountDownLatch l2 = new CountDownLatch(1);

        startGrid(4).events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                l1.countDown();

                try {
                    assertTrue(l2.await(5, TimeUnit.SECONDS));
                }
                catch (InterruptedException e) {
                    throw new AssertionError();
                }

                return false;
            }
        }, EventType.EVT_CACHE_REBALANCE_STOPPED);

        assertTrue(l1.await(5, TimeUnit.SECONDS));

        ignite = ignite(4);

        CacheMetrics metrics1 = ignite.cache(CACHE1).localMetrics();
        CacheMetrics metrics2 = ignite.cache(CACHE2).localMetrics();

        l2.countDown();

        long rate1 = metrics1.getRebalancingKeysRate();
        long rate2 = metrics2.getRebalancingKeysRate();

        assertTrue(rate1 > 0);
        assertTrue(rate2 > 0);

        // rate1 has to be roughly twice more than rate2.
        double ratio = ((double)rate2 / rate1) * 100;

        log.info("Ratio: " + ratio);

        assertTrue(ratio > 40 && ratio < 60);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRebalanceEstimateFinishTime() throws Exception {
        System.setProperty(IGNITE_REBALANCE_STATISTICS_TIME_INTERVAL, String.valueOf(1000));

        Ignite ig1 = startGrid(1);

        final int KEYS = 4_000_000;

        IgniteCache<Object, Object> cache1 = ig1.cache(CACHE1);

        try (IgniteDataStreamer<Integer, String> st = ig1.dataStreamer(CACHE1)) {
            for (int i = 0; i < KEYS; i++)
                st.addData(i, CACHE1 + "-" + i);
        }

        final CountDownLatch finishRebalanceLatch = new CountDownLatch(1);

        final Ignite ig2 = startGrid(2);

        ig2.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                CacheRebalancingEvent rebEvent = (CacheRebalancingEvent)evt;

                if (rebEvent.cacheName().equals(CACHE1)) {
                    System.out.println("CountDown rebalance stop latch:" + rebEvent.cacheName());

                    finishRebalanceLatch.countDown();
                }

                return false;
            }
        }, EventType.EVT_CACHE_REBALANCE_STOPPED);

        waitForCondition(new PA() {
            @Override public boolean apply() {
                return ig2.cache(CACHE1).localMetrics().getRebalancingStartTime() != -1L;
            }
        }, 5_000);

        CacheMetrics metrics = ig2.cache(CACHE1).localMetrics();

        long startTime = metrics.getRebalancingStartTime();

        assertTrue(startTime > 0);
        assertTrue((U.currentTimeMillis() - startTime) < 5000);
        assertTrue((U.currentTimeMillis() - startTime) > 0);

        final CountDownLatch latch = new CountDownLatch(1);

        runAsync(new Runnable() {
            @Override public void run() {
                // Waiting 25% keys will be rebalanced.
                int partKeys = KEYS / 2;

                final long keysLine = (long)(partKeys - (partKeys * 0.25));

                System.out.println("Wait until keys left will be less " + keysLine);

                while (finishRebalanceLatch.getCount() != 0) {
                    CacheMetrics m = ig2.cache(CACHE1).localMetrics();

                    long keyLeft = m.getKeysToRebalanceLeft();

                    if (keyLeft > 0 && keyLeft < keysLine)
                        latch.countDown();

                    System.out.println("Keys left: " + m.getKeysToRebalanceLeft());

                    try {
                        Thread.sleep(1_000);
                    }
                    catch (InterruptedException e) {
                        System.out.println("Interrupt thread: " + e.getMessage());

                        Thread.currentThread().interrupt();
                    }
                }
            }
        });

        latch.await();

        long finishTime = ig2.cache(CACHE1).localMetrics().getEstimatedRebalancingFinishTime();

        assertTrue(finishTime > 0);

        long timePassed = U.currentTimeMillis() - startTime;
        long timeLeft = finishTime - System.currentTimeMillis();

        assertTrue(finishRebalanceLatch.await(timeLeft + 2_000, TimeUnit.SECONDS));

        System.out.println(
            "TimePassed:" + timePassed +
                "\nTimeLeft:" + timeLeft +
                "\nTime to rebalance: " + (finishTime - startTime) +
                "\nStartTime: " + startTime +
                "\nFinishTime: " + finishTime
        );

        System.clearProperty(IGNITE_REBALANCE_STATISTICS_TIME_INTERVAL);

        System.out.println("Rebalance time:" + (U.currentTimeMillis() - startTime));

        long diff = finishTime - U.currentTimeMillis();

        assertTrue("Expected less 5000, Actual:" + diff, Math.abs(diff) < 10_000);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRebalanceDelay() throws Exception {
        Ignite ig1 = startGrid(1);

        IgniteCache<Object, Object> cache = ig1.cache(CACHE3);

        for (int i = 0; i < 10000; i++)
            cache.put(i, CACHE3 + "-" + i);

        long beforeStartTime = U.currentTimeMillis();

        startGrid(2);
        startGrid(3);

        waitForCondition(new PA() {
            @Override public boolean apply() {
                return cache.localMetrics().getRebalancingStartTime() != -1L;
            }
        }, 5_000);

        assert(cache.localMetrics().getRebalancingStartTime() < U.currentTimeMillis() + REBALANCE_DELAY);
        assert(cache.localMetrics().getRebalancingStartTime() > beforeStartTime + REBALANCE_DELAY);
    }
}
