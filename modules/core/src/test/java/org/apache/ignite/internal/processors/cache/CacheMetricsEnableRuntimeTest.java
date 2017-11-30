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
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_REBALANCE_STATISTICS_TIME_INTERVAL;

/**
 *
 */
public class CacheMetricsEnableRuntimeTest extends GridCommonAbstractTest {
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
            .setRebalanceBatchSize(100);

        CacheConfiguration cfg2 = new CacheConfiguration(cfg1)
            .setName(CACHE2);

        CacheConfiguration cfg3 = new CacheConfiguration()
            .setName(CACHE3)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setRebalanceMode(CacheRebalanceMode.ASYNC)
            .setRebalanceBatchSize(100)
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
                CacheRebalancingEvent rebEvt = (CacheRebalancingEvent)evt;

                if (rebEvt.cacheName().equals(CACHE1)) {
                    System.out.println("CountDown rebalance stop latch:" + rebEvt.cacheName());

                    finishRebalanceLatch.countDown();
                }

                return false;
            }
        }, EventType.EVT_CACHE_REBALANCE_STOPPED);

        doSleep(500_000);
    }
}
