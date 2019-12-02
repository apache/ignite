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

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import com.google.common.collect.Lists;
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
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.IntGauge;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.node.VisorNodeDataCollectorTask;
import org.apache.ignite.internal.visor.node.VisorNodeDataCollectorTaskArg;
import org.apache.ignite.internal.visor.node.VisorNodeDataCollectorTaskResult;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_REBALANCE_STATISTICS_TIME_INTERVAL;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_STARTED;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_STOPPED;
import static org.apache.ignite.internal.processors.cache.CacheGroupMetricsImpl.CACHE_GROUP_METRICS_PREFIX;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 *
 */
public class CacheGroupsMetricsRebalanceTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE1 = "cache1";

    /** */
    private static final String CACHE2 = "cache2";

    /** */
    private static final String CACHE3 = "cache3";

    /** */
    private static final String CACHE4 = "cache4";

    /** */
    private static final String CACHE5 = "cache5";

    /** */
    private static final String CACHE6 = "cache6";

    /** */
    private static final long REBALANCE_DELAY_5000 = 5_000;

    /** */
    private static final long REBALANCE_DELAY_500 = 500;

    /** */
    private static final String GROUP1 = "group1";

    /** */
    private static final String GROUP2 = "group2";

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

        CacheConfiguration cfg1 = new CacheConfiguration()
            .setName(CACHE1)
            .setGroupName(GROUP1)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setRebalanceMode(CacheRebalanceMode.ASYNC)
            .setRebalanceBatchSize(100)
            .setStatisticsEnabled(true);

        CacheConfiguration cfg2 = new CacheConfiguration(cfg1)
            .setName(CACHE2);

        CacheConfiguration cfg3 = new CacheConfiguration()
            .setName(CACHE3)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setRebalanceMode(CacheRebalanceMode.ASYNC)
            .setRebalanceBatchSize(100)
            .setStatisticsEnabled(true)
            .setRebalanceDelay(REBALANCE_DELAY_5000);

        CacheConfiguration cfg4 = new CacheConfiguration()
            .setName(CACHE4)
            .setCacheMode(CacheMode.REPLICATED)
            .setRebalanceDelay(REBALANCE_DELAY_500)
            .setGroupName(GROUP2);

        CacheConfiguration cfg5 = new CacheConfiguration(cfg4)
            .setName(CACHE5);

        CacheConfiguration cfg6 = new CacheConfiguration(cfg4)
            .setName(CACHE6);

        cfg.setCacheConfiguration(cfg1, cfg2, cfg3, cfg4, cfg5, cfg6);

        cfg.setIncludeEventTypes(EventType.EVTS_ALL);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheRebalance() throws Exception {
        Ignite ignite = startGrids(4);

        IgniteCache<Object, Object> cache1 = ignite.cache(CACHE1);
        IgniteCache<Object, Object> cache2 = ignite.cache(CACHE2);

        for (int i = 0; i < 10_000; i++) {
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

        // rate1 has to be roughly the same as rate2
        double ratio = ((double)rate2 / rate1);

        log.info("Ratio: " + ratio);

        assertTrue(ratio > 0.9 && ratio < 1.1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheGroupRebalance() throws Exception {
        int keysCount = 30_000;
        int timeout = 5 * 1000;
        int valueSize = 64;

        List<String> cachesNames = Lists.newArrayList(CACHE4, CACHE5, CACHE6);

        IgniteEx ignite = startGrid(1);

        for (String name : cachesNames) {
            IgniteCache<Integer, byte[]> cache = ignite.getOrCreateCache(name);

            for (int i = 0; i < keysCount; i++) {
                cache.put(i, new byte[valueSize]);
            }
        }

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(cachesNames.size());

        (ignite = startGrid(2)).events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                String cacheName = ((CacheRebalancingEvent)evt).cacheName();
                boolean started = evt.type() == EVT_CACHE_REBALANCE_STARTED;

                if (cachesNames.contains(cacheName)) {
                    if (started)
                        startLatch.countDown();
                    else
                        finishLatch.countDown();
                }

                return true;
            }
        }, EVT_CACHE_REBALANCE_STARTED, EVT_CACHE_REBALANCE_STOPPED);

        assertTrue("Rebalancing for caches " + cachesNames + " not started",
            startLatch.await(timeout, TimeUnit.MILLISECONDS));

        MetricRegistry mreg = ignite.context().metric().registry(
            metricName(CACHE_GROUP_METRICS_PREFIX, GROUP2));

        LongMetric rebalancingStartTimeMetric = mreg.findMetric("RebalancingStartTime");
        LongMetric rebalancingFinishTimeMetric = mreg.findMetric("RebalancingFinishTime");
        LongMetric rebalancingReceivedKeysMetric = mreg.findMetric("RebalancingReceivedKeys");

        IntGauge rebalancingPartitionsLeftMetric = mreg.findMetric("RebalancingPartitionsLeft");

        waitForCondition(() -> rebalancingStartTimeMetric.value() != -1, timeout);

        long rebalancingStartTime = rebalancingStartTimeMetric.value();
        long currentTimeMillis = System.currentTimeMillis();

        assertTrue("Invalid rebalancing start time [rebalancingStartTime=" + rebalancingStartTime +
            ", currentTime=" + currentTimeMillis + ", delay=" + REBALANCE_DELAY_500 + "]",
            rebalancingStartTime <= currentTimeMillis);

        long rebalancingFinishTime = rebalancingFinishTimeMetric.value();

        assertEquals("Rebalancing finish time must be -1 before rebalancing stops " +
            "[rebalancingFinishTime=" + rebalancingFinishTime + "]", -1L, rebalancingFinishTime);

        waitForCondition(() -> rebalancingPartitionsLeftMetric.value() > 0, timeout);

        int rebalancingPartitionsLeft = rebalancingPartitionsLeftMetric.value();

        assertTrue("Not a positive rebalancing partitions left:" + rebalancingPartitionsLeft,
            rebalancingPartitionsLeft > 0);

        assertTrue("Rebalancing stopped for " + (cachesNames.size() - finishLatch.getCount()) + " caches from " +
                cachesNames.size(), finishLatch.await(timeout, TimeUnit.MILLISECONDS));

        waitForCondition(() -> rebalancingFinishTimeMetric.value() != -1, timeout);

        rebalancingFinishTime = rebalancingFinishTimeMetric.value();

        assertTrue("Invalid rebalancing finish time [rebalancingStartTime=" + rebalancingStartTime +
            ", rebalancingFinishTime=" + rebalancingFinishTime + "]", rebalancingStartTime < rebalancingFinishTime);

        assertTrue("Invalid rebalancing finish time [rebalancingFinishTime=" + rebalancingFinishTime +
            ", currentTime=" + U.currentTimeMillis() + "]", rebalancingFinishTime <= U.currentTimeMillis());

        rebalancingPartitionsLeft = rebalancingPartitionsLeftMetric.value();

        assertEquals("Not all partitions rebalanced:" + rebalancingPartitionsLeft, 0, rebalancingPartitionsLeft);

        waitForCondition(() -> rebalancingReceivedKeysMetric.value() == cachesNames.size() * keysCount, timeout);

        long rebalancingReceivedKeys = rebalancingReceivedKeysMetric.value();

        assertEquals("Not all keys received:" + rebalancingReceivedKeys + " from " + (keysCount *
                cachesNames.size()), rebalancingReceivedKeys, cachesNames.size() * keysCount);

        long rebalancingReceivedBytes = mreg.<LongMetric>findMetric("RebalancingReceivedBytes").value();

        assertTrue("Not all bytes received:" + rebalancingReceivedBytes,
            rebalancingReceivedBytes > rebalancingReceivedKeys * valueSize);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRebalanceProgressUnderLoad() throws Exception {
        Ignite ignite = startGrids(4);

        IgniteCache<Object, Object> cache1 = ignite.cache(CACHE1);

        Random r = new Random();

        GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < 100_000; i++) {
                    int next = r.nextInt();

                    cache1.put(next, CACHE1 + "-" + next);
                }
            }
        });

        IgniteEx ig = startGrid(4);

        GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < 100_000; i++) {
                    int next = r.nextInt();

                    cache1.put(next, CACHE1 + "-" + next);
                }
            }
        });

        CountDownLatch latch = new CountDownLatch(1);

        ig.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                latch.countDown();

                return false;
            }
        }, EventType.EVT_CACHE_REBALANCE_STOPPED);

        latch.await();

        VisorNodeDataCollectorTaskArg taskArg = new VisorNodeDataCollectorTaskArg();
        taskArg.setCacheGroups(Collections.emptySet());

        VisorTaskArgument<VisorNodeDataCollectorTaskArg> arg = new VisorTaskArgument<>(
            Collections.singletonList(ignite.cluster().localNode().id()),
            taskArg,
            false
        );

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                VisorNodeDataCollectorTaskResult res = ignite.compute().execute(VisorNodeDataCollectorTask.class, arg);

                CacheMetrics snapshot = ig.cache(CACHE1).metrics();

                return snapshot.getRebalancedKeys() > snapshot.getEstimatedRebalancingKeys()
                    && Double.compare(res.getRebalance().get(ignite.cluster().localNode().id()), 1.0) == 0
                    && snapshot.getRebalancingPartitionsCount() == 0;
            }
        }, 5000);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRebalanceEstimateFinishTime() throws Exception {
        System.setProperty(IGNITE_REBALANCE_STATISTICS_TIME_INTERVAL, String.valueOf(1000));

        Ignite ig1 = startGrid(1);

        final int KEYS = 4_000_000;

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
                    log.info("CountDown rebalance stop latch: " + rebEvent.cacheName());

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
        long currTime = U.currentTimeMillis();

        assertTrue("Invalid start time [startTime=" + startTime + ", currTime=" + currTime + ']',
            startTime > 0L && (currTime - startTime) >= 0L && (currTime - startTime) <= 5000L);

        final CountDownLatch latch = new CountDownLatch(1);

        runAsync(new Runnable() {
            @Override public void run() {
                // Waiting 25% keys will be rebalanced.
                int partKeys = KEYS / 2;

                final long keysLine = partKeys * 3L / 4L;

                log.info("Wait until keys left will be less than: " + keysLine);

                while (true) {
                    CacheMetrics m = ig2.cache(CACHE1).localMetrics();

                    long keyLeft = m.getKeysToRebalanceLeft();

                    if (keyLeft > 0 && keyLeft < keysLine) {
                        latch.countDown();

                        break;
                    }

                    log.info("Keys left: " + m.getKeysToRebalanceLeft());

                    try {
                        Thread.sleep(1_000);
                    }
                    catch (InterruptedException e) {
                        log.warning("Interrupt thread", e);

                        Thread.currentThread().interrupt();
                    }
                }
            }
        });

        assertTrue(latch.await(getTestTimeout(), TimeUnit.MILLISECONDS));

        waitForCondition(new PA() {
            @Override public boolean apply() {
                return ig2.cache(CACHE1).localMetrics().getEstimatedRebalancingFinishTime() != -1L;
            }
        }, 5_000L);

        long finishTime = ig2.cache(CACHE1).localMetrics().getEstimatedRebalancingFinishTime();

        assertTrue("Not a positive estimation of rebalancing finish time: " + finishTime,
            finishTime > 0L);

        currTime = U.currentTimeMillis();

        long timePassed = currTime - startTime;
        long timeLeft = finishTime - currTime;

        // TODO: finishRebalanceLatch gets countdown much earlier because of ForceRebalanceExchangeTask triggered by cache with delay
//        assertTrue("Got timeout while waiting for rebalancing. Estimated left time: " + timeLeft,
//            finishRebalanceLatch.await(timeLeft + 10_000L, TimeUnit.MILLISECONDS));

        waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return ig2.cache(CACHE1).localMetrics().getKeysToRebalanceLeft() == 0;
            }
        }, timeLeft + 12_000L);

        log.info("[timePassed=" + timePassed + ", timeLeft=" + timeLeft +
                ", Time to rebalance=" + (finishTime - startTime) +
                ", startTime=" + startTime + ", finishTime=" + finishTime + ']'
        );

        System.clearProperty(IGNITE_REBALANCE_STATISTICS_TIME_INTERVAL);

        currTime = U.currentTimeMillis();

        log.info("Rebalance time: " + (currTime - startTime));

        long diff = finishTime - currTime;

        assertTrue("Expected less than 12000, but actual: " + diff, Math.abs(diff) < 12_000L);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRebalanceDelay() throws Exception {
        Ignite ig1 = startGrid(1);

        final IgniteCache<Object, Object> cache = ig1.cache(CACHE3);

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

        assert(cache.localMetrics().getRebalancingStartTime() < U.currentTimeMillis() + REBALANCE_DELAY_5000);
        assert(cache.localMetrics().getRebalancingStartTime() > beforeStartTime + REBALANCE_DELAY_5000);
    }
}
