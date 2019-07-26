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

package org.apache.ignite.spi.discovery;

import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.CURRENT_PME_CACHE_OPERATIONS_BLOCKED_DURATION;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.CURRENT_PME_DURATION;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.PME_CACHE_OPERATIONS_BLOCKED_DURATION;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.PME_DURATION;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Cluster metrics test.
 */
@GridCommonTest(group = "Utils")
public class ClusterMetricsSelfTest extends GridCommonAbstractTest {
    /** */
    private static final long METRIC_UPDATE_FREQUENCY = 100;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setMetricsUpdateFrequency(METRIC_UPDATE_FREQUENCY);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        boolean client = igniteInstanceName.startsWith("client");

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPmeMetricsWithBlockingEvent() throws Exception {
        checkPmeMetricsOnNodeJoin(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPmeMetricsWithNotBlockingEvent() throws Exception {
        checkPmeMetricsOnNodeJoin(true);
    }

    /**
     * @param client Client flag.
     * @throws Exception If failed.
     */
    private void checkPmeMetricsOnNodeJoin(boolean client) throws Exception {
        IgniteEx ignite = startGrid(0);

        MetricRegistry reg = ignite.context().metric().registry(GridMetricManager.PME_METRICS);

        LongMetric currentPMEDuration = reg.findMetric(CURRENT_PME_DURATION);
        LongMetric currentBlockingPMEDuration = reg.findMetric(CURRENT_PME_CACHE_OPERATIONS_BLOCKED_DURATION);

        HistogramMetric durationHistogram = reg.findMetric(PME_DURATION);
        HistogramMetric blockindDurationHistogram = reg.findMetric(PME_CACHE_OPERATIONS_BLOCKED_DURATION);

        IgniteCache<Object, Object> cache = ignite.getOrCreateCache(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setAtomicityMode(TRANSACTIONAL));

        cache.put(1, 1);

        awaitPartitionMapExchange();

        assertTrue(GridTestUtils.waitForCondition(() -> currentPMEDuration.value() == 0, 1000));
        assertTrue(currentBlockingPMEDuration.value() == 0);

        // There was two blocking exchange: server node start and cache start.
        assertTrue(Arrays.stream(durationHistogram.value()).sum() == 2);
        assertTrue(Arrays.stream(blockindDurationHistogram.value()).sum() == 2);

        Lock lock = cache.lock(1);

        lock.lock();

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(ignite);

        spi.blockMessages((node, message) -> message instanceof GridDhtPartitionsFullMessage);

        GridTestUtils.runAsync(() -> client ? startGrid("client") : startGrid(1));

        assertTrue(waitForCondition(() ->
            ignite.context().cache().context().exchange().lastTopologyFuture().initialVersion().topologyVersion() == 2,
            1000));

        // Check cluster group metric because on server-side PME completes locally.
        assertTrue(GridTestUtils.waitForCondition(() -> ignite.cluster().metrics().getCurrentPmeDuration() > 0, 1000));

        if (client)
            assertTrue(currentBlockingPMEDuration.value() == 0);
        else
            assertTrue(currentBlockingPMEDuration.value() > 0);

        lock.unlock();

        spi.waitForBlocked();
        spi.stopBlock();

        awaitPartitionMapExchange();

        assertTrue(GridTestUtils.waitForCondition(() -> currentPMEDuration.value() == 0, 1000));
        assertTrue(currentBlockingPMEDuration.value() == 0);

        if (client) {
            // There was non-blocking exchange: client node start.
            assertTrue(Arrays.stream(durationHistogram.value()).sum() == 3);
            assertTrue(Arrays.stream(blockindDurationHistogram.value()).sum() == 2);
        }
        else {
            // There was two blocking exchange: server node start and rebalance completing.
            assertTrue(Arrays.stream(durationHistogram.value()).sum() == 4);
            assertTrue(Arrays.stream(blockindDurationHistogram.value()).sum() == 4);
        }
    }
}
