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

package org.apache.ignite.internal.processors.performancestatistics;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.INACTIVE;

/**
 * Tests rebalance performance statistics.
 */
public class RebalanceTest extends AbstractPerformanceStatisticsTest {
    /** Whether node starts with persistence enabled. */
    private volatile boolean persistenceEnabled;

    /** */
    private final ListeningTestLogger logListener = new ListeningTestLogger();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setMetricsEnabled(true)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(persistenceEnabled)));

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
            .setBackups(1));

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());
        cfg.setClusterStateOnStart(INACTIVE);
        cfg.setGridLogger(logListener);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();

        persistenceEnabled = false;
    }

    /** @throws Exception If failed. */
    @Test
    public void testRebalanceInMemory() throws Exception {
        runTest();
    }

    /** @throws Exception If failed. */
    @Test
    public void testRebalancePersistence() throws Exception {
        persistenceEnabled = true;

        runTest();
    }

    /** @throws Exception If failed. */
    private void runTest() throws Exception {
        IgniteEx srv = startGrid();

        srv.cluster().state(ClusterState.ACTIVE);

        srv.cluster().baselineAutoAdjustEnabled(false);

        IgniteCache<Long, Long> cache = srv.getOrCreateCache(DEFAULT_CACHE_NAME);

        cache.put(ThreadLocalRandom.current().nextLong(1024), ThreadLocalRandom.current().nextLong());

        LogListener lsnr = LogListener.matches("Completed rebalance chain").build();
        logListener.registerListener(lsnr);

        startCollectStatistics();

        long beforeStart = U.currentTimeMillis();

        IgniteEx node = startGrid(1);

        srv.cluster().setBaselineTopology(2);

        assertTrue(lsnr.check(TIMEOUT));

        AtomicInteger cntCF = new AtomicInteger();
        AtomicInteger cntPME = new AtomicInteger();

        stopCollectStatisticsAndRead(new TestHandler() {
            @Override public void rebalanceChainFinished(UUID nodeId, long rebalanceId, long parts, long entries,
                long bytes, long startTime, long duration) {

                assertEquals(node.localNode().id(), nodeId);
                assertTrue(rebalanceId > 0);
                assertTrue(parts > 0);
                assertEquals(1, entries);
                assertTrue(bytes > 0);
                assertTrue(startTime > beforeStart);
                assertTrue(duration > 0);

                cntCF.incrementAndGet();
            }

            @Override public void pme(UUID nodeId, long startTime, long duration, boolean rebalanced) {
                assertTrue(startTime > beforeStart);

                cntPME.incrementAndGet();
            }
        });

        assertEquals(1, cntCF.get());
        assertTrue(cntPME.get() > 0);
    }
}
