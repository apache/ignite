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

package org.apache.ignite.cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointEntry;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointHistory;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointHistoryResult;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointMarkersStorage;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.aware.SegmentAware;
import org.apache.ignite.internal.util.lang.IgniteThrowableConsumer;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;
import static org.apache.ignite.testframework.GridTestUtils.setFieldValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Testing the release of WAL segments during historical rebalance.
 */
@WithSystemProperty(key = IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "0")
public class ReleaseSegmentOnHistoricalRebalance extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setFailureHandler(new StopNodeFailureHandler())
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setWalSegmentSize((int)(2 * U.MB))
                    .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
            ).setCacheConfiguration(
                new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                    .setAffinity(new RendezvousAffinityFunction(false, 2))
                    .setBackups(1)
            );
    }

    @Test
    public void test0() throws Exception {
        checkHistoricalRebalance(n -> {
            CheckpointHistory spy = spy(dbMgr(n).checkpointHistory());

            when(spy.searchAndReserveCheckpoints(any())).thenAnswer(m -> {
                CheckpointHistoryResult res = (CheckpointHistoryResult)m.callRealMethod();

                WALPointer reserved = res.reservedCheckpointMark();
                assertNotNull(reserved);

                release(n, reserved);

                return res;
            });

            cpHistory(n, spy);
        });
    }

    @Test
    public void test1() throws Exception {
        checkHistoricalRebalance(n -> {
            GridCacheDatabaseSharedManager spy = spy(dbMgr(n));

            doAnswer(m -> {
                release(n, getFieldValue(spy, "reservedForExchange"));

                return m.callRealMethod();
            }).when(spy).releaseHistoryForExchange();

            dbMgr(n, spy);
        });
    }

    @Test
    public void test2() throws Exception {
        checkHistoricalRebalance(n -> {
            GridCacheDatabaseSharedManager spy = spy(dbMgr(n));

            when(spy.reserveHistoryForPreloading(any())).thenAnswer(m -> {
                WALPointer reserved = dbMgr(n).checkpointHistory().searchCheckpointEntry(m.getArgument(0))
                    .values().stream().map(CheckpointEntry::checkpointMark).min(WALPointer::compareTo).get();

                assertNotNull(reserved);

                release(n, reserved);

                assertTrue(segmentAware(n).minReserveIndex(reserved.index()));

                return m.callRealMethod();
            });

            dbMgr(n, spy);
        });
    }

    @Test
    public void test3() throws Exception {
        checkHistoricalRebalance(n -> {
            GridCacheDatabaseSharedManager spy = spy(dbMgr(n));

            doAnswer(m -> {
                release(n, getFieldValue(spy, "reservedForPreloading"));

                return m.callRealMethod();
            }).when(spy).releaseHistoryForPreloading();

            dbMgr(n, spy);
        });
    }

    /**
     * Populate the cache.
     *
     * @param cache Cache.
     * @param cnt Entry count.
     * @param o Key offset.
     */
    private void populate(IgniteCache<? super Object, ? super Object> cache, int cnt, int o) {
        for (int i = 0; i < cnt; i++)
            cache.put(i + o, new byte[64 * 1024]);
    }

    /**
     * Set the spy to {@code CheckpointMarkersStorage#cpHistory}.
     *
     * @param n Node.
     * @param spy Spy.
     */
    private void cpHistory(IgniteEx n, CheckpointHistory spy) {
        CheckpointMarkersStorage s = getFieldValue(dbMgr(n), "checkpointManager", "checkpointMarkersStorage");

        setFieldValue(s, "cpHistory", spy);
    }

    /**
     * Set the spy to {@code GridCacheSharedContext#dbMgr}.
     *
     * @param n Node.
     * @param spy Spy.
     */
    private void dbMgr(IgniteEx n, GridCacheDatabaseSharedManager spy) {
        setFieldValue(n.context().cache().context(), "dbMgr", spy);
    }

    /**
     * Release WAL segment.
     *
     * @param n Node.
     * @param reserved Reserved segment.
     */
    private void release(IgniteEx n, @Nullable WALPointer reserved) {
        while (reserved != null && walMgr(n).reserved(reserved))
            walMgr(n).release(reserved);
    }

    /**
     * Getting segment aware.
     *
     * @return Segment aware.
     */
    private SegmentAware segmentAware(IgniteEx n) {
        return getFieldValue(walMgr(n), "segmentAware");
    }

    /**
     * Checks that the historical rebalance will not fail the nodes.
     *
     * @param c Closure to be performed before the node returns to the topology, argument is a running node.
     * @throws Exception If failed.
     */
    private void checkHistoricalRebalance(IgniteThrowableConsumer<IgniteEx> c) throws Exception {
        IgniteEx n0 = startGrids(2);

        n0.cluster().state(ACTIVE);
        awaitPartitionMapExchange();

        populate(n0.cache(DEFAULT_CACHE_NAME), 1_000, 0);

        stopGrid(1);

        populate(n0.cache(DEFAULT_CACHE_NAME), 1_000, 1_000);

        c.accept(n0);

        startGrid(1);
        awaitPartitionMapExchange();

        assertEquals(2, G.allGrids().size());
    }
}
