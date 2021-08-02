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

import java.lang.reflect.Method;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManagerImpl;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheOffheapManager;
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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Testing the release of WAL segments during historical rebalance.
 */
@WithSystemProperty(key = IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "0")
public class ReleaseSegmentOnHistoricalRebalanceTest extends GridCommonAbstractTest {
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

    /**
     * Checks that if release the segment after {@link CheckpointHistory#searchAndReserveCheckpoints},
     * there will be no errors and the rebalance will be completed.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReleaseSegmentAfterSearchAndReserveCheckpoints() throws Exception {
        checkHistoricalRebalance(n -> {
            CheckpointHistory spy = spy(dbMgr(n).checkpointHistory());

            when(spy.searchAndReserveCheckpoints(any())).thenAnswer(m -> {
                CheckpointHistoryResult res = (CheckpointHistoryResult)m.callRealMethod();

                WALPointer reserved = res.reservedCheckpointMark();
                assertNotNull(reserved);

                release(n, reserved);

                return res;
            });

            checkpointHistory(n, spy);
        });
    }

    /**
     * Checks that if release the segment before {@link GridCacheDatabaseSharedManager#releaseHistoryForExchange},
     * there will be no errors and the rebalance will be completed.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReleaseBeforeReleaseHistoryForExchange() throws Exception {
        checkHistoricalRebalance(n -> {
            GridCacheDatabaseSharedManager spy = spy(dbMgr(n));

            doAnswer(m -> {
                release(n, getFieldValue(spy, "reservedForExchange"));

                return m.callRealMethod();
            }).when(spy).releaseHistoryForExchange();

            databaseManager(n, spy);
        });
    }

    /**
     * Checks that if there is no segment reservation in {@link GridCacheDatabaseSharedManager#reserveHistoryForPreloading},
     * there will be no errors and the rebalance will be completed.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNoReserveHistoryForPreloading() throws Exception {
        checkHistoricalRebalance(n -> {
            GridCacheDatabaseSharedManager spy = spy(dbMgr(n));

            when(spy.reserveHistoryForPreloading(any())).thenAnswer(m -> false);

            databaseManager(n, spy);
        });
    }

    /**
     * Checks that if release the segment before {@link GridCacheDatabaseSharedManager#releaseHistoryForPreloading},
     * there will be no errors and the rebalance will be completed.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReleaseBeforeReleaseHistoryForPreloading() throws Exception {
        checkHistoricalRebalance(n -> {
            GridCacheDatabaseSharedManager spy = spy(dbMgr(n));

            doAnswer(m -> {
                release(n, spy.latestWalPointerReservedForPreloading());

                return m.callRealMethod();
            }).when(spy).releaseHistoryForPreloading();

            databaseManager(n, spy);
        });
    }

    /**
     * Checks that if release the segment before {@link IgniteCacheOffheapManagerImpl#rebalanceIterator},
     * there will be no errors and the rebalance will be completed.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReleaseBeforeRebalanceIterator() throws Exception {
        checkHistoricalRebalance(n -> {
            IgniteInternalCache<?, ?> cachex = n.cachex(DEFAULT_CACHE_NAME);

            GridCacheOffheapManager spy = spy(offheapManager(cachex));

            doAnswer(m -> {
                CheckpointHistory cpHist = dbMgr(n).checkpointHistory();

                for (Long cp : cpHist.checkpoints())
                    release(n, entry(cpHist, cp).checkpointMark());

                return m.callRealMethod();
            }).when(spy).rebalanceIterator(any(), any());

            offheapManager(cachex, spy);
        });
    }

    /**
     * Checks that if release the segment during {@link IgniteCacheOffheapManagerImpl#rebalanceIterator},
     * there will be no errors and the rebalance will be completed.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReleaseDuringRebalanceIterator() throws Exception {
        checkHistoricalRebalance(n -> {
            IgniteInternalCache<?, ?> cachex = n.cachex(DEFAULT_CACHE_NAME);

            GridCacheOffheapManager spy = spy(offheapManager(cachex));

            doAnswer(m -> {
                WALPointer reserved = dbMgr(n).latestWalPointerReservedForPreloading();

                assertNotNull(reserved);

                Object o = m.callRealMethod();

                release(n, reserved);

                assertTrue(segmentAware(n).minReserveIndex(Long.MAX_VALUE));

                return o;
            }).when(spy).rebalanceIterator(any(), any());

            offheapManager(cachex, spy);
        });
    }

    /**
     * Checks that if the reservation is released immediately,
     * there will be no errors and the rebalance will be completed.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testImmediateReleaseSegment() throws Exception {
        checkHistoricalRebalance(n -> {
            SegmentAware spy = spy(segmentAware(n));

            doAnswer(m -> {
                Object o = m.callRealMethod();

                spy.release(m.getArgument(0));

                return o;
            }).when(spy).reserve(anyLong());

            segmentAware(n, spy);
        });
    }

    /**
     * Checks that that if there is no reservation,
     * there will be no errors and the rebalance will be completed.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNoReserveSegment() throws Exception {
        checkHistoricalRebalance(n -> {
            SegmentAware spy = spy(segmentAware(n));

            when(spy.reserve(anyLong())).thenAnswer(m -> false);

            segmentAware(n, spy);
        });
    }

    /**
     * Populates the given cache and forces a new checkpoint every 100 updates.
     *
     * @param cache Cache.
     * @param cnt Entry count.
     * @param o Key offset.
     * @throws Exception If failed.
     */
    private void populate(IgniteCache<Integer, ? super Object> cache, int cnt, int o) throws Exception {
        for (int i = 0; i < cnt; i++) {
            if (i % 100 == 0)
                forceCheckpoint();

            cache.put(i + o, new byte[64 * 1024]);
        }
    }

    /**
     * Sets the spy to {@code CheckpointMarkersStorage#cpHistory}.
     *
     * @param n Node.
     * @param spy Spy.
     */
    private void checkpointHistory(IgniteEx n, CheckpointHistory spy) {
        CheckpointMarkersStorage s = getFieldValue(dbMgr(n), "checkpointManager", "checkpointMarkersStorage");

        setFieldValue(s, "cpHistory", spy);
    }

    /**
     * Sets the spy to {@code GridCacheSharedContext#dbMgr}.
     *
     * @param n Node.
     * @param spy Spy.
     */
    private void databaseManager(IgniteEx n, GridCacheDatabaseSharedManager spy) {
        setFieldValue(n.context().cache().context(), "dbMgr", spy);
    }

    /**
     * Sets the spy to {@code CacheGroupContext#offheapMgr}.
     *
     * @param cache Cache.
     * @param spy Spy.
     */
    private void offheapManager(IgniteInternalCache<?, ?> cache, GridCacheOffheapManager spy) {
        setFieldValue(cache.context().group(), "offheapMgr", spy);
    }

    /**
     * Sets the spy to {@code FileWriteAheadLogManager#segmentAware}.
     *
     * @param n Node.
     * @param spy Spy.
     */
    private void segmentAware(IgniteEx n, SegmentAware spy) {
        setFieldValue(walMgr(n), "segmentAware", spy);
    }

    /**
     * Releases WAL segment.
     *
     * @param n Node.
     * @param reserved Reserved segment.
     */
    private void release(IgniteEx n, @Nullable WALPointer reserved) {
        while (reserved != null && walMgr(n).reserved(reserved))
            walMgr(n).release(reserved);
    }

    /**
     * Returns an instance of {@link SegmentAware} for the given ignite node.
     *
     * @return Segment aware.
     */
    private SegmentAware segmentAware(IgniteEx n) {
        return getFieldValue(walMgr(n), "segmentAware");
    }

    /**
     * Returns an instance of {@link GridCacheOffheapManager} for the given ignite node.
     *
     * @param cache Cache.
     * @return Offheap manager.
     */
    private GridCacheOffheapManager offheapManager(IgniteInternalCache<?, ?> cache) {
        return (GridCacheOffheapManager)cache.context().group().offheap();
    }

    /**
     * Invokes the {@code CheckpointHistory#entry}.
     *
     * @param cpHist Checkpoint history.
     * @param cpTs Checkpoint timestamp.
     * @return Checkpoint entry.
     */
    private CheckpointEntry entry(CheckpointHistory cpHist, Long cpTs) throws Exception {
        Method entry = U.getNonPublicMethod(cpHist.getClass(), "entry", cpTs.getClass());

        return (CheckpointEntry)entry.invoke(cpHist, cpTs);
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

        IgniteEx n1 = startGrid(1);
        awaitPartitionMapExchange();

        assertEquals(2, G.allGrids().size());

        stopGrid(0);
        awaitPartitionMapExchange();

        for (int i = 0; i < 2_000; i++)
            assertNotNull(String.valueOf(i), n1.cache(DEFAULT_CACHE_NAME).get(i));
    }
}
