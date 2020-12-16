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

package org.apache.ignite.internal.processors.cache.persistence.db;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_MAX_CHECKPOINT_MEMORY_HISTORY_SIZE;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Test correctness of truncating unused WAL segments.
 */
@WithSystemProperty(key = IGNITE_PDS_MAX_CHECKPOINT_MEMORY_HISTORY_SIZE, value = "2")
public class IgnitePdsReserveWalSegmentsTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setCacheConfiguration(
                new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                    .setAffinity(new RendezvousAffinityFunction(false, 32))
            ).setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setCheckpointFrequency(Long.MAX_VALUE)
                    .setWalMode(WALMode.LOG_ONLY)
                    .setMaxWalArchiveSize(DataStorageConfiguration.UNLIMITED_WAL_ARCHIVE)
                    .setWalSegmentSize(1024 * 1024)
                    .setWalSegments(10)
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setMaxSize(100 * 1024 * 1024)
                            .setPersistenceEnabled(true)
                    )
            );
    }

    /**
     * Tests that range reserved method return correct number of reserved WAL segments.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testWalManagerRangeReservation() throws Exception {
        IgniteEx n = prepareGrid(2);

        IgniteWriteAheadLogManager wal = n.context().cache().context().wal();

        assertNotNull(wal);

        long resIdx = getReservedWalSegmentIndex(wal);

        assertTrue("Expected that at least resIdx greater than 0, real is " + resIdx, resIdx > 0);

        WALPointer lowPtr = lastCheckpointPointer(n);

        assertTrue("Expected that dbMbr returns valid resIdx", lowPtr.index() == resIdx);

        // Reserve previous WAL segment.
        wal.reserve(new WALPointer(resIdx - 1, 0, 0));

        int resCnt = wal.reserved(new WALPointer(resIdx - 1, 0, 0), new WALPointer(resIdx, 0, 0));

        assertTrue("Expected resCnt is 2, real is " + resCnt, resCnt == 2);
    }

    /**
     * Tests that grid cache manager correctly truncates unused WAL segments;
     *
     * @throws Exception if failed.
     */
    @Test
    public void testWalDoesNotTruncatedWhenSegmentReserved() throws Exception {
        IgniteEx n = prepareGrid(2);

        IgniteWriteAheadLogManager wal = n.context().cache().context().wal();

        assertNotNull(wal);

        long resIdx = getReservedWalSegmentIndex(wal);

        assertTrue("Expected that at least resIdx greater than 0, real is " + resIdx, resIdx > 0);

        WALPointer lowPtr = lastCheckpointPointer(n);

        assertTrue("Expected that dbMbr returns valid resIdx", lowPtr.index() == resIdx);

        // Reserve previous WAL segment.
        wal.reserve(new WALPointer(resIdx - 1, 0, 0));

        int numDel = wal.truncate(lowPtr);

        int expNumDel = (int)resIdx - 1;

        assertTrue("Expected del segments is " + expNumDel + ", real is " + numDel, expNumDel == numDel);
    }

    /**
     * Checking that there will be no truncation of segments required for binary recovery.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNotTruncateSegmentsForBinaryRecovery() throws Exception {
        IgniteEx n = prepareGrid(1);

        IgniteWriteAheadLogManager wal = n.context().cache().context().wal();

        assertNotNull(wal);

        long resIdx = getReservedWalSegmentIndex(wal);
        assertTrue(resIdx > 3);

        WALPointer lastCheckpointPtr = lastCheckpointPointer(n);
        assertEquals(lastCheckpointPtr.index(), resIdx);

        wal.notchLastCheckpointPtr(new WALPointer(1, 0, 0));

        if (compactionEnabled(n))
            assertTrue(waitForCondition(() -> wal.lastCompactedSegment() >= 1, 10_000));

        int truncated = wal.truncate(lastCheckpointPtr);
        assertTrue("truncated: " + truncated, truncated >= 1);

        truncated = wal.truncate(lastCheckpointPtr);
        assertEquals(0, truncated);

        wal.notchLastCheckpointPtr(new WALPointer(2, 0, 0));

        if (compactionEnabled(n))
            assertTrue(waitForCondition(() -> wal.lastCompactedSegment() >= 2, 10_000));

        truncated = wal.truncate(lastCheckpointPtr);
        assertTrue("truncated: " + truncated, truncated >= 1);

        truncated = wal.truncate(lastCheckpointPtr);
        assertEquals(0, truncated);
    }

    /**
     * Starts grid and populates test data.
     *
     * @param cnt Grid count.
     * @return First started grid.
     * @throws Exception If failed.
     */
    private IgniteEx prepareGrid(int cnt) throws Exception {
        IgniteEx ig0 = startGrids(cnt);

        ig0.cluster().state(ClusterState.ACTIVE);
        awaitPartitionMapExchange();

        IgniteCache<Object, Object> cache = ig0.cache(DEFAULT_CACHE_NAME);

        for (int k = 0; k < 1_000; k++) {
            cache.put(k, new byte[1024]);

            if (k % 100 == 0)
                forceCheckpoint();
        }

        return ig0;
    }

    /**
     * Get index of reserved WAL segment by checkpoint.
     *
     * @param dbMgr Database shared manager.
     */
    private long getReservedWalSegmentIndex(IgniteWriteAheadLogManager dbMgr) {
        return ((WALPointer)GridTestUtils.getFieldValueHierarchy(dbMgr, "lastCheckpointPtr")).index();
    }

    /**
     * Getting WAL pointer last checkpoint.
     *
     * @param n Node.
     * @return WAL pointer last checkpoint.
     */
    private WALPointer lastCheckpointPointer(IgniteEx n) {
        return ((GridCacheDatabaseSharedManager)n.context().cache().context().database())
            .checkpointHistory().lastCheckpoint().checkpointMark();
    }

    /**
     * Checking that wal compaction enabled.
     *
     * @param n Node.
     * @return {@code True} if enabled.
     */
    private boolean compactionEnabled(IgniteEx n) {
        return n.configuration().getDataStorageConfiguration().isWalCompactionEnabled();
    }
}
