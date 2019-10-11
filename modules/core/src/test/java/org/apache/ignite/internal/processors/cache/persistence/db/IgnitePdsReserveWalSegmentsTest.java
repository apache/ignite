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
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointHistory;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_MAX_CHECKPOINT_MEMORY_HISTORY_SIZE;

/**
 * Test correctness of truncating unused WAL segments.
 */
@WithSystemProperty(key = IGNITE_PDS_MAX_CHECKPOINT_MEMORY_HISTORY_SIZE, value = "2")
public class IgnitePdsReserveWalSegmentsTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        CacheConfiguration<Integer, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));

        cfg.setCacheConfiguration(ccfg);

        DataStorageConfiguration dbCfg = new DataStorageConfiguration();

        cfg.setDataStorageConfiguration(dbCfg);

        dbCfg.setWalSegmentSize(1024 * 1024)
            .setMaxWalArchiveSize(Long.MAX_VALUE)
            .setWalSegments(10)
            .setWalMode(WALMode.LOG_ONLY)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(100 * 1024 * 1024)
                .setPersistenceEnabled(true));

        return cfg;
    }

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

    /**
     * Tests that range reserved method return correct number of reserved WAL segments.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testWalManagerRangeReservation() throws Exception {
        IgniteEx ig0 = prepareGrid(4);

        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)ig0.context().cache().context()
            .database();

        IgniteWriteAheadLogManager wal = ig0.context().cache().context().wal();

        long resIdx = getReservedWalSegmentIndex(dbMgr);

        assertTrue("Expected that at least resIdx greater than 0, real is " + resIdx, resIdx > 0);

            FileWALPointer lowPtr = (FileWALPointer)dbMgr.checkpointHistory().firstCheckpointPointer();

        assertTrue("Expected that dbMbr returns valid resIdx", lowPtr.index() == resIdx);

        // Reserve previous WAL segment.
        wal.reserve(new FileWALPointer(resIdx - 1, 0, 0));

        int resCnt = wal.reserved(new FileWALPointer(resIdx - 1, 0, 0), new FileWALPointer(resIdx, 0, 0));

        assertTrue("Expected resCnt is 2, real is " + resCnt, resCnt == 2);
    }

    /**
     * Tests that grid cache manager correctly truncates unused WAL segments;
     *
     * @throws Exception if failed.
     */
    @Test
    public void testWalDoesNotTruncatedWhenSegmentReserved() throws Exception {
        IgniteEx ig0 = prepareGrid(4);

        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)ig0.context().cache().context()
            .database();

        IgniteWriteAheadLogManager wal = ig0.context().cache().context().wal();

        long resIdx = getReservedWalSegmentIndex(dbMgr);

        assertTrue("Expected that at least resIdx greater than 0, real is " + resIdx, resIdx > 0);

            FileWALPointer lowPtr = (FileWALPointer) dbMgr.checkpointHistory().firstCheckpointPointer();

        assertTrue("Expected that dbMbr returns valid resIdx", lowPtr.index() == resIdx);

        // Reserve previous WAL segment.
        wal.reserve(new FileWALPointer(resIdx - 1, 0, 0));

        int numDel = wal.truncate(null, lowPtr);

        int expNumDel = (int)resIdx - 1;

        assertTrue("Expected del segments is " + expNumDel + ", real is " + numDel, expNumDel == numDel);
    }

    /**
     * Starts grid and populates test data.
     *
     * @param cnt Grid count.
     * @return First started grid.
     * @throws Exception If failed.
     */
    private IgniteEx prepareGrid(int cnt) throws Exception {
        IgniteEx ig0 = (IgniteEx)startGrids(cnt);

        ig0.cluster().active(true);

        IgniteCache<Object, Object> cache = ig0.cache(DEFAULT_CACHE_NAME);

        for (int k = 0; k < 1_000; k++) {
            cache.put(k, new byte[1024]);

            if (k % 100 == 0)
                forceCheckpoint();
        }

        return ig0;
    }

    /**
     * Get index of reserved WAL segment by checkpointer.
     *
     * @param dbMgr Database shared manager.
     */
    private long getReservedWalSegmentIndex(GridCacheDatabaseSharedManager dbMgr) {
        CheckpointHistory cpHist = dbMgr.checkpointHistory();

        return ((FileWALPointer) cpHist.firstCheckpointPointer()).index();
    }
}
