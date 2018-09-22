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
package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.configuration.WALMode.FSYNC;
import static org.apache.ignite.configuration.WALMode.LOG_ONLY;
import static org.apache.ignite.internal.pagemem.wal.record.RolloverType.CURRENT_SEGMENT;
import static org.apache.ignite.internal.pagemem.wal.record.RolloverType.NEXT_SEGMENT;

/**
 *
 */
public class WalRolloverTypesTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private WALMode walMode;

    /** */
    private static class AdHocWALRecord extends CheckpointRecord {
        /** */
        private AdHocWALRecord() {
            super(null);
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setMaxSize(20 * 1024 * 1024))
            .setWalMode(walMode)
            .setWalSegmentSize(4 * 1024 * 1024));

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

    /** */
    public void testCurrentSegmentTypeLogOnlyMode() throws Exception {
        walMode = LOG_ONLY;

        checkCurrentSegmentType();
    }

    /** */
    public void testCurrentSegmentTypeLogFsyncMode() throws Exception {
        walMode = FSYNC;

        checkCurrentSegmentType();
    }

    /** */
    public void testNextSegmentTypeLogOnlyMode() throws Exception {
        walMode = LOG_ONLY;

        checkNextSegmentType();
    }

    /** */
    public void testNextSegmentTypeFsyncMode() throws Exception {
        walMode = FSYNC;

        checkNextSegmentType();
    }

    /** */
    private void checkCurrentSegmentType() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().active(true);

        IgniteWriteAheadLogManager walMgr = ig.context().cache().context().wal();

        // This will ensure initial checkpoint (and rollover) completion.
        assertTrue(GridTestUtils.waitForCondition(() -> walMgr.lastArchivedSegment() == 0, 10_000));

        // Now current segment is 1.

        ig.context().cache().context().database().checkpointReadLock();

        try {
            WALPointer ptr = walMgr.log(new AdHocWALRecord(), CURRENT_SEGMENT);

            assertEquals(1, ((FileWALPointer)ptr).index());
        }
        finally {
            ig.context().cache().context().database().checkpointReadUnlock();
        }
    }

    /** */
    private void checkNextSegmentType() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().active(true);

        IgniteWriteAheadLogManager walMgr = ig.context().cache().context().wal();

        // This will ensure initial checkpoint (and rollover) completion.
        assertTrue(GridTestUtils.waitForCondition(() -> walMgr.lastArchivedSegment() == 0, 10_000));

        // Now current segment is 1.

        ig.context().cache().context().database().checkpointReadLock();

        try {
            WALPointer ptr = walMgr.log(new AdHocWALRecord(), NEXT_SEGMENT);

            assertEquals(2, ((FileWALPointer)ptr).index());
        }
        finally {
            ig.context().cache().context().database().checkpointReadUnlock();
        }
    }

    /** */
    public void testNextSegmentTypeWithCacheActivityLogOnlyMode() throws Exception {
        walMode = LOG_ONLY;

        checkNextSegmentTypeWithCacheActivity();
    }

    /** */
    public void testNextSegmentTypeWithCacheActivityFsyncMode() throws Exception {
        walMode = FSYNC;

        checkNextSegmentTypeWithCacheActivity();
    }

    /**
     * Under load, ensures the record gets into very beginning of the segment in {@code NEXT_SEGMENT} log mode.
     */
    private void checkNextSegmentTypeWithCacheActivity() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().active(true);

        IgniteCache<Integer, Integer> cache = ig.getOrCreateCache(DEFAULT_CACHE_NAME);

        long startTime = U.currentTimeMillis();

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(
            () -> {
                ThreadLocalRandom random = ThreadLocalRandom.current();

                while (U.currentTimeMillis() - startTime < 30_000)
                    cache.put(random.nextInt(100_000), random.nextInt(100_000));
            },
            8, "cache-put-thread");

        List<Long> forcedRolloverSegs = new ArrayList<>();

        IgniteWriteAheadLogManager walMgr = ig.context().cache().context().wal();

        IgniteCacheDatabaseSharedManager dbMgr = ig.context().cache().context().database();

        AdHocWALRecord markerRecord = new AdHocWALRecord();

        do {
            try {
                U.sleep(1000);

                dbMgr.checkpointReadLock();

                try {
                    WALPointer ptr = walMgr.log(markerRecord, NEXT_SEGMENT);

                    assert ptr instanceof FileWALPointer;

                    forcedRolloverSegs.add(((FileWALPointer)ptr).index());
                }
                finally {
                    dbMgr.checkpointReadUnlock();
                }
            }
            catch (IgniteCheckedException e) {
                log.error(e.getMessage(), e);
            }
        } while (U.currentTimeMillis() - startTime < 30_000);

        fut.get();

        dbMgr.checkpointReadLock();

        try {
            walMgr.log(new CheckpointRecord(null), NEXT_SEGMENT);
        }
        finally {
            dbMgr.checkpointReadUnlock();
        }

        assertTrue(GridTestUtils.waitForCondition(
            () -> walMgr.lastArchivedSegment() >= forcedRolloverSegs.get(forcedRolloverSegs.size() - 1), 10_000));

        WALIterator walIter = walMgr.replay(null);

        Iterator<Long> segIter = forcedRolloverSegs.iterator();

        while (walIter.hasNext() && segIter.hasNext()) {
            long idx = segIter.next();

            IgniteBiTuple<WALPointer, WALRecord> walEntry;

            do {
                assertTrue(walIter.hasNext());

                walEntry = walIter.next();

                assertTrue(walEntry.getKey() instanceof FileWALPointer);
            } while (((FileWALPointer)walEntry.getKey()).index() < idx);

            WALRecord rec = walEntry.getValue();

            assertTrue(rec instanceof CheckpointRecord);

            assertEquals(markerRecord.checkpointId(), ((CheckpointRecord)rec).checkpointId());
        }
   }
}
