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
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.checkpoint.noop.NoopCheckpointSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_ARCHIVE_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_PATH;
import static org.apache.ignite.configuration.WALMode.FSYNC;
import static org.apache.ignite.configuration.WALMode.LOG_ONLY;
import static org.apache.ignite.internal.pagemem.wal.record.RolloverType.CURRENT_SEGMENT;
import static org.apache.ignite.internal.pagemem.wal.record.RolloverType.NEXT_SEGMENT;
import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer.HEADER_RECORD_SIZE;

/**
 *
 */
public class WalRolloverTypesTest extends GridCommonAbstractTest {
    /** */
    private WALMode walMode;

    /** */
    private boolean disableWALArchiving;

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

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setMaxSize(20 * 1024 * 1024))
            .setWalMode(walMode)
            .setWalArchivePath(disableWALArchiving ? DFLT_WAL_PATH : DFLT_WAL_ARCHIVE_PATH)
            .setWalSegmentSize(4 * 1024 * 1024))
            .setCheckpointSpi(new NoopCheckpointSpi());

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
    @Test
    public void testCurrentSegmentTypeLogOnlyModeArchiveOn() throws Exception {
        checkCurrentSegmentType(LOG_ONLY, false);
    }

    /** */
    @Test
    public void testCurrentSegmentTypeLogOnlyModeArchiveOff() throws Exception {
        checkCurrentSegmentType(LOG_ONLY, true);
    }

    /** */
    @Test
    public void testCurrentSegmentTypeLogFsyncModeArchiveOn() throws Exception {
        checkCurrentSegmentType(FSYNC, false);
    }

    /** */
    @Test
    public void testCurrentSegmentTypeLogFsyncModeArchiveOff() throws Exception {
        checkCurrentSegmentType(FSYNC, true);
    }

    /** */
    @Test
    public void testNextSegmentTypeLogOnlyModeArchiveOn() throws Exception {
        checkNextSegmentType(LOG_ONLY, false);
    }

    /** */
    @Test
    public void testNextSegmentTypeLogOnlyModeArchiveOff() throws Exception {
        checkNextSegmentType(LOG_ONLY, true);
    }

    /** */
    @Test
    public void testNextSegmentTypeFsyncModeArchiveOn() throws Exception {
        checkNextSegmentType(FSYNC, false);
    }

    /** */
    @Test
    public void testNextSegmentTypeFsyncModeArchiveOff() throws Exception {
        checkNextSegmentType(FSYNC, true);
    }

    /** */
    private void checkCurrentSegmentType(WALMode mode, boolean disableArch) throws Exception {
        walMode = mode;
        disableWALArchiving = disableArch;

        IgniteEx ig = startGrid(0);

        ig.cluster().active(true);

        IgniteWriteAheadLogManager walMgr = ig.context().cache().context().wal();

        ig.context().cache().context().database().checkpointReadLock();

        try {
            WALPointer ptr = walMgr.log(new AdHocWALRecord(), CURRENT_SEGMENT);

            assertEquals(0, ((FileWALPointer)ptr).index());
        }
        finally {
            ig.context().cache().context().database().checkpointReadUnlock();
        }
    }

    /** */
    private void checkNextSegmentType(WALMode mode, boolean disableArch) throws Exception {
        walMode = mode;
        disableWALArchiving = disableArch;

        IgniteEx ig = startGrid(0);

        ig.cluster().active(true);

        IgniteWriteAheadLogManager walMgr = ig.context().cache().context().wal();

        ig.context().cache().context().database().checkpointReadLock();

        try {
            WALPointer ptr = walMgr.log(new AdHocWALRecord(), NEXT_SEGMENT);

            assertEquals(1, ((FileWALPointer)ptr).index());
        }
        finally {
            ig.context().cache().context().database().checkpointReadUnlock();
        }
    }

    /** */
    @Test
    public void testNextSegmentTypeWithCacheActivityLogOnlyModeArchiveOn() throws Exception {
        checkNextSegmentTypeWithCacheActivity(LOG_ONLY, false);
    }

    /** */
    @Test
    public void testNextSegmentTypeWithCacheActivityLogOnlyModeArchiveOff() throws Exception {
        checkNextSegmentTypeWithCacheActivity(LOG_ONLY, true);
    }

    /** */
    @Test
    public void testNextSegmentTypeWithCacheActivityFsyncModeArchiveOn() throws Exception {
        checkNextSegmentTypeWithCacheActivity(FSYNC, false);
    }

    /** */
    @Test
    public void testNextSegmentTypeWithCacheActivityFsyncModeArchiveOff() throws Exception {
        checkNextSegmentTypeWithCacheActivity(FSYNC, true);
    }

    /**
     * Under load, ensures the record gets into very beginning of the segment in {@code NEXT_SEGMENT} log mode.
     */
    private void checkNextSegmentTypeWithCacheActivity(WALMode mode, boolean disableArch) throws Exception {
        walMode = mode;
        disableWALArchiving = disableArch;

        IgniteEx ig = startGrid(0);

        ig.cluster().active(true);

        IgniteCache<Integer, Integer> cache = ig.getOrCreateCache(DEFAULT_CACHE_NAME);

        final long testDuration = 30_000;

        long startTime = U.currentTimeMillis();

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(
            () -> {
                ThreadLocalRandom random = ThreadLocalRandom.current();

                while (U.currentTimeMillis() - startTime < testDuration)
                    cache.put(random.nextInt(100), random.nextInt(100_000));
            },
            8, "cache-put-thread");

        IgniteWriteAheadLogManager walMgr = ig.context().cache().context().wal();

        IgniteCacheDatabaseSharedManager dbMgr = ig.context().cache().context().database();

        AdHocWALRecord markerRecord = new AdHocWALRecord();

        WALPointer ptr0;
        WALPointer ptr1;

        do {
            try {
                U.sleep(1000);

                ptr0 = walMgr.log(markerRecord);

                dbMgr.checkpointReadLock();

                try {
                    ptr1 = walMgr.log(markerRecord, NEXT_SEGMENT);
                }
                finally {
                    dbMgr.checkpointReadUnlock();
                }

                assertTrue(ptr0 instanceof FileWALPointer);
                assertTrue(ptr1 instanceof FileWALPointer);

                assertTrue(((FileWALPointer)ptr0).index() < ((FileWALPointer)ptr1).index());

                assertEquals(HEADER_RECORD_SIZE, ((FileWALPointer)ptr1).fileOffset());
            }
            catch (IgniteCheckedException e) {
                log.error(e.getMessage(), e);
            }
        }
        while (U.currentTimeMillis() - startTime < testDuration);

        fut.get();
    }

    /** */
    @Test
    public void testCurrentSegmentTypeWithCacheActivityLogOnlyModeArchiveOn() throws Exception {
        checkCurrentSegmentTypeWithCacheActivity(LOG_ONLY, false);
    }

    /** */
    @Test
    public void testCurrentSegmentTypeWithCacheActivityLogOnlyModeArchiveOff() throws Exception {
        checkCurrentSegmentTypeWithCacheActivity(LOG_ONLY, true);
    }

    /** */
    @Test
    public void testCurrentSegmentTypeWithCacheActivityFsyncModeArchiveOn() throws Exception {
        checkCurrentSegmentTypeWithCacheActivity(FSYNC, false);
    }

    /** */
    @Test
    public void testCurrentSegmentTypeWithCacheActivityFsyncModeArchiveOff() throws Exception {
        checkCurrentSegmentTypeWithCacheActivity(FSYNC, true);
    }

    /**
     * Under load, ensures the record gets into very beginning of the segment in {@code NEXT_SEGMENT} log mode.
     */
    private void checkCurrentSegmentTypeWithCacheActivity(WALMode mode, boolean disableArch) throws Exception {
        walMode = mode;
        disableWALArchiving = disableArch;

        IgniteEx ig = startGrid(0);

        ig.cluster().active(true);

        IgniteCache<Integer, Integer> cache = ig.getOrCreateCache(DEFAULT_CACHE_NAME);

        final long testDuration = 30_000;

        long startTime = U.currentTimeMillis();

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(
            () -> {
                ThreadLocalRandom random = ThreadLocalRandom.current();

                while (U.currentTimeMillis() - startTime < testDuration)
                    cache.put(random.nextInt(100), random.nextInt(100_000));
            },
            8, "cache-put-thread");

        IgniteWriteAheadLogManager walMgr = ig.context().cache().context().wal();

        IgniteCacheDatabaseSharedManager dbMgr = ig.context().cache().context().database();

        AdHocWALRecord markerRecord = new AdHocWALRecord();

        WALPointer ptr0;
        WALPointer ptr1;

        do {
            try {
                U.sleep(1000);

                dbMgr.checkpointReadLock();

                try {
                    ptr0 = walMgr.log(markerRecord, CURRENT_SEGMENT);
                }
                finally {
                    dbMgr.checkpointReadUnlock();
                }

                ptr1 = walMgr.log(markerRecord);

                assertTrue(ptr0 instanceof FileWALPointer);
                assertTrue(ptr1 instanceof FileWALPointer);

                assertTrue(((FileWALPointer)ptr0).index() < ((FileWALPointer)ptr1).index());
            }
            catch (IgniteCheckedException e) {
                log.error(e.getMessage(), e);
            }
        }
        while (U.currentTimeMillis() - startTime < testDuration);

        fut.get();
    }
}
