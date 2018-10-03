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
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

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
            .setWalArchivePath(DFLT_WAL_PATH)
            .setWalSegmentSize(4 * 1024 * 1024))
            .setCheckpointSpi(new NoopCheckpointSpi())
        ;

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
        fail("https://issues.apache.org/jira/browse/IGNITE-9776");

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
        fail("https://issues.apache.org/jira/browse/IGNITE-9776");

        walMode = FSYNC;

        checkNextSegmentType();
    }

    /** */
    private void checkCurrentSegmentType() throws Exception {
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
    private void checkNextSegmentType() throws Exception {
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
    public void testNextSegmentTypeWithCacheActivityLogOnlyMode() throws Exception {
        walMode = LOG_ONLY;

        checkNextSegmentTypeWithCacheActivity();
    }

    /** */
    public void testNextSegmentTypeWithCacheActivityFsyncMode() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-9776");

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

        WALPointer ptr;

        do {
            try {
                U.sleep(1000);

                dbMgr.checkpointReadLock();

                try {
                    ptr = walMgr.log(markerRecord, NEXT_SEGMENT);
                }
                finally {
                    dbMgr.checkpointReadUnlock();
                }

                assertTrue(ptr instanceof FileWALPointer);
                assertEquals(HEADER_RECORD_SIZE, ((FileWALPointer)ptr).fileOffset());
            }
            catch (IgniteCheckedException e) {
                log.error(e.getMessage(), e);
            }
        }
        while (U.currentTimeMillis() - startTime < testDuration);

        fut.get();
    }
}
