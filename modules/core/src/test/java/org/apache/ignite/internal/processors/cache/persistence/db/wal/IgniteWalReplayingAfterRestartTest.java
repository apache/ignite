/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageUpdatePartitionDataRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory.IteratorParametersBuilder;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_PATH;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordPurpose.PHYSICAL;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.CHECKPOINT_RECORD;

/**
 * Tests WAL replying after node restart when wal archiver is disabled.
 */
public class IgniteWalReplayingAfterRestartTest extends GridCommonAbstractTest {
    /** */
    public static final String CACHE_NAME = "test-cache";

    /** */
    public static final String CONSISTENT_ID = "wal-replying-after-restart-test";

    /** */
    public static final int SEGMENTS_CNT = 3;

    /** */
    public static final int PART_NUM = 32;

    /** */
    private WALMode logMode = WALMode.LOG_ONLY;

    /** */
    @Before
    public void beforeIgniteWalReplayingAfterRestartTest() throws Exception {
        U.delete(Paths.get(U.defaultWorkDirectory()));
    }

    /** */
    @After
    public void afterIgniteWalReplayingAfterRestartTest() throws Exception {
        stopAllGrids();

        U.delete(Paths.get(U.defaultWorkDirectory()));
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration<Integer, byte[]> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, PART_NUM));

        cfg.setCacheConfiguration(ccfg);

        DataStorageConfiguration dbCfg = new DataStorageConfiguration()
            .setWalMode(logMode)
            .setWalSegments(SEGMENTS_CNT)
            .setWalSegmentSize(512 * 1024)
            .setWalHistorySize(100)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setCheckpointPageBufferSize(4L * 1024 * 1024 * 1024))
            // disable WAL archiver
            .setWalArchivePath(DFLT_WAL_PATH)
            .setWalPath(DFLT_WAL_PATH)
            .setCheckpointFrequency(1_000_000);

        cfg.setDataStorageConfiguration(dbCfg);

        cfg.setConsistentId(CONSISTENT_ID);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testWalRecordsAfterRestart() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        IgniteCache<Integer, byte[]> cache = ignite.getOrCreateCache(CACHE_NAME);

        int key = 0;

        while (ignite.context().cache().context().wal().lastArchivedSegment() < SEGMENTS_CNT)
            cache.put(key++ % PART_NUM, new byte[1024]);

        ignite.context().cache().context().database().waitForCheckpoint("test-checkpoint");

        long lastArchived = ignite.context().cache().context().wal().lastArchivedSegment();

        while (ignite.context().cache().context().wal().lastArchivedSegment() < lastArchived + 1)
            cache.put(key++ % PART_NUM, new byte[1024]);

        stopGrid(0);

        // There are no exceptions should be thrown here.
        ignite = startGrid(0);

        ignite.cluster().active();

        // delta records should always follow PageSnapshot records.
        String workDir = U.defaultWorkDirectory();

        IteratorParametersBuilder builder = new IteratorParametersBuilder()
            .filesOrDirs(workDir)
            .filter((rec, ptr) -> rec.purpose() == PHYSICAL);

        Map<FullPageId, PageSnapshot> snapshots = new HashMap<>();

        try (WALIterator it = new IgniteWalIteratorFactory().iterator(builder)) {
            while (it.hasNext()) {
                IgniteBiTuple<WALPointer, WALRecord> tup = it.next();

                WALRecord rec = tup.get2();

                if (rec.type() == CHECKPOINT_RECORD)
                    snapshots.clear();

                // let's check partition meta pages.
                if (rec instanceof PageSnapshot) {
                    PageSnapshot snpRec = (PageSnapshot)rec;

                    assertFalse(snapshots.containsKey(snpRec.fullPageId()));

                    snapshots.put(snpRec.fullPageId(), snpRec);
                }
                else if (rec instanceof MetaPageUpdatePartitionDataRecord) {
                    MetaPageUpdatePartitionDataRecord metaRec = (MetaPageUpdatePartitionDataRecord)rec;

                    assertTrue(snapshots.containsKey(metaRec.fullPageId()));
                }
            }
        }
    }

    /**
     * Verifies that validation of WAL segment sizes isn't triggered when WAL archive is disabled.
     */
    @Test
    public void testFsyncWalValidationAfterRestart() throws Exception {
        logMode = WALMode.FSYNC;

        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        IgniteCache<Object, Object> cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 128; i++)
            cache.put("key" + i, new byte[1024]);

        stopGrid(0);

        startGrid(0);
    }
}
