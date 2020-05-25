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

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.crc.WalTestUtils;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.FilteredWalIterator;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.WalFilters;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DISABLE_GRP_STATE_LAZY_STORE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;

/**
 * Tests if reservation of corrupted checkpoint works correctly, also checks correct behaviour for corrupted zip wal file
 * during PME.
 */
public class CorruptedCheckpointReservationTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** Partitions count. */
    private static final int PARTS_CNT = 16;

    /** Wal compaction enabled. */
    private boolean walCompactionEnabled;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        CacheConfiguration<Object, Object> ccfg1 = new CacheConfiguration<>(CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(1)
            .setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT));

        cfg.setCacheConfiguration(ccfg1);

        DataStorageConfiguration dbCfg = new DataStorageConfiguration()
            .setWalMode(WALMode.LOG_ONLY)
            .setWalSegmentSize(1024 * 1024)
            .setCheckpointFrequency(Integer.MAX_VALUE)
            .setWalCompactionEnabled(walCompactionEnabled)
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setMaxSize(100 * 1024 * 1024)
            );

        cfg.setDataStorageConfiguration(dbCfg);

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

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        withSystemPropertyByClass(IGNITE_PDS_WAL_REBALANCE_THRESHOLD, "0");

        withSystemPropertyByClass(IGNITE_DISABLE_GRP_STATE_LAZY_STORE, "true");
    }

    /**
     * @throws Exception if failed.
     */
    public void testCorruptedCheckpointReservation() throws Exception {
        fail("https://ggsystems.atlassian.net/browse/GG-26123");

        walCompactionEnabled = false;

        startGrids(2);

        IgniteEx ig0 = grid(0);

        ig0.cluster().active(true);

        generateCps(ig0);

        corruptWalRecord(ig0, 3, false);

        startGrid(1);

        awaitPartitionMapExchange();
    }

    /**
     * @throws Exception if failed.
     */
    public void testCorruptedCheckpointInCompressedWalReservation() throws Exception {
        fail("https://ggsystems.atlassian.net/browse/GG-26123");

        walCompactionEnabled = true;

        startGrids(2);

        IgniteEx ig0 = grid(0);

        ig0.cluster().active(true);

        generateCps(ig0);

        corruptWalRecord(ig0, 3, true);

        startGrid(1);

        awaitPartitionMapExchange();
    }

    /**
     * @throws Exception if failed.
     */
    public void testCorruptedCompressedWalSegment() throws Exception {
        fail("https://ggsystems.atlassian.net/browse/GG-26123");

        walCompactionEnabled = true;

        startGrids(2);

        IgniteEx ig0 = grid(0);

        ig0.cluster().active(true);

        generateCps(ig0);

        corruptCompressedWalSegment(ig0, 3);

        startGrid(1);

        awaitPartitionMapExchange();
    }

    /**
     * @param ig Ignite.
     */
    private void generateCps(IgniteEx ig) throws IgniteCheckedException {
        IgniteCache<Object, Object> cache = ig.cache(CACHE_NAME);

        final int entryCnt = PARTS_CNT * 1000;

        for (int i = 0; i < entryCnt; i++)
            cache.put(i, i);

        forceCheckpoint();

        for (int i = 0; i < entryCnt; i++)
            cache.put(i, i);

        forceCheckpoint();

        stopGrid(1);

        for (int i = 0; i < entryCnt; i++)
            cache.put(i, i + 100);

        forceCheckpoint();

        for (int i = 0; i < entryCnt; i++)
            cache.put(i, i + 1000);

        forceCheckpoint();

        for (int i = 0; i < entryCnt; i++)
            cache.put(i, i + 10000);

        forceCheckpoint();
    }

    /**
     * @param ig Ignite.
     * @param cpIdx Checkpoint index.
     */
    private void corruptWalRecord(IgniteEx ig, int cpIdx, boolean segmentCompressed) throws IgniteCheckedException, IOException {
        IgniteWriteAheadLogManager walMgr = ig.context().cache().context().wal();

        FileWALPointer corruptedCp = getCp(ig, cpIdx);

        Optional<FileDescriptor> cpSegment = getFileDescriptor(segmentCompressed, walMgr, corruptedCp);

        if (segmentCompressed) {
            assertTrue("Cannot find " + FilePageStoreManager.ZIP_SUFFIX + " segment for checkpoint.", cpSegment.isPresent());

            WalTestUtils.corruptWalRecordInCompressedSegment(cpSegment.get(), corruptedCp);
        }
        else {
            assertTrue("Cannot find " + FileDescriptor.WAL_SEGMENT_FILE_EXT + " segment for checkpoint.", cpSegment.isPresent());

            WalTestUtils.corruptWalRecord(cpSegment.get(), corruptedCp);
        }
    }

    /**
     * @param segmentCompressed Segment compressed.
     * @param walMgr Wal manager.
     * @param corruptedCp Corrupted checkpoint.
     */
    @NotNull private Optional<FileDescriptor> getFileDescriptor(boolean segmentCompressed,
        IgniteWriteAheadLogManager walMgr, FileWALPointer corruptedCp) {

        IgniteWalIteratorFactory iterFactory = new IgniteWalIteratorFactory();

        File walArchiveDir = U.field(walMgr, "walArchiveDir");

        List<FileDescriptor> walFiles = getWalFiles(walArchiveDir, iterFactory);

        String suffix = segmentCompressed ? FilePageStoreManager.ZIP_SUFFIX : FileDescriptor.WAL_SEGMENT_FILE_EXT;

        return walFiles.stream().filter(
            w -> w.idx() == corruptedCp.index() && w.file().getName().endsWith(suffix)
        ).findFirst();
    }

    /**
     * @param ig Ignite.
     * @param cpIdx Checkpoint index.
     */
    private void corruptCompressedWalSegment(IgniteEx ig, int cpIdx) throws IgniteCheckedException, IOException {
        IgniteWriteAheadLogManager walMgr = ig.context().cache().context().wal();

        FileWALPointer corruptedCp = getCp(ig, cpIdx);

        Optional<FileDescriptor> cpSegment = getFileDescriptor(true, walMgr, corruptedCp);

        assertTrue("Cannot find " + FilePageStoreManager.ZIP_SUFFIX + " segment for checkpoint.", cpSegment.isPresent());

        WalTestUtils.corruptCompressedFile(cpSegment.get());
    }

    /**
     * @param ig Ignite.
     * @param cpIdx Checkpoint index.
     */
    private FileWALPointer getCp(IgniteEx ig, int cpIdx) throws IgniteCheckedException {
        IgniteWriteAheadLogManager walMgr = ig.context().cache().context().wal();

        List<IgniteBiTuple<WALPointer, WALRecord>> checkpoints;

        try (FilteredWalIterator iter = new FilteredWalIterator(walMgr.replay(null), WalFilters.checkpoint())) {
            checkpoints = Lists.newArrayList((Iterable<? extends IgniteBiTuple<WALPointer, WALRecord>>)iter);
        }

        return (FileWALPointer) checkpoints.get(cpIdx).get2().position();
    }

    /**
     * @param walDir Wal directory.
     * @param iterFactory Iterator factory.
     */
    private List<FileDescriptor> getWalFiles(File walDir, IgniteWalIteratorFactory iterFactory) {
        return iterFactory.resolveWalFiles(
            new IgniteWalIteratorFactory.IteratorParametersBuilder()
                .filesOrDirs(walDir)
        );
    }
}
