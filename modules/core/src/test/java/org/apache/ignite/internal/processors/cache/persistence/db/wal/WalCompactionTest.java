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

import java.io.File;
import java.io.FilenameFilter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.RolloverType;
import org.apache.ignite.internal.processors.cache.persistence.DummyPageIO;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.ZIP_SUFFIX;

/**
 *
 */
public class WalCompactionTest extends GridCommonAbstractTest {
    /** Wal segment size. */
    private static final int WAL_SEGMENT_SIZE = 4 * 1024 * 1024;

    /** Cache name. */
    public static final String CACHE_NAME = "cache";

    /** Entries count. */
    public static final int ENTRIES = 1000;

    /** Compaction enabled flag. */
    private boolean compactionEnabled;

    /** Wal mode. */
    private WALMode walMode;

    /** */
    private static class RolloverRecord extends CheckpointRecord {
        /** */
        private RolloverRecord() {
            super(null);
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setMaxSize(200L * 1024 * 1024))
            .setWalMode(walMode)
            .setWalSegmentSize(WAL_SEGMENT_SIZE)
            .setWalHistorySize(500)
            .setWalCompactionEnabled(compactionEnabled));

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(CACHE_NAME);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 16));
        ccfg.setBackups(0);

        cfg.setCacheConfiguration(ccfg);
        cfg.setConsistentId(name);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        compactionEnabled = true;

        walMode = WALMode.LOG_ONLY;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Tests applying updates from compacted WAL archive.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testApplyingUpdatesFromCompactedWal() throws Exception {
        testApplyingUpdatesFromCompactedWal(false);
    }

    /**
     * Tests applying updates from compacted WAL archive when compressor is disabled.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testApplyingUpdatesFromCompactedWalWhenCompressorDisabled() throws Exception {
        testApplyingUpdatesFromCompactedWal(true);
    }

    /**
     * @param switchOffCompressor Switch off compressor after restart.
     * @throws Exception If failed.
     */
    private void testApplyingUpdatesFromCompactedWal(boolean switchOffCompressor) throws Exception {
        IgniteEx ig = (IgniteEx)startGrids(3);

        ig.cluster().baselineAutoAdjustEnabled(false);
        ig.cluster().active(true);

        IgniteCache<Integer, byte[]> cache = ig.cache(CACHE_NAME);

        final int pageSize = ig.cachex(CACHE_NAME).context().dataRegion().pageMemory().pageSize();

        for (int i = 0; i < ENTRIES; i++) { // At least 20MB of raw data in total.
            final byte[] val = new byte[20000];

            val[i] = 1;

            cache.put(i, val);
        }

        byte[] dummyPage = dummyPage(pageSize);

        // Spam WAL to move all data records to compressible WAL zone.
        for (int i = 0; i < WAL_SEGMENT_SIZE / pageSize * 2; i++) {
            ig.context().cache().context().wal().log(new PageSnapshot(new FullPageId(-1, -1), dummyPage,
                pageSize));
        }

        // WAL archive segment is allowed to be compressed when it's at least one checkpoint away from current WAL head.
        ig.context().cache().context().database().wakeupForCheckpoint("Forced checkpoint").get();
        ig.context().cache().context().database().wakeupForCheckpoint("Forced checkpoint").get();

        String nodeFolderName = ig.context().pdsFolderResolver().resolveFolders().folderName();

        File dbDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false);
        File walDir = new File(dbDir, "wal");
        File archiveDir = new File(walDir, "archive");
        File nodeArchiveDir = new File(archiveDir, nodeFolderName);
        File walSegment = new File(nodeArchiveDir, FileDescriptor.fileName(0) + ZIP_SUFFIX);

        // Allow compressor to compress WAL segments.
        assertTrue(GridTestUtils.waitForCondition(walSegment::exists, 15_000));

        assertTrue(walSegment.length() < WAL_SEGMENT_SIZE / 2); // Should be compressed at least in half.

        stopAllGrids();

        File nodeLfsDir = new File(dbDir, nodeFolderName);
        File cpMarkersDir = new File(nodeLfsDir, "cp");

        File[] cpMarkers = cpMarkersDir.listFiles();

        assertNotNull(cpMarkers);
        assertTrue(cpMarkers.length > 0);

        File cacheDir = new File(nodeLfsDir, "cache-" + CACHE_NAME);
        File[] lfsFiles = cacheDir.listFiles();

        assertNotNull(lfsFiles);
        assertTrue(lfsFiles.length > 0);

        // Enforce reading WAL from the very beginning at the next start.
        for (File f : cpMarkers)
            f.delete();

        for (File f : lfsFiles)
            f.delete();

        compactionEnabled = !switchOffCompressor;

        ig = (IgniteEx)startGrids(3);

        awaitPartitionMapExchange();

        cache = ig.cache(CACHE_NAME);

        boolean fail = false;

        // Check that all data is recovered from compacted WAL.
        for (int i = 0; i < ENTRIES; i++) {
            byte[] arr = cache.get(i);

            if (arr == null) {
                System.out.println(">>> Missing: " + i);

                fail = true;
            }
            else if (arr[i] != 1) {
                System.out.println(">>> Corrupted: " + i);

                fail = true;
            }
        }

        assertFalse(fail);

        // Check compation successfully reset on blt changed.
        stopAllGrids();

        Ignite ignite = startGrids(2);

        ignite.cluster().active(true);

        resetBaselineTopology();

        ignite.resetLostPartitions(Collections.singleton(CACHE_NAME));

        // This node will join to different blt.
        startGrid(2);

        awaitPartitionMapExchange();
    }

    /**
     *
     */
    @Test
    public void testCompressorToleratesEmptyWalSegmentsFsync() throws Exception {
        testCompressorToleratesEmptyWalSegments(WALMode.FSYNC);
    }

    /**
     *
     */
    @Test
    public void testCompressorToleratesEmptyWalSegmentsLogOnly() throws Exception {
        testCompressorToleratesEmptyWalSegments(WALMode.LOG_ONLY);
    }

    /**
     *
     */
    @Test
    public void testOptimizedWalSegments() throws Exception {
        IgniteConfiguration icfg = getConfiguration(getTestIgniteInstanceName(0));

        icfg.getDataStorageConfiguration().setWalSegmentSize(300_000_000);
        icfg.getDataStorageConfiguration().setWalSegments(2);

        IgniteEx ig = (IgniteEx)startGrid(getTestIgniteInstanceName(0), optimize(icfg), null);

        ig.cluster().active(true);

        IgniteCache<Integer, byte[]> cache = ig.cache(CACHE_NAME);

        for (int i = 0; i < 2500; i++) { // At least 50MB of raw data in total.
            final byte[] val = new byte[20000];

            val[i] = 1;

            cache.put(i, val);
        }

        IgniteWriteAheadLogManager walMgr = ig.context().cache().context().wal();

        IgniteCacheDatabaseSharedManager dbMgr = ig.context().cache().context().database();

        RolloverRecord rec = new RolloverRecord();

        try {
            dbMgr.checkpointReadLock();

            try {
                walMgr.log(rec, RolloverType.NEXT_SEGMENT);
            }
            finally {
                dbMgr.checkpointReadUnlock();
            }
        }
        catch (IgniteCheckedException e) {
            log.error(e.getMessage(), e);
        }

        long start = System.currentTimeMillis();

        do {
            Thread.yield();
        }
        while (walMgr.lastArchivedSegment() < 0 && (System.currentTimeMillis() - start < 15_000));

        assertTrue(System.currentTimeMillis() - start < 15_000);

        String nodeFolderName = ig.context().pdsFolderResolver().resolveFolders().folderName();

        stopAllGrids();

        File dbDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false);
        File walDir = new File(dbDir, "wal");
        File archiveDir = new File(walDir, "archive");
        File nodeArchiveDir = new File(archiveDir, nodeFolderName);
        File walSegment = new File(nodeArchiveDir, FileDescriptor.fileName(0));

        assertTrue("" + walSegment.length(), walSegment.length() < 200_000_000);
    }

    /**
     * Tests that WAL compaction won't be stopped by single broken WAL segment.
     */
    private void testCompressorToleratesEmptyWalSegments(WALMode walMode) throws Exception {
        this.walMode = walMode;
        compactionEnabled = false;

        IgniteEx ig = startGrid(0);
        ig.cluster().active(true);

        IgniteCache<Integer, byte[]> cache = ig.cache(CACHE_NAME);

        for (int i = 0; i < 2500; i++) { // At least 50MB of raw data in total.
            final byte[] val = new byte[20000];

            val[i] = 1;

            cache.put(i, val);
        }

        // WAL archive segment is allowed to be compressed when it's at least one checkpoint away from current WAL head.
        ig.context().cache().context().database().wakeupForCheckpoint("Forced checkpoint").get();
        ig.context().cache().context().database().wakeupForCheckpoint("Forced checkpoint").get();

        String nodeFolderName = ig.context().pdsFolderResolver().resolveFolders().folderName();

        stopAllGrids();

        int emptyIdx = 5;

        File dbDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false);
        File walDir = new File(dbDir, "wal");
        File archiveDir = new File(walDir, "archive");
        File nodeArchiveDir = new File(archiveDir, nodeFolderName);
        File walSegment = new File(nodeArchiveDir, FileDescriptor.fileName(emptyIdx));
        File zippedWalSegment = new File(nodeArchiveDir, FileDescriptor.fileName(emptyIdx + 1) + ZIP_SUFFIX);

        long start = U.currentTimeMillis();
        do {
            try (RandomAccessFile raf = new RandomAccessFile(walSegment, "rw")) {
                raf.setLength(0); // Clear wal segment, but don't delete.
            }

            if (walSegment.length() == 0)
                break;

            if (U.currentTimeMillis() - start >= 10000)
                throw new IgniteCheckedException("Can't trucate: " + walSegment.getAbsolutePath());

            Thread.yield();
        }
        while (true);

        compactionEnabled = true;

        ig = startGrid(0);
        ig.cluster().active(true);

        // Allow compressor to compress WAL segments.
        assertTrue(GridTestUtils.waitForCondition(zippedWalSegment::exists, 15_000));

        File[] compressedSegments = nodeArchiveDir.listFiles(new FilenameFilter() {
            @Override public boolean accept(File dir, String name) {
                return name.endsWith(".wal.zip");
            }
        });

        long maxIdx = -1;
        for (File f : compressedSegments) {
            String idxPart = f.getName().substring(0, f.getName().length() - ".wal.zip".length());

            maxIdx = Math.max(maxIdx, Long.parseLong(idxPart));
        }

        System.out.println("Max compressed index: " + maxIdx);
        assertTrue(maxIdx > emptyIdx);

        if (!walSegment.exists()) {
            File[] list = nodeArchiveDir.listFiles();

            Arrays.sort(list);

            log.info("Files in archive:" + list.length);

            for (File f : list)
                log.info(f.getAbsolutePath());

            // Failed to compress WAL segment shoudn't be deleted.
            fail("File " + walSegment.getAbsolutePath() + " does not exist.");
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSeekingStartInCompactedSegment() throws Exception {
        IgniteEx ig = startGrids(3);
        ig.cluster().active(true);

        IgniteCache<Integer, byte[]> cache = ig.cache(CACHE_NAME);

        for (int i = 0; i < 100; i++) {
            final byte[] val = new byte[20000];

            val[i] = 1;

            cache.put(i, val);
        }

        ig.context().cache().context().database().wakeupForCheckpoint("Forced checkpoint").get();
        ig.context().cache().context().database().wakeupForCheckpoint("Forced checkpoint").get();

        String nodeFolderName = ig.context().pdsFolderResolver().resolveFolders().folderName();

        File dbDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false);
        File nodeLfsDir = new File(dbDir, nodeFolderName);
        File cpMarkersDir = new File(nodeLfsDir, "cp");

        Set<String> cpMarkersToSave = Arrays.stream(cpMarkersDir.listFiles()).map(File::getName).collect(toSet());

        assertTrue(cpMarkersToSave.size() >= 2);

        for (int i = 100; i < ENTRIES; i++) { // At least 20MB of raw data in total.
            final byte[] val = new byte[20000];

            val[i] = 1;

            cache.put(i, val);

            //It trigger checkout in the middle of put that it shifts 'keepUncompressedIdx'
            // to allow the compressor to delete unzipped segments.
            if (i % 100 == 0)
                ig.context().cache().context().database().wakeupForCheckpoint("Forced checkpoint").get();
        }

        final int pageSize = ig.cachex(CACHE_NAME).context().dataRegion().pageMemory().pageSize();

        byte[] dummyPage = dummyPage(pageSize);

        // Spam WAL to move all data records to compressible WAL zone.
        for (int i = 0; i < WAL_SEGMENT_SIZE / pageSize * 2; i++) {
            ig.context().cache().context().wal().log(new PageSnapshot(new FullPageId(-1, -1), dummyPage,
                pageSize));
        }

        // WAL archive segment is allowed to be compressed when it's at least one checkpoint away from current WAL head.
        ig.context().cache().context().database().wakeupForCheckpoint("Forced checkpoint").get();
        ig.context().cache().context().database().wakeupForCheckpoint("Forced checkpoint").get();

        File nodeArchiveDir = dbDir.toPath().resolve(Paths.get("wal", "archive", nodeFolderName)).toFile();
        File unzippedWalSegment = new File(nodeArchiveDir, FileDescriptor.fileName(0));
        File walSegment = new File(nodeArchiveDir, FileDescriptor.fileName(0) + ZIP_SUFFIX);

        // Allow compressor to compress WAL segments.
        assertTrue(GridTestUtils.waitForCondition(() -> !unzippedWalSegment.exists(), 15_000));

        assertTrue(walSegment.exists());
        assertTrue(walSegment.length() < WAL_SEGMENT_SIZE / 2); // Should be compressed at least in half.

        stopAllGrids();

        File[] cpMarkers = cpMarkersDir.listFiles((dir, name) -> !cpMarkersToSave.contains(name));

        assertNotNull(cpMarkers);
        assertTrue(cpMarkers.length > 0);

        File cacheDir = new File(nodeLfsDir, "cache-" + CACHE_NAME);
        File[] lfsFiles = cacheDir.listFiles();

        assertNotNull(lfsFiles);
        assertTrue(lfsFiles.length > 0);

        // Enforce reading WAL from the very beginning at the next start.
        for (File f : cpMarkers)
            f.delete();

        for (File f : lfsFiles)
            f.delete();

        ig = startGrids(3);

        awaitPartitionMapExchange();

        cache = ig.cache(CACHE_NAME);

        int missing = 0;

        for (int i = 0; i < 100; i++) {
            if (!cache.containsKey(i))
                missing++;
        }

        log.info(">>> Missing " + missing + " entries logged before WAL iteration start");
        assertTrue(missing > 0);

        boolean fail = false;

        // Check that all data is recovered from compacted WAL.
        for (int i = 100; i < ENTRIES; i++) {
            byte[] arr = cache.get(i);

            if (arr == null) {
                log.info(">>> Missing: " + i);

                fail = true;
            }
            else if (arr[i] != 1) {
                log.info(">>> Corrupted: " + i);

                fail = true;
            }
        }

        assertFalse(fail);
    }

    /**
     * @param pageSize Page size.
     */
    private static byte[] dummyPage(int pageSize) {
        ByteBuffer pageBuf = ByteBuffer.allocateDirect(pageSize);

        DummyPageIO.VERSIONS.latest().initNewPage(GridUnsafe.bufferAddress(pageBuf), -1, pageSize);

        byte[] pageData = new byte[pageSize];

        pageBuf.get(pageData);

        return pageData;
    }
}
