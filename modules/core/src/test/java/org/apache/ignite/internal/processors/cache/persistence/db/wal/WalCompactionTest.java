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
import java.util.Arrays;
import java.util.Comparator;
import org.apache.ignite.IgniteCache;
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
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_PAGE_SIZE;

/**
 *
 */
public class WalCompactionTest extends GridCommonAbstractTest {
    /** Ip finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

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

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setMaxSize(200L * 1024 * 1024))
            .setWalMode(walMode)
            .setWalSegmentSize(WAL_SEGMENT_SIZE)
            .setWalHistorySize(500)
            .setWalCompactionEnabled(compactionEnabled));

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName("cache");
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 16));
        ccfg.setBackups(0);

        cfg.setCacheConfiguration(ccfg);
        cfg.setConsistentId(name);

        return cfg;
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
    public void testApplyingUpdatesFromCompactedWal() throws Exception {
        testApplyingUpdatesFromCompactedWal(false);
    }

    /**
     * Tests applying updates from compacted WAL archive when compressor is disabled.
     *
     * @throws Exception If failed.
     */
    public void testApplyingUpdatesFromCompactedWalWhenCompressorDisabled() throws Exception {
        testApplyingUpdatesFromCompactedWal(true);
    }

    /**
     * @param switchOffCompressor Switch off compressor after restart.
     * @throws Exception If failed.
     */
    private void testApplyingUpdatesFromCompactedWal(boolean switchOffCompressor) throws Exception {
        IgniteEx ig = (IgniteEx)startGrids(3);
        ig.cluster().active(true);

        IgniteCache<Integer, byte[]> cache = ig.cache("cache");

        for (int i = 0; i < ENTRIES; i++) { // At least 20MB of raw data in total.
            final byte[] val = new byte[20000];

            val[i] = 1;

            cache.put(i, val);
        }

        // Spam WAL to move all data records to compressible WAL zone.
        for (int i = 0; i < WAL_SEGMENT_SIZE / DFLT_PAGE_SIZE * 2; i++)
            ig.context().cache().context().wal().log(new PageSnapshot(new FullPageId(-1, -1), new byte[DFLT_PAGE_SIZE]));

        // WAL archive segment is allowed to be compressed when it's at least one checkpoint away from current WAL head.
        ig.context().cache().context().database().wakeupForCheckpoint("Forced checkpoint").get();
        ig.context().cache().context().database().wakeupForCheckpoint("Forced checkpoint").get();

        Thread.sleep(15_000); // Allow compressor to compress WAL segments.

        String nodeFolderName = ig.context().pdsFolderResolver().resolveFolders().folderName();

        File dbDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false);
        File walDir = new File(dbDir, "wal");
        File archiveDir = new File(walDir, "archive");
        File nodeArchiveDir = new File(archiveDir, nodeFolderName);
        File walSegment = new File(nodeArchiveDir, FileDescriptor.fileName(0) + FilePageStoreManager.ZIP_SUFFIX);

        assertTrue(walSegment.exists());
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
    }

    /**
     *
     */
    public void testCompressorToleratesEmptyWalSegmentsFsync() throws Exception {
        testCompressorToleratesEmptyWalSegments(WALMode.FSYNC);
    }

    /**
     *
     */
    public void testCompressorToleratesEmptyWalSegmentsLogOnly() throws Exception {
        testCompressorToleratesEmptyWalSegments(WALMode.LOG_ONLY);
    }

    /**
     * Tests that WAL compaction won't be stopped by single broken WAL segment.
     */
    private void testCompressorToleratesEmptyWalSegments(WALMode walMode) throws Exception {
        this.walMode = walMode;
        compactionEnabled = false;

        IgniteEx ig = startGrid(0);
        ig.cluster().active(true);

        IgniteCache<Integer, byte[]> cache = ig.cache("cache");

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

        try (RandomAccessFile raf = new RandomAccessFile(walSegment, "rw")) {
            raf.setLength(0); // Clear wal segment, but don't delete.
        }

        compactionEnabled = true;

        ig = startGrid(0);
        ig.cluster().active(true);

        Thread.sleep(15_000); // Allow compressor to compress WAL segments.

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

        assertTrue(walSegment.exists()); // Failed to compress WAL segment shoudn't be deleted.
    }

    /**
     * @throws Exception If failed.
     */
    public void testSeekingStartInCompactedSegment() throws Exception {
        IgniteEx ig = (IgniteEx)startGrids(3);
        ig.cluster().active(true);

        IgniteCache<Integer, byte[]> cache = ig.cache("cache");

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

        final File[] cpMarkersToSave = cpMarkersDir.listFiles();

        assert cpMarkersToSave != null;
        assertTrue(cpMarkersToSave.length >= 2);

        Arrays.sort(cpMarkersToSave, new Comparator<File>() {
            @Override public int compare(File o1, File o2) {
                return o1.getName().compareTo(o2.getName());
            }
        });

        for (int i = 100; i < ENTRIES; i++) { // At least 20MB of raw data in total.
            final byte[] val = new byte[20000];

            val[i] = 1;

            cache.put(i, val);
        }

        // Spam WAL to move all data records to compressible WAL zone.
        for (int i = 0; i < WAL_SEGMENT_SIZE / DFLT_PAGE_SIZE * 2; i++)
            ig.context().cache().context().wal().log(new PageSnapshot(new FullPageId(-1, -1), new byte[DFLT_PAGE_SIZE]));

        // WAL archive segment is allowed to be compressed when it's at least one checkpoint away from current WAL head.
        ig.context().cache().context().database().wakeupForCheckpoint("Forced checkpoint").get();
        ig.context().cache().context().database().wakeupForCheckpoint("Forced checkpoint").get();

        Thread.sleep(15_000); // Allow compressor to compress WAL segments.

        File walDir = new File(dbDir, "wal");
        File archiveDir = new File(walDir, "archive");
        File nodeArchiveDir = new File(archiveDir, nodeFolderName);
        File walSegment = new File(nodeArchiveDir, FileDescriptor.fileName(0) + FilePageStoreManager.ZIP_SUFFIX);

        assertTrue(walSegment.exists());
        assertTrue(walSegment.length() < WAL_SEGMENT_SIZE / 2); // Should be compressed at least in half.

        stopAllGrids();

        File[] cpMarkers = cpMarkersDir.listFiles(new FilenameFilter() {
            @Override public boolean accept(File dir, String name) {
                return !(
                    name.equals(cpMarkersToSave[0].getName()) ||
                    name.equals(cpMarkersToSave[1].getName()) ||
                    name.equals(cpMarkersToSave[2].getName()) ||
                    name.equals(cpMarkersToSave[3].getName()) ||
                    name.equals(cpMarkersToSave[4].getName())
                );
            }
        });

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

        ig = (IgniteEx)startGrids(3);

        awaitPartitionMapExchange();

        cache = ig.cache(CACHE_NAME);

        int missing = 0;

        for (int i = 0; i < 100; i++) {
            if (!cache.containsKey(i))
                missing++;
        }

        System.out.println(">>> Missing " + missing + " entries logged before WAL iteration start");
        assertTrue(missing > 0);

        boolean fail = false;

        // Check that all data is recovered from compacted WAL.
        for (int i = 100; i < ENTRIES; i++) {
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
    }
}
