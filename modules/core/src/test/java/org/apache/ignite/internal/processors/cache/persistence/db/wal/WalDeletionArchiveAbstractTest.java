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
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointHistory;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.Checkpointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_CHECKPOINT_TRIGGER_ARCHIVE_SIZE_PERCENTAGE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_MAX_CHECKPOINT_MEMORY_HISTORY_SIZE;
import static org.apache.ignite.internal.util.IgniteUtils.GB;
import static org.apache.ignite.internal.util.IgniteUtils.KB;
import static org.apache.ignite.internal.util.IgniteUtils.MB;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValueHierarchy;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

/**
 *
 */
public abstract class WalDeletionArchiveAbstractTest extends GridCommonAbstractTest {
    /**
     * Start grid with override default configuration via customConfigurator.
     */
    private Ignite startGrid(Consumer<DataStorageConfiguration> customConfigurator) throws Exception {
        IgniteConfiguration configuration = getConfiguration(getTestIgniteInstanceName());

        DataStorageConfiguration dbCfg = new DataStorageConfiguration();

        dbCfg.setWalMode(walMode());
        dbCfg.setWalSegmentSize(512 * 1024);
        dbCfg.setCheckpointFrequency(60 * 1000); //too high value for turn off frequency checkpoint.
        dbCfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration()
            .setMaxSize(100 * 1024 * 1024)
            .setPersistenceEnabled(true));

        customConfigurator.accept(dbCfg);

        configuration.setDataStorageConfiguration(dbCfg);

        Ignite ignite = startGrid(configuration);

        ignite.cluster().state(ClusterState.ACTIVE);

        return ignite;
    }

    /** */
    private CacheConfiguration<Integer, Object> cacheConfiguration() {
        CacheConfiguration<Integer, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        return ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
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
     * @return WAL mode used in test.
     */
    protected abstract WALMode walMode();

    /**
     * find first cause's message
     */
    private String findSourceMessage(Throwable ex) {
        return ex.getCause() == null ? ex.getMessage() : findSourceMessage(ex.getCause());
    }

    /**
     * Correct delete archived wal files.
     */
    @Test
    public void testCorrectDeletedArchivedWalFiles() throws Exception {
        //given: configured grid with setted max wal archive size
        long maxWalArchiveSize = 2 * 1024 * 1024;
        Ignite ignite = startGrid(dbCfg -> dbCfg.setMaxWalArchiveSize(maxWalArchiveSize));

        GridCacheDatabaseSharedManager dbMgr = gridDatabase(ignite);

        CheckpointHistory hist = dbMgr.checkpointHistory();
        assertNotNull(hist);

        IgniteCache<Integer, Object> cache = ignite.getOrCreateCache(cacheConfiguration());

        //when: put to cache more than 2 MB
        for (int i = 0; i < 500; i++) {
            if (i % 100 == 0)
                forceCheckpoint();

            cache.put(i, i);
        }

        //then: total archive size less than of maxWalArchiveSize(by current logic)
        FileWriteAheadLogManager wal = wal(ignite);

        assertTrue(waitForCondition(() -> wal.lastTruncatedSegment() >= 0, 10_000));

        FileDescriptor[] files = wal.walArchiveFiles();

        long totalSize = wal.totalSize(files);

        assertTrue(files.length >= 1);
        assertTrue(totalSize < maxWalArchiveSize);
        assertFalse(Stream.of(files).anyMatch(desc -> desc.file().getName().endsWith("00001.wal")));

        assertTrue(!hist.checkpoints().isEmpty());
    }

    /**
     * Checkpoint triggered depends on wal size.
     */
    @Test
    public void testCheckpointStarted_WhenWalHasTooBigSizeWithoutCheckpoint() throws Exception {
        //given: configured grid with max wal archive size = 1MB, wal segment size = 512KB
        Ignite ignite = startGrid(dbCfg -> dbCfg.setMaxWalArchiveSize(1024 * 1024));

        GridCacheDatabaseSharedManager dbMgr = gridDatabase(ignite);

        IgniteCache<Integer, Object> cache = ignite.getOrCreateCache(cacheConfiguration());

        for (int i = 0; i < 500; i++)
            cache.put(i, i);

        //then: checkpoint triggered by size limit of wall without checkpoint
        Checkpointer checkpointer = dbMgr.getCheckpointer();

        String checkpointReason = U.field((Object)U.field(checkpointer, "curCpProgress"), "reason");

        assertEquals("too big size of WAL without checkpoint", checkpointReason);
    }

    /**
     * Test for check deprecated removing checkpoint by deprecated walHistorySize parameter
     *
     * @deprecated Test old removing process depends on WalHistorySize.
     */
    @Test
    public void testCheckpointHistoryRemovingByTruncate() throws Exception {
        Ignite ignite = startGrid(dbCfg -> dbCfg.setMaxWalArchiveSize(2 * 1024 * 1024));

        GridCacheDatabaseSharedManager dbMgr = gridDatabase(ignite);

        IgniteCache<Integer, Object> cache = ignite.getOrCreateCache(cacheConfiguration());

        CheckpointHistory hist = dbMgr.checkpointHistory();
        assertNotNull(hist);

        int startHistSize = hist.checkpoints().size();

        int checkpointCnt = 10;

        for (int i = 0; i < checkpointCnt; i++) {
            cache.put(i, i);
            //and: wait for checkpoint finished
            forceCheckpoint();
            // Check that the history is growing.
            assertEquals(startHistSize + (i + 1), hist.checkpoints().size());
        }

        // Ensure rollover and wal archive cleaning.
        for (int i = 0; i < 6; i++)
            cache.put(i, new byte[ignite.configuration().getDataStorageConfiguration().getWalSegmentSize() / 2]);

        FileWriteAheadLogManager wal = wal(ignite);
        assertTrue(waitForCondition(() -> wal.lastTruncatedSegment() >= 0, 10_000));

        assertTrue(hist.checkpoints().size() < checkpointCnt + startHistSize);

        File[] cpFiles = dbMgr.checkpointDirectory().listFiles();

        assertTrue(cpFiles.length <= (checkpointCnt * 2 + 1)); // starts & ends + node_start
    }

    /**
     * Correct delete checkpoint history from memory depends on IGNITE_PDS_MAX_CHECKPOINT_MEMORY_HISTORY_SIZE.
     * WAL files doesn't delete because deleting was disabled.
     */
    @Test
    @WithSystemProperty(key = IGNITE_PDS_MAX_CHECKPOINT_MEMORY_HISTORY_SIZE, value = "2")
    public void testCorrectDeletedCheckpointHistoryButKeepWalFiles() throws Exception {
        //given: configured grid with disabled WAL removing.
        Ignite ignite = startGrid(dbCfg -> dbCfg.setMaxWalArchiveSize(DataStorageConfiguration.UNLIMITED_WAL_ARCHIVE));

        GridCacheDatabaseSharedManager dbMgr = gridDatabase(ignite);

        CheckpointHistory hist = dbMgr.checkpointHistory();
        assertNotNull(hist);

        IgniteCache<Integer, Object> cache = ignite.getOrCreateCache(cacheConfiguration());

        //when: put to cache
        for (int i = 0; i < 500; i++) {
            cache.put(i, i);

            if (i % 10 == 0)
                forceCheckpoint();
        }

        forceCheckpoint();

        //then: WAL files was not deleted but some of checkpoint history was deleted.
        FileWriteAheadLogManager wal = wal(ignite);
        assertNull(getFieldValueHierarchy(wal, "cleaner"));

        FileDescriptor[] files = wal.walArchiveFiles();

        assertTrue(Stream.of(files).anyMatch(desc -> desc.file().getName().endsWith("0001.wal")));

        assertTrue(hist.checkpoints().size() == 2);
    }

    /**
     * Checks that the deletion of WAL segments occurs with the maximum number of segments.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_CHECKPOINT_TRIGGER_ARCHIVE_SIZE_PERCENTAGE, value = "1000")
    public void testSingleCleanWalArchive() throws Exception {
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(0))
            .setCacheConfiguration(cacheConfiguration())
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setCheckpointFrequency(Long.MAX_VALUE)
                    .setMaxWalArchiveSize(5 * MB)
                    .setWalSegmentSize((int)MB)
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setPersistenceEnabled(true)
                            .setMaxSize(GB)
                            .setCheckpointPageBufferSize(GB)
                    )
            );

        ListeningTestLogger listeningLog = new ListeningTestLogger(cfg.getGridLogger());
        cfg.setGridLogger(listeningLog);

        IgniteEx n = startGrid(cfg);

        n.cluster().state(ClusterState.ACTIVE);
        awaitPartitionMapExchange();

        for (int i = 0; walArchiveSize(n) < 20L * cfg.getDataStorageConfiguration().getWalSegmentSize(); )
            n.cache(DEFAULT_CACHE_NAME).put(i++, new byte[(int)(512 * KB)]);

        assertEquals(-1, wal(n).lastTruncatedSegment());
        assertEquals(0, gridDatabase(n).lastCheckpointMarkWalPointer().index());

        Collection<String> logStrs = new ConcurrentLinkedQueue<>();
        listeningLog.registerListener(logStr -> {
            if (logStr.contains("Finish clean WAL archive"))
                logStrs.add(logStr);
        });

        forceCheckpoint();

        long maxWalArchiveSize = cfg.getDataStorageConfiguration().getMaxWalArchiveSize();
        assertTrue(waitForCondition(() -> walArchiveSize(n) < maxWalArchiveSize, getTestTimeout()));

        assertEquals(logStrs.toString(), 1, logStrs.size());
    }

    /**
     * Check that if the maximum archive size is equal to one WAL segment size, then when the archive is overflowing,
     * segments that are needed for binary recovery will not be deleted from the archive until the checkpoint occurs
     * and finishes.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMaxWalArchiveSizeEqualsOneWalSegmentSize() throws Exception {
        int walSegmentSize = (int)MB;

        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(0))
            .setCacheConfiguration(cacheConfiguration())
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setCheckpointFrequency(Long.MAX_VALUE)
                    .setMaxWalArchiveSize(walSegmentSize)
                    .setWalSegmentSize(walSegmentSize)
                    .setWalSegments(2)
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setPersistenceEnabled(true)
                            .setMaxSize(GB)
                            .setCheckpointPageBufferSize(GB)
                    )
            );

        IgniteEx n = startGrid(cfg);

        n.cluster().state(ClusterState.ACTIVE);

        // Let's not let a checkpoint happen.
        gridDatabase(n).checkpointReadLock();

        FileWriteAheadLogManager wal = wal(n);

        try {
            // Let's reserve the very first segment that will definitely be needed for the checkpoint.
            assertTrue(wal.reserve(new FileWALPointer(0, 0, 0)));

            for (int i = 0; wal.lastArchivedSegment() < 20L; i++)
                n.cache(DEFAULT_CACHE_NAME).put(i, new byte[(int)(512 * KB)]);

            // Make sure nothing has been deleted from the archive.
            assertThat(walArchiveSize(n), greaterThanOrEqualTo(20L * walSegmentSize));
            assertThat(wal.lastTruncatedSegment(), equalTo(-1L));

            // Let's try to reserve all the segments and then immediately release them.
            long lastWalSegmentIndex = ((FileWALPointer)wal.lastWritePointer()).index();

            for (int i = 0; i < lastWalSegmentIndex; i++) {
                FileWALPointer pointer = new FileWALPointer(i, 0, 0);

                // Unable to reserve because the archive is full.
                assertFalse(String.valueOf(i), wal.reserve(pointer));

                wal.release(pointer);
            }

            assertTrue(
                String.valueOf(lastWalSegmentIndex),
                wal.reserve(new FileWALPointer(lastWalSegmentIndex, 0, 0))
            );

            wal.release(new FileWALPointer(lastWalSegmentIndex, 0, 0));

            // Let's wait a bit, suddenly there will be a deletion from the archive?
            assertFalse(waitForCondition(() -> wal.lastTruncatedSegment() >= 0, 1_000, 100));

            // Make sure nothing has been deleted from the archive.
            assertThat(walArchiveSize(n), greaterThanOrEqualTo(20L * walSegmentSize));
            assertThat(wal.lastTruncatedSegment(), equalTo(-1L));
        }
        finally {
            gridDatabase(n).checkpointReadUnlock();
        }

        // Now let's run a checkpoint and make sure that only one segment remains in the archive.
        forceCheckpoint(n);

        assertTrue(
            IgniteUtils.humanReadableByteCount(walArchiveSize(n)),
            waitForCondition(() -> walArchiveSize(n) <= walSegmentSize, 1_000, 100)
        );

        assertThat(
            wal.lastTruncatedSegment(),
            lessThan(((FileWALPointer)getFieldValueHierarchy(wal, "lastCheckpointPtr")).index())
        );
    }

    /**
     * Extract GridCacheDatabaseSharedManager.
     */
    private GridCacheDatabaseSharedManager gridDatabase(Ignite ignite) {
        return (GridCacheDatabaseSharedManager)((IgniteEx)ignite).context().cache().context().database();
    }

    /**
     * Extract IgniteWriteAheadLogManager.
     */
    private FileWriteAheadLogManager wal(Ignite ignite) {
        return (FileWriteAheadLogManager)((IgniteEx)ignite).context().cache().context().wal();
    }

    /**
     * Calculate current WAL archive size.
     *
     * @param n Node.
     * @return Total WAL archive size.
     */
    private long walArchiveSize(Ignite n) {
        return Arrays.stream(wal(n).walArchiveFiles()).mapToLong(fd -> fd.file().length()).sum();
    }
}
