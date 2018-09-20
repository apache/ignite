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
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointHistory;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_MAX_CHECKPOINT_MEMORY_HISTORY_SIZE;

/**
 *
 */
public abstract class WalDeletionArchiveAbstractTest extends GridCommonAbstractTest {
    /** */
    public static final String CACHE_NAME = "SomeCache";

    /**
     * Start grid with override default configuration via customConfigurator.
     */
    private Ignite startGrid(Consumer<DataStorageConfiguration> customConfigurator) throws Exception {
        IgniteConfiguration configuration = getConfiguration(getTestIgniteInstanceName());

        DataStorageConfiguration dbCfg = new DataStorageConfiguration();

        dbCfg.setWalMode(walMode());
        dbCfg.setWalSegmentSize(512 * 1024);
        dbCfg.setCheckpointFrequency(60 * 1000);//too high value for turn off frequency checkpoint.
        dbCfg.setPageSize(4 * 1024);
        dbCfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration()
            .setMaxSize(100 * 1024 * 1024)
            .setPersistenceEnabled(true));

        customConfigurator.accept(dbCfg);

        configuration.setDataStorageConfiguration(dbCfg);

        Ignite ignite = startGrid(configuration);

        ignite.active(true);

        return ignite;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        System.clearProperty(IGNITE_PDS_MAX_CHECKPOINT_MEMORY_HISTORY_SIZE);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @return WAL mode used in test.
     */
    abstract protected WALMode walMode();

    /**
     * History size parameters consistency check. Should be set just one of wal history size or max wal archive size.
     */
    public void testGridDoesNotStart_BecauseBothWalHistorySizeAndMaxWalArchiveSizeUsed() throws Exception {
        //given: wal history size and max wal archive size are both set.
        IgniteConfiguration configuration = getConfiguration(getTestIgniteInstanceName());

        DataStorageConfiguration dbCfg = new DataStorageConfiguration();
        dbCfg.setWalHistorySize(12);
        dbCfg.setMaxWalArchiveSize(9);
        configuration.setDataStorageConfiguration(dbCfg);

        try {
            //when: start grid.
            startGrid(getTestIgniteInstanceName(), configuration);
            fail("Should be fail because both wal history size and max wal archive size was used");
        }
        catch (IgniteException e) {
            //then: exception is occurrence because should be set just one parameters.
            assertTrue(findSourceMessage(e).startsWith("Should be used only one of wal history size or max wal archive size"));
        }
    }

    /**
     * find first cause's message
     */
    private String findSourceMessage(Throwable ex) {
        return ex.getCause() == null ? ex.getMessage() : findSourceMessage(ex.getCause());
    }

    /**
     * Correct delete archived wal files.
     */
    public void testCorrectDeletedArchivedWalFiles() throws Exception {
        //given: configured grid with setted max wal archive size
        long maxWalArchiveSize = 2 * 1024 * 1024;
        Ignite ignite = startGrid(dbCfg -> {
            dbCfg.setMaxWalArchiveSize(maxWalArchiveSize);
        });

        GridCacheDatabaseSharedManager dbMgr = gridDatabase(ignite);

        long allowedThresholdWalArchiveSize = maxWalArchiveSize / 2;

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(CACHE_NAME);

        //when: put to cache more than 2 MB
        for (int i = 0; i < 500; i++)
            cache.put(i, i);

        forceCheckpoint();

        //then: total archive size less than half of maxWalArchiveSize(by current logic)
        IgniteWriteAheadLogManager wal = wal(ignite);

        FileDescriptor[] files = (FileDescriptor[])U.findNonPublicMethod(wal.getClass(), "walArchiveFiles").invoke(wal);

        Long totalSize = Stream.of(files)
            .map(desc -> desc.file().length())
            .reduce(0L, Long::sum);

        assertTrue(files.length >= 1);
        assertTrue(totalSize <= allowedThresholdWalArchiveSize);
        assertFalse(Stream.of(files).anyMatch(desc -> desc.file().getName().endsWith("00001.wal")));

        CheckpointHistory hist = dbMgr.checkpointHistory();

        assertTrue(!hist.checkpoints().isEmpty());
    }

    /**
     * Checkpoint triggered depends on wal size.
     */
    public void testCheckpointStarted_WhenWalHasTooBigSizeWithoutCheckpoint() throws Exception {
        //given: configured grid with max wal archive size = 1MB, wal segment size = 512KB
        Ignite ignite = startGrid(dbCfg -> {
            dbCfg.setMaxWalArchiveSize(1 * 1024 * 1024);// 1 Mbytes
        });

        GridCacheDatabaseSharedManager dbMgr = gridDatabase(ignite);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(CACHE_NAME);

        for (int i = 0; i < 500; i++)
            cache.put(i, i);

        //then: checkpoint triggered by size limit of wall without checkpoint
        GridCacheDatabaseSharedManager.Checkpointer checkpointer = dbMgr.getCheckpointer();

        String checkpointReason = U.field((Object)U.field(checkpointer, "curCpProgress"), "reason");

        assertEquals("too big size of WAL without checkpoint", checkpointReason);
    }

    /**
     * Test for check deprecated removing checkpoint by deprecated walHistorySize parameter
     *
     * @deprecated Test old removing process depends on WalHistorySize.
     */
    public void testCheckpointHistoryRemovingByWalHistorySize() throws Exception {
        //given: configured grid with wal history size = 10
        int walHistorySize = 10;

        Ignite ignite = startGrid(dbCfg -> {
            dbCfg.setWalHistorySize(walHistorySize);
        });

        GridCacheDatabaseSharedManager dbMgr = gridDatabase(ignite);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(CACHE_NAME);

        //when: put to cache and do checkpoint
        int testNumberOfCheckpoint = walHistorySize * 2;

        for (int i = 0; i < testNumberOfCheckpoint; i++) {
            cache.put(i, i);
            //and: wait for checkpoint finished
            forceCheckpoint();
        }

        //then: number of checkpoints less or equal than walHistorySize
        CheckpointHistory hist = dbMgr.checkpointHistory();
        assertTrue(hist.checkpoints().size() == walHistorySize);

        File[] cpFiles = dbMgr.checkpointDirectory().listFiles();

        assertTrue(cpFiles.length <= (walHistorySize * 2 + 1));// starts & ends + node_start
    }

    /**
     * Correct delete checkpoint history from memory depends on IGNITE_PDS_MAX_CHECKPOINT_MEMORY_HISTORY_SIZE. WAL files
     * doesn't delete because deleting was disabled.
     */
    public void testCorrectDeletedCheckpointHistoryButKeepWalFiles() throws Exception {
        System.setProperty(IGNITE_PDS_MAX_CHECKPOINT_MEMORY_HISTORY_SIZE, "2");
        //given: configured grid with disabled WAL removing.
        Ignite ignite = startGrid(dbCfg -> {
            dbCfg.setMaxWalArchiveSize(Long.MAX_VALUE);
        });

        GridCacheDatabaseSharedManager dbMgr = gridDatabase(ignite);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(CACHE_NAME);

        //when: put to cache
        for (int i = 0; i < 500; i++) {
            cache.put(i, i);

            if (i % 10 == 0)
                forceCheckpoint();
        }

        forceCheckpoint();

        //then: WAL files was not deleted but some of checkpoint history was deleted.
        IgniteWriteAheadLogManager wal = wal(ignite);

        FileDescriptor[] files = (FileDescriptor[])U.findNonPublicMethod(wal.getClass(), "walArchiveFiles").invoke(wal);

        boolean hasFirstSegment = Stream.of(files)
            .anyMatch(desc -> desc.file().getName().endsWith("0001.wal"));

        assertTrue(hasFirstSegment);

        CheckpointHistory hist = dbMgr.checkpointHistory();

        assertTrue(hist.checkpoints().size() == 2);
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
    private IgniteWriteAheadLogManager wal(Ignite ignite) {
        return ((IgniteEx)ignite).context().cache().context().wal();
    }
}
