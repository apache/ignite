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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.io.File;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.startsWith;

/**
 *
 */
public class WalDeletionArchiveTest extends GridCommonAbstractTest {

    /**
     * history size parameters consistency checked. Should be set just one of wal history size or max wal archive size
     */
    public void testGridDoesNotStart_BecauseBothWalHistorySizeAndMaxWalArchiveSizeUsed() throws Exception {
        //given: wal history size and max wal archive size are both set
        IgniteConfiguration configuration = getConfiguration(getTestIgniteInstanceName());

        DataStorageConfiguration dbCfg = new DataStorageConfiguration();
        dbCfg.setWalHistorySize(12);
        dbCfg.setMaxWalArchiveSize(9);
        configuration.setDataStorageConfiguration(dbCfg);

        try {
            //when: start grid
            startGrid(getTestIgniteInstanceName(), configuration);
            fail("Should be fail because both wal history size and max wal archive size was used");
        }
        catch (IgniteException e) {
            //then: exception is occurrence because should be set just one parameters
            assertThat(findSourceMessage(e), is(startsWith("Should be used only one of wal history size or max wal archive size")));
        }
    }

    /** find first cause's message */
    private String findSourceMessage(Throwable ex) {
        return ex.getCause() == null ? ex.getMessage() : findSourceMessage(ex.getCause());
    }

    /** correct delete archived wal files. */
    public void testCorrectDeletedArchivedWalFiles() throws Exception {
        try {
            //given: configured grid with max wal archive size = 1MB, wal segment size = 128KB
            long maxWalArchiveSize = 1 * 1024 * 1024;
            Ignite ignite = startGrid(dbCfg -> {
                dbCfg.setMaxWalArchiveSize(maxWalArchiveSize);// 1 Mbytes
                dbCfg.setWalSegmentSize(128 * 1024);
                dbCfg.setCheckpointFrequency(60 * 1000);
            });

            GridCacheDatabaseSharedManager dbMgr = gridDatabase(ignite);

            long allowedThresholdWalArchiveSize = maxWalArchiveSize / 2;

            IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache("SomeCache");

            //when: put to cache more than 1 MB
            for (int i = 0; i < 250; i++)
                cache.put(i, i);

            //and: wait for checkpoint finished
            dbMgr.waitForCheckpoint("test");

            //then: total archive size less than half of maxWalArchiveSize(by current logic)
            FileDescriptor[] files = wal(ignite).walArchiveFiles();

            Long totalSize = Stream.of(files)
                .map(desc -> desc.file().length())
                .reduce(0L, Long::sum);

            assertThat(files.length, greaterThanOrEqualTo(1));
            assertThat(totalSize, lessThanOrEqualTo(allowedThresholdWalArchiveSize));

            GridCacheDatabaseSharedManager.CheckpointHistory hist = dbMgr.checkpointHistory();
            assertThat(hist.checkpoints(), hasSize(greaterThan(0)));
        }
        finally {
            stopAllGrids();
        }
    }

    /** checkpoint triggered depends on wal size */
    public void testCheckpointStarted_WhenWalHasTooBigSizeWithoutCheckpoint() throws Exception {
        try {
            //given: configured grid with max wal archive size = 1MB, wal segment size = 128KB
            Ignite ignite = startGrid(dbCfg -> {
                dbCfg.setMaxWalArchiveSize(1 * 1024 * 1024);// 1 Mbytes
                dbCfg.setWalSegmentSize(128 * 1024);
                dbCfg.setCheckpointFrequency(60 * 1000);
            });

            GridCacheDatabaseSharedManager dbMgr = gridDatabase(ignite);

            IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache("SomeCache");

            //when: put to cache more than 1 MB
            for (int i = 0; i < 250; i++)
                cache.put(i, i);

            //then: checkpoint triggered by size limit of wall without checkpoint
            String reason = U.field((Object)U.field(dbMgr.getCheckpointer(), "scheduledCp"), "reason");

            assertThat(reason, is("too big size of WAL without checkpoint"));
        }
        finally {
            stopAllGrids();
        }
    }

    /** start grid with override default configuration via customConfigurator */
    private Ignite startGrid(Consumer<DataStorageConfiguration> customConfigurator) throws Exception {
        IgniteConfiguration configuration = getConfiguration(getTestIgniteInstanceName());

        DataStorageConfiguration dbCfg = new DataStorageConfiguration();

        dbCfg.setPageSize(4 * 1024);
        dbCfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration()
            .setMaxSize(100 * 1024 * 1024)
            .setPersistenceEnabled(true));

        customConfigurator.accept(dbCfg);

        configuration.setDataStorageConfiguration(dbCfg);

        Ignite ignite = startGrid(getTestIgniteInstanceName(), configuration);

        ignite.active(true);

        return ignite;
    }

    /**
     * Test for check deprecated removing checkpoint by deprecated walHistorySize parameter
     */
    public void testCheckpointHistoryRemovingByWalHistorySize() throws Exception {
        try {
            //given: configured grid with wal history size = 10
            int walHistorySize = 10;

            Ignite ignite = startGrid(dbCfg -> {
                dbCfg.setWalHistorySize(walHistorySize);
                dbCfg.setWalSegmentSize(128 * 1024);
                dbCfg.setCheckpointFrequency(60 * 1000);
            });

            GridCacheDatabaseSharedManager dbMgr = gridDatabase(ignite);

            IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache("SomeCache");

            //when: put to cache and do checkpoint
            int testNumberOfCheckpoint = walHistorySize * 2;

            for (int i = 0; i < testNumberOfCheckpoint; i++) {
                cache.put(i, i);
                //and: wait for checkpoint finished
                dbMgr.waitForCheckpoint("test");
            }


            //then: number of checkpoints less or equal than walHistorySize
            GridCacheDatabaseSharedManager.CheckpointHistory hist = dbMgr.checkpointHistory();
            assertTrue(hist.checkpoints().size() <= walHistorySize);

            File[] cpFiles = dbMgr.checkpointDirectory().listFiles();

            assertThat(cpFiles.length , lessThanOrEqualTo(walHistorySize * 2 + 1));// starts & ends + node_start
        }
        finally {
            stopAllGrids();
        }
    }

    /** extract GridCacheDatabaseSharedManager */
    private GridCacheDatabaseSharedManager gridDatabase(Ignite ignite) {
        return (GridCacheDatabaseSharedManager)((IgniteEx)ignite).context().cache().context().database();
    }

    /** extract IgniteWriteAheadLogManager */
    private IgniteWriteAheadLogManager wal(Ignite ignite) {
        return ((IgniteEx)ignite).context().cache().context().wal();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cleanPersistenceDir();
    }
}
