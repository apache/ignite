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

package org.apache.ignite.cdc;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory.IteratorParametersBuilder;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cdc.CdcSelfTest.WAL_ARCHIVE_TIMEOUT;
import static org.apache.ignite.configuration.DataStorageConfiguration.UNLIMITED_WAL_ARCHIVE;
import static org.apache.ignite.internal.util.IgniteUtils.KB;
import static org.apache.ignite.internal.util.IgniteUtils.MB;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** Check only {@link DataRecord} written to the WAL for in-memory cache. */
@RunWith(Parameterized.class)
public class WalForCdcTest extends GridCommonAbstractTest {
    /** */
    private static final int RECORD_COUNT = 10;

    /** */
    @Parameterized.Parameter
    public CacheMode mode;

    /** */
    @Parameterized.Parameter(1)
    public CacheAtomicityMode atomicityMode;

    /** */
    private boolean persistenceEnabled;

    /** */
    private boolean cdcEnabled;

    /** */
    private long archiveSz = UNLIMITED_WAL_ARCHIVE;

    /** */
    @Parameterized.Parameters(name = "mode={0}, atomicityMode={1}")
    public static Collection<?> parameters() {
        List<Object[]> params = new ArrayList<>();

        for (CacheMode mode : Arrays.asList(REPLICATED, PARTITIONED))
            for (CacheAtomicityMode atomicityMode : Arrays.asList(ATOMIC, TRANSACTIONAL))
                params.add(new Object[] {mode, atomicityMode});

        return params;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setWalForceArchiveTimeout(WAL_ARCHIVE_TIMEOUT)
            .setWalSegmentSize((int)(2 * MB))
            .setMaxWalArchiveSize(archiveSz)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(persistenceEnabled)
                .setCdcEnabled(cdcEnabled)));

        cfg.setConsistentId(igniteInstanceName);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        cdcEnabled = true;
        persistenceEnabled = false;
    }

    /** */
    @Test
    public void testOnlyDataRecordWritten() throws Exception {
        IgniteEx ignite1 = startGrid(0);

        ignite1.cluster().state(ClusterState.ACTIVE);

        AtomicInteger cntr = new AtomicInteger();

        // Check only `DataRecords` written in WAL for in-memory cache with CDC enabled.
        doTestWal(ignite1, cache -> {
            for (int i = 0; i < RECORD_COUNT; i++)
                cache.put(keyForNode(ignite1.affinity(DEFAULT_CACHE_NAME), cntr, ignite1.localNode()), i);
        }, RECORD_COUNT);

        // Check no WAL written during rebalance.
        IgniteEx ignite2 = startGrid(1);

        awaitPartitionMapExchange(false, true, null);

        // Can't use `waitForCondition` because if test passed
        // then no `DataRecords` loged therefore no segment archivation.
        Thread.sleep(3 * WAL_ARCHIVE_TIMEOUT);

        int walRecCnt = checkDataRecords(ignite2);

        assertEquals(0, walRecCnt);

        // Check `DataRecords` written on second node after rebalance.
        doTestWal(ignite1, cache -> {
            for (int i = 0; i < RECORD_COUNT; i++)
                cache.put(keyForNode(ignite1.affinity(DEFAULT_CACHE_NAME), cntr, ignite1.localNode()), i);
        }, RECORD_COUNT * 2);

        doTestWal(ignite2, cache -> {
            for (int i = 0; i < RECORD_COUNT; i++)
                cache.put(keyForNode(ignite2.affinity(DEFAULT_CACHE_NAME), cntr, ignite2.localNode()), i);
        }, RECORD_COUNT * (mode == REPLICATED ? 2 : 1));
    }

    /** */
    @Test
    public void testWalDisable() throws Exception {
        persistenceEnabled = true;

        IgniteEx ignite = startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);

        doTestWal(ignite, cache -> {
            for (int i = 0; i < RECORD_COUNT / 2; i++)
                cache.put(i, i);

            ignite.cluster().disableWal(DEFAULT_CACHE_NAME);

            for (int i = 0; i < RECORD_COUNT; i++)
                cache.put(i, i);

            ignite.cluster().enableWal(DEFAULT_CACHE_NAME);

            for (int i = RECORD_COUNT / 2; i < RECORD_COUNT; i++)
                cache.put(i, i);
        }, RECORD_COUNT);
    }

    /** */
    @Test
    public void testWalDisabledIfPersistenceAndCdcDisabled() throws Exception {
        persistenceEnabled = false;
        cdcEnabled = false;

        IgniteEx ignite = startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);

        ignite.getOrCreateCache(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
                .setCacheMode(mode)
                .setAtomicityMode(atomicityMode));

        assertNull(ignite.context().cache().context().wal());
        assertNull(getFieldValue(ignite.context().cache().context(), "cdcWalMgr"));
    }

    /** */
    @Test
    public void testArchiveCleared() throws Exception {
        persistenceEnabled = false;
        cdcEnabled = true;
        archiveSz = 10 * MB;

        IgniteEx ignite = startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, byte[]> cache = ignite.getOrCreateCache(
            new CacheConfiguration<Integer, byte[]>(DEFAULT_CACHE_NAME)
                .setCacheMode(mode)
                .setAtomicityMode(atomicityMode));

        IntConsumer createData = (entryCnt) -> {
            for (int i = 0; i < entryCnt; i++) {
                byte[] payload = new byte[(int)KB];

                ThreadLocalRandom.current().nextBytes(payload);

                cache.put(i, payload);
            }
        };

        IgniteWriteAheadLogManager wal = ignite.context().cache().context().wal(true);

        long startSgmnt = wal.currentSegment();

        createData.accept((int)(archiveSz / (2 * KB)));

        long finishSgmnt = wal.currentSegment();

        String archive = archive(ignite);

        assertTrue(finishSgmnt > startSgmnt);
        assertTrue(
            "Wait for start segment archivation",
            waitForCondition(() -> startSgmnt <= wal.lastArchivedSegment(), getTestTimeout())
        );

        File startSgmntArchived = new File(archive, FileDescriptor.fileName(startSgmnt));

        assertTrue("Check archived segment file exists", startSgmntArchived.exists());

        createData.accept((int)(archiveSz / KB));

        assertTrue(
            "Wait for archived segment cleaned",
            waitForCondition(() -> !startSgmntArchived.exists(), getTestTimeout())
        );
    }

    /** */
    private void doTestWal(
        IgniteEx ignite,
        Consumer<IgniteCache<Integer, Integer>> putData,
        int expWalRecCnt
    ) throws Exception {
        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(
            new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
                .setCacheMode(mode)
                .setAtomicityMode(atomicityMode));

        long archiveIdx = ignite.context().cache().context().wal(true).lastArchivedSegment();

        putData.accept(cache);

        assertTrue(waitForCondition(
            () -> archiveIdx < ignite.context().cache().context().wal(true).lastArchivedSegment(),
            getTestTimeout()
        ));

        int walRecCnt = checkDataRecords(ignite);

        assertEquals(expWalRecCnt, walRecCnt);
    }

    /** */
    private int checkDataRecords(IgniteEx ignite) throws IgniteCheckedException {
        WALIterator iter = new IgniteWalIteratorFactory(log).iterator(new IteratorParametersBuilder()
            .ioFactory(new RandomAccessFileIOFactory())
            .filesOrDirs(archive(ignite)));

        int walRecCnt = 0;

        while (iter.hasNext()) {
            IgniteBiTuple<WALPointer, WALRecord> rec = iter.next();

            if (persistenceEnabled && (!(rec.get2() instanceof DataRecord)))
                continue;

            assertTrue(rec.get2() instanceof DataRecord);

            DataRecord dataRec = (DataRecord)rec.get2();

            for (int i = 0; i < dataRec.entryCount(); i++)
                assertEquals(CU.cacheId(DEFAULT_CACHE_NAME), dataRec.get(i).cacheId());

            walRecCnt++;
        }

        return walRecCnt;
    }

    /**
     * @param ignite Ignite.
     * @return WAL archive patch
     * @throws IgniteCheckedException If failed
     */
    private static String archive(IgniteEx ignite) throws IgniteCheckedException {
        return U.resolveWorkDirectory(
            U.defaultWorkDirectory(),
            ignite.configuration().getDataStorageConfiguration().getWalArchivePath() + "/" +
                U.maskForFileName(ignite.configuration().getIgniteInstanceName()),
            false
        ).getAbsolutePath();
    }
}
