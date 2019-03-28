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

package org.apache.ignite.internal.processors.cache.persistence;

import java.io.File;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory.IteratorParametersBuilder;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 *
 */
public class IgnitePdsSporadicDataRecordsOnBackupTest extends GridCommonAbstractTest {
    /** */
    private static final int GRID_CNT = 2;

    /** Transactional cache name. */
    protected static final String TX_CACHE_NAME = "txCache";

    /** Keys count. */
    protected static final int KEYS_CNT = 100;

    /** Stop tx load flag. */
    protected static final AtomicBoolean txStop = new AtomicBoolean();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setMaxSize(10L * 1024 * 1024).setPersistenceEnabled(true))
            .setWalMode(WALMode.LOG_ONLY);

        cfg.setDataStorageConfiguration(memCfg);

        CacheConfiguration<Integer, Long> txCacheCfg = new CacheConfiguration<Integer, Long>(TX_CACHE_NAME)
            .setBackups(1)
            .setAtomicityMode(TRANSACTIONAL)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setWriteSynchronizationMode(FULL_SYNC)
            .setQueryEntities(Collections.singletonList(new QueryEntity(Integer.class, Long.class)));

        cfg.setCacheConfiguration(txCacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSporadicDataRecordsOnBackup() throws Exception {
        IgniteEx ig0 = startGrid(0);
        IgniteEx ig1 = startGrid(1);

        grid(0).cluster().active(true);

        String nodeFolderName0 = ig0.context().pdsFolderResolver().resolveFolders().folderName();
        String nodeFolderName1 = ig1.context().pdsFolderResolver().resolveFolders().folderName();

        IgniteCache<Integer, Long> cache = grid(0).cache(TX_CACHE_NAME);

        for (int i = 0; i < KEYS_CNT; ++i)
            cache.put(i, (long)i);

        IgniteInternalFuture txLoadFut = startTxLoad(5, null);

        doSleep(10_000);

        txStop.set(true);

        txLoadFut.get();

        grid(0).cluster().active(false);

        stopAllGrids();

        assertEquals(0,findSporadicDataRecords(nodeFolderName0) + findSporadicDataRecords(nodeFolderName1));
    }

    /**
     * Returns a number of {@link DataRecord} that do not have a xid version,
     * in other words, number of records that were created by GridCacheUtils#createBackupPostProcessingClosure().
     *
     * @throws IgniteCheckedException If failed.
     */
    private long findSporadicDataRecords(String nodeFolderName) throws IgniteCheckedException {
        File dbDir = new File(U.defaultWorkDirectory(), "db");
        File commonWalDir = new File(dbDir, "wal");
        File walDir = new File(commonWalDir, nodeFolderName);
        File walArchiveDir = new File(new File(commonWalDir, "archive"), nodeFolderName);

        assertTrue(walDir.exists());
        assertTrue(walArchiveDir.exists());

        IteratorParametersBuilder params = new IteratorParametersBuilder();

        params.bufferSize(1024 * 1024);
        params.filesOrDirs(walDir, walArchiveDir);
        params.filter((type, pointer) -> type == WALRecord.RecordType.DATA_RECORD);

        int cacheId = CU.cacheId(TX_CACHE_NAME);

        long createOpCnt = 0;

        try (WALIterator itr = new IgniteWalIteratorFactory().iterator(params)) {
            while (itr.hasNext()) {
                IgniteBiTuple<WALPointer, WALRecord> walEntry = itr.next();

                assertTrue(walEntry.get2() instanceof DataRecord);

                DataRecord rec = (DataRecord)walEntry.get2();

                createOpCnt += rec.writeEntries()
                    .stream()
                    .filter(e ->
                        e.cacheId() == cacheId && GridCacheOperation.CREATE == e.op() && e.nearXidVersion() == null)
                    .count();
            }
        }

        return createOpCnt;
    }

    /**
     * @param threads Threads.
     * @param ignite Load source instance.
     */
    @SuppressWarnings({"SameParameterValue"})
    protected IgniteInternalFuture startTxLoad(int threads, Ignite ignite) {
        txStop.set(false);

        return GridTestUtils.runMultiThreadedAsync(() -> {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                while (!txStop.get()) {
                    Ignite ig = ignite == null ? grid(rnd.nextInt(GRID_CNT)) : ignite;

                    if (ig == null)
                        continue;

                    IgniteCache<Integer, Long> cache = ig.cache(TX_CACHE_NAME);

                    try (Transaction tx = ig.transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
                        int acc0 = rnd.nextInt(KEYS_CNT);

                        int acc1 = rnd.nextInt(KEYS_CNT);

                        while (acc1 == acc0)
                            acc1 = rnd.nextInt(KEYS_CNT);

                        // Avoid deadlocks.
                        if (acc0 > acc1) {
                            int tmp = acc0;
                            acc0 = acc1;
                            acc1 = tmp;
                        }

                        long val0 = cache.get(acc0);
                        long val1 = cache.get(acc1);

                        long delta = rnd.nextLong(Math.max(val0, val1));

                        if (val0 < val1) {
                            cache.put(acc0, val0 + delta);
                            cache.put(acc1, val1 - delta);
                        }
                        else {
                            cache.put(acc0, val0 - delta);
                            cache.put(acc1, val1 + delta);
                        }

                        tx.commit();
                    }
                    catch (Throwable e) {
                        assertFalse(e instanceof NullPointerException);
                    }
                }
            },
            threads, "tx-load-thread"
        );
    }
}
