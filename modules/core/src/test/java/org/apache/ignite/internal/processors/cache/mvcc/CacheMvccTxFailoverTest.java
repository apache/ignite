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

package org.apache.ignite.internal.processors.cache.mvcc;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.WalStateManager;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 * Check Tx state recovery from WAL.
 */
public class CacheMvccTxFailoverTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryVmIpFinder FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(FINDER))
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setMaxSize(100_000_000L)
                    .setPersistenceEnabled(true))
                .setWalMode(WALMode.BACKGROUND)
            )
            .setMvccVacuumFrequency(Long.MAX_VALUE)
            .setCacheConfiguration(cacheConfiguration());
    }

    /**
     * @return Cache configuration.
     */
    @SuppressWarnings("unchecked")
    protected CacheConfiguration<Integer, Integer> cacheConfiguration() {
        return defaultCacheConfiguration()
            .setNearConfiguration(null)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
    }

    /**
     * @throws Exception If fails.
     */
    public void testSingleNodeTxMissedRollback() throws Exception {
        checkSingleNodeRestart(true, false);
    }

    /**
     * @throws Exception If fails.
     */
    public void testSingleNodeTxMissedRollbackNoCheckpoint() throws Exception {
        checkSingleNodeRestart(true, true);
    }

    /**
     * @throws Exception If fails.
     */
    public void testSingleNodeTxMissedCommit() throws Exception {
        checkSingleNodeRestart(false, false);
    }

    /**
     * @throws Exception If fails.
     */
    public void testSingleNodeTxMissedCommitNoCheckpoint() throws Exception {
        checkSingleNodeRestart(false, true);
    }

    /**
     * @throws Exception If fails.
     */
    public void checkSingleNodeRestart(boolean rollBack, boolean disableCheckpointer) throws Exception {
        IgniteEx node = startGrid(0);

        node.cluster().active(true);

        IgniteCache<Integer, Integer> cache = node.getOrCreateCache(DEFAULT_CACHE_NAME);

        cache.put(1, 1);
        cache.put(2, 1);

        IgniteTransactions txs = node.transactions();

        IgniteWriteAheadLogManager wal = node.context().cache().context().wal();

        if (disableCheckpointer)
            ((GridCacheDatabaseSharedManager)node.context().cache().context().database()).enableCheckpoints(false).get();

        GridTimeoutProcessor.CancelableTask flushTask = GridTestUtils.getFieldValue(wal, FileWriteAheadLogManager.class, "backgroundFlushSchedule");
        WalStateManager.WALDisableContext wctx = GridTestUtils.getFieldValue(wal, FileWriteAheadLogManager.class, "walDisableContext");

        // Disable checkpoint and WAL flusher.
        node.context().timeout().removeTimeoutObject(flushTask);

        try (Transaction tx = txs.txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
            assertEquals((Integer)1, cache.get(1));
            cache.put(2, 2);

            flushTask.onTimeout(); // Flush WAL.

            // Disable wal.
            GridTestUtils.setFieldValue(wctx, "disableWal", true);

            if (rollBack)
                tx.rollback();
            else
                tx.commit();

            GridTestUtils.setFieldValue(wctx, "disableWal", false);
        }

        stopGrid(0);

        node = startGrid(0);
        cache = node.cache(DEFAULT_CACHE_NAME);

        assertEquals((Integer)1, cache.get(1));

        if (!rollBack && !disableCheckpointer)
            assertEquals((Integer)2, cache.get(2)); // Checkpoint has happened on node stop.
        else
            assertEquals((Integer)1, cache.get(2));

        cache.put(2, 3);

        assertEquals((Integer)3, cache.get(2));
    }
}
