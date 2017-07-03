/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import java.lang.reflect.Field;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.GridKernalState;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import sun.nio.ch.FileChannelImpl;

/**
 *
 */
public class IgniteWalFlushFailoverTest extends GridCommonAbstractTest {

    /** */
    private static final String TEST_CACHE = "testCache";

    /** */
    private boolean errorOnQueueFlusher;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        deleteWorkFiles();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        deleteWorkFiles();
    }

    /** {@inheritDoc} */
    @Override
    protected long getTestTimeout() {
        return 30_000L;
    }

    /**
     * Test flushing error recovery when flush is triggered by QueueFlusher asynchronously
     *
     * @throws Exception In case of fail
     */
    public void testErrorOnQueueFlusher() throws Exception {
        errorOnQueueFlusher = true;
        flushingErrorTest();
    }

    /**
     * Test flushing error recovery when flush is triggered directly by transaction commit
     *
     * @throws Exception In case of fail
     */
    public void testErrorOnDirectFlush() throws Exception {
        errorOnQueueFlusher = false;
        flushingErrorTest();
    }

    /**
     * @throws Exception if failed.
     */
    private void flushingErrorTest() throws Exception {
        IgniteEx grid = startGrid(0);
        grid.active(true);

        IgniteCache<Object, Object> cache = grid.cache(TEST_CACHE);

        final int iterations = 100;

        try {
            for (int i = 0; i < iterations; i++) {
                Transaction tx = grid.transactions().txStart(
                        TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED);

                cache.put(i, "testValue" + i);

                Thread.sleep(100L);

                tx.commitAsync().get();

                // Corrupt WAL after some iterations
                if (i > 15)
                    corruptWalLogFile(grid);
            }
        }
        catch (Throwable expected) {
            // There can be any exception. Do nothing.
        }

        // We should await successful stop of node.
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override
            public boolean apply() {
                return grid.context().gateway().getState() == GridKernalState.STOPPED;
            }
        }, getTestTimeout());
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration cacheCfg = new CacheConfiguration(TEST_CACHE)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        cfg.setCacheConfiguration(cacheCfg);

        MemoryPolicyConfiguration memPlcCfg = new MemoryPolicyConfiguration()
                .setName("dfltMemPlc")
                .setInitialSize(2 * 1024L * 1024L * 1024L);

        MemoryConfiguration memCfg = new MemoryConfiguration()
                .setMemoryPolicies(memPlcCfg)
                .setDefaultMemoryPolicyName(memPlcCfg.getName());

        cfg.setMemoryConfiguration(memCfg);

        PersistentStoreConfiguration storeCfg = new PersistentStoreConfiguration()
                .setWalMode(WALMode.BACKGROUND)
                // Setting WAL Segment size to high values forces flushing by QueueFlusher.
                .setWalSegmentSize(errorOnQueueFlusher ? 500_000 : 50_000);

        cfg.setPersistentStoreConfiguration(storeCfg);

        return cfg;
    }

    /**
     * @throws IgniteCheckedException
     */
    private void deleteWorkFiles() throws IgniteCheckedException {
        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false));
    }

    /**
     * This holy shit is needed to corrupt current WAL file to ensure
     * that RuntimeException is thrown during WAL flush operation.
     *
     * @param grid Node
     */
    private void corruptWalLogFile(IgniteEx grid) {
        FileWriteAheadLogManager wal = ((FileWriteAheadLogManager) grid.context().cache().context().wal());
        try {
            Field handleField = FileWriteAheadLogManager.class.getDeclaredField("currentHnd");
            handleField.setAccessible(true);
            FileWriteAheadLogManager.FileWriteHandle handle = (FileWriteAheadLogManager.FileWriteHandle) handleField.get(wal);

            Field fileChannelField = FileWriteAheadLogManager.FileHandle.class.getDeclaredField("ch");
            fileChannelField.setAccessible(true);
            FileChannelImpl fileChannel = (FileChannelImpl) fileChannelField.get(handle);

            Field writableField = FileChannelImpl.class.getDeclaredField("writable");
            writableField.setAccessible(true);

            writableField.set(fileChannel, false);
        } catch (Exception e) {
            throw new RuntimeException("Unable to corrupt WAL log file", e);
        }
    }

}
