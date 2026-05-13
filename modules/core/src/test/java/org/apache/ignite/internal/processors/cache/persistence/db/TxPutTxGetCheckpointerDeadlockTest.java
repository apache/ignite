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

package org.apache.ignite.internal.processors.cache.persistence.db;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointTimeoutLock;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/**
 * Test verifies there is no deadlock between GridCacheMapEntry.unswap() in tx put operation and a checkpointer requesting cp write lock
 * with a parallel tx get operation.
 * <p/>
 * Root cause of the deadlock was wrong locking order when cp readlock is acquired under already locked GridCacheMapEntry instance
 * by the first tx put op.
 */
public class TxPutTxGetCheckpointerDeadlockTest extends GridCommonAbstractTest {
    /** */
    private final AtomicBoolean deadlockDetected = new AtomicBoolean(false);

    /** */
    private final AtomicBoolean testFinished = new AtomicBoolean(false);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                )
        );

        cfg.setFailureHandler(new StopNodeFailureHandler());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        testFinished.set(true);

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Tests for the absence of a deadlock between transactional cache operations and checkpointer write lock acquisition.
     * <p>
     * This test simulates a scenario where:
     * <ul>
     *   <li>One thread performs continuous transactional {@code put} operations on a cache entry.</li>
     *   <li>Another thread performs continuous transactional {@code get} operations on the same entry, potentially
     *       triggering unswapping of the entry under lock.</li>
     *   <li>A third thread repeatedly acquires and releases the checkpoint write lock, simulating checkpointer activity.</li>
     * </ul>
     * </p>
     * <p>
     * The primary purpose is to verify that there is no deadlock caused by incorrect lock ordering,
     * specifically when a transactional {@code put} holds a lock on a {@link org.apache.ignite.internal.processors.cache.GridCacheMapEntry}
     * while attempting to acquire a checkpoint read lock, at the same time as the checkpointer
     * tries to acquire checkpoint write lock.
     * </p>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDeadlock() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = ignite
            .getOrCreateCache(new CacheConfiguration<>("test").setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        GridTestUtils.runAsync(
            () -> {
                while (!testFinished.get()) {
                    try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                        cache.put(0, 1);
                        tx.commit();
                    }
                }
            },
            "write-tx-runner"
        );

        GridTestUtils.runAsync(
            () -> {
                while (!testFinished.get()) {
                    try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                        cache.get(0);
                    }
                }
            },
            "read-tx-runner"
        );

        GridTestUtils.runAsync(
            () -> {
                IgniteCacheDatabaseSharedManager db = ignite.context().cache().context().database();
                CheckpointManager cpMgr = ((GridCacheDatabaseSharedManager)db).getCheckpointManager();
                CheckpointTimeoutLock timeoutLock = cpMgr.checkpointTimeoutLock();
                ReentrantReadWriteLock.WriteLock cpWriteLock = GridTestUtils.getFieldValue(timeoutLock,
                    "checkpointReadWriteLock", "checkpointLock", "writeLock");

                while (!testFinished.get()) {
                    // An interruptible version of lock method is used to allow end the deadlock at the end of the test.
                    try {
                        if (!cpWriteLock.tryLock(4, TimeUnit.SECONDS)) {
                            deadlockDetected.set(true);
                            testFinished.set(true);

                            return;
                        }
                    }
                    finally {
                        cpWriteLock.unlock();
                    }
                }
            },
            "cp-write-lock-switching-runner"
        );

        assertFalse("Unexpected deadlock detected", GridTestUtils.waitForCondition(deadlockDetected::get, 10_000L));
    }
}
