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

package org.apache.ignite.internal.processors.cache;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionTimeoutException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests transactional entry locking through internal cache API.
 */
@RunWith(Parameterized.class)
public class LockTxEntryOneNodeTest extends GridCommonAbstractTest {
    /** */
    private static final int KEY = 1;

    /** */
    private static final int INIT_VAL = 1;

    /** */
    private static final int UPDATED_VAL = 2;

    /** */
    private static Ignite ignite;

    /** */
    @Parameterized.Parameter(0)
    public boolean commit;

    /** */
    @Parameterized.Parameter(1)
    public boolean useNearCache;

    /** */
    @Parameterized.Parameter(2)
    public CacheMode cacheMode;

    /** Operation that enlists an entry in a transaction before a versioned lock is acquired. */
    private enum TxOperation {
        /** Read. */
        GET,

        /** Update. */
        PUT,

        /** Update with previous value returned. */
        GET_AND_PUT,

        /** Delete. */
        REMOVE,

        /** Delete with previous value returned. */
        GET_AND_REMOVE,

        /** Transform. */
        INVOKE
    }

    /** Cache. */
    private IgniteCache<Integer, Integer> cache;

    /**
     * Returns data for test.
     *
     * @return Test parameters.
     */
    @Parameterized.Parameters(name = "commit={0}, useNearCache={1}, cacheMode={2}")
    public static Collection<Object[]> testData() {
        return List.of(new Object[][] {
            {false, false, PARTITIONED},
            {false, false, REPLICATED},
            {true, false, PARTITIONED},
            {true, false, REPLICATED},
            {false, true, PARTITIONED},
            {false, true, REPLICATED},
            {true, true, PARTITIONED},
            {true, true, REPLICATED},
        });
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ignite = startGrid(0);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        ignite = null;

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cache = ignite.createCache(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setNearConfiguration(useNearCache ? new NearCacheConfiguration<>() : null)
            .setCacheMode(cacheMode));

        cache.put(KEY, INIT_VAL);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        ignite.destroyCache(DEFAULT_CACHE_NAME);

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 30_000;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLockTxEntryInPessimisticTransaction() throws Exception {
        CacheEntry<Integer, Integer> entry = cache.getEntry(KEY);

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            assertTrue(acquireLockForEntry(entry, 0));

            assertEquals(entry.getValue(), cache.get(KEY));

            checkInaccessInOtherTx(cache);

            if (commit)
                tx.commit();
        }

        assertEquals(INIT_VAL, cache.get(KEY).intValue());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLockAlreadyLockedTxEntry() throws Exception {
        CacheEntry<Integer, Integer> entry = cache.getEntry(KEY);

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            cache.put(KEY, UPDATED_VAL);

            assertTrue(acquireLockForEntry(entry, 0));

            assertEquals(UPDATED_VAL, cache.get(KEY).intValue());

            checkInaccessInOtherTx(cache);

            if (commit)
                tx.commit();
        }

        assertEquals(commit ? UPDATED_VAL : INIT_VAL, cache.get(KEY).intValue());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLockAlreadyEnlistedTxEntryAfterOperation() throws Exception {
        for (TxOperation op : TxOperation.values()) {
            int key = KEY + op.ordinal() + 1;

            cache.put(key, INIT_VAL);

            CacheEntry<Integer, Integer> entry = cache.getEntry(key);

            try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                applyOperation(op, key);

                try {
                    assertTrue("Failed to lock already enlisted tx entry [op=" + op + ']',
                        acquireLockForEntry(entry, 0));
                }
                catch (AssertionError e) {
                    throw new AssertionError("Failed to lock already enlisted tx entry [op=" + op + ']', e);
                }

                checkInaccessInOtherTx(cache, key);

                if (commit)
                    tx.commit();
            }

            assertEquals("Unexpected value after transaction [op=" + op + ']',
                commit ? expectedValue(op) : Integer.valueOf(INIT_VAL),
                cache.get(key));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFailToLockTxEntryInPessimisticTransaction() throws Exception {
        CacheEntry<Integer, Integer> entry = cache.getEntry(KEY);

        cache.put(KEY, UPDATED_VAL);

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            assertFalse(acquireLockForEntry(entry, 0));

            assertEquals(UPDATED_VAL, cache.get(KEY).intValue());

            checkAccessInOtherTx(cache);

            if (commit)
                tx.commit();
        }

        assertEquals(UPDATED_VAL, cache.get(KEY).intValue());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLockTxEntryReturnsFalseOnTimeoutWhenLockedInOtherTransaction() throws Exception {
        CacheEntry<Integer, Integer> entry = cache.getEntry(KEY);

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            assertTrue(acquireLockForEntry(entry, 0));

            IgniteInternalFuture<Boolean> lockFut = GridTestUtils.runAsync(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    boolean locked = true;

                    try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                        locked = acquireLockForEntry(entry, 100);

                        assertFalse(locked);

                        cache.put(2, 2);

                        if (commit)
                            tx.commit();
                    }

                    return locked;
                }
            });

            assertFalse(lockFut.get(10_000));

            if (commit)
                tx.commit();
        }

        assertEquals(INIT_VAL, cache.get(KEY).intValue());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLockTxEntryReturnsFalseForLockedKeyAndTrueForOtherKey() throws Exception {
        int otherKey = KEY + 1;

        cache.put(otherKey, INIT_VAL);

        CacheEntry<Integer, Integer> entry = cache.getEntry(KEY);
        CacheEntry<Integer, Integer> otherEntry = cache.getEntry(otherKey);

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            assertTrue(acquireLockForEntry(entry, 0));

            IgniteInternalFuture<Void> lockFut = GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                        assertFalse(acquireLockForEntry(entry, -1));
                        assertTrue(acquireLockForEntry(otherEntry, -1));

                        if (commit)
                            tx.commit();
                    }

                    return null;
                }
            });

            lockFut.get(10_000);

            if (commit)
                tx.commit();
        }

        assertEquals(INIT_VAL, cache.get(KEY).intValue());
        assertEquals(INIT_VAL, cache.get(otherKey).intValue());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUpdateAfterLock() throws Exception {
        CacheEntry<Integer, Integer> entry = cache.getEntry(KEY);

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            assertTrue(acquireLockForEntry(entry, 0));

            checkInaccessInOtherTx(cache);

            assertEquals(entry.getValue(), cache.get(KEY));

            cache.put(KEY, UPDATED_VAL);

            assertEquals(UPDATED_VAL, cache.get(KEY).intValue());

            if (commit)
                tx.commit();
        }

        assertEquals(commit ? UPDATED_VAL : INIT_VAL, cache.get(KEY).intValue());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLockTxEntryFailsWithoutTransaction() throws Exception {
        CacheEntry<Integer, Integer> entry = cache.getEntry(KEY);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                acquireLockForEntry(entry, 0);

                return null;
            }
        }, IgniteCheckedException.class, "without transaction");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLockTxEntryAsyncFailsInOptimisticTransaction() throws Exception {
        CacheEntry<Integer, Integer> entry = cache.getEntry(KEY);

        try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, REPEATABLE_READ)) {
            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    return acquireLockForEntry(entry, 0);
                }
            }, IgniteCheckedException.class, "optimistic transaction");
        }
    }

    /**
     * Checks that another transaction can access the cache.
     *
     * @param cache Cache.
     * @throws IgniteCheckedException If failed.
     */
    private void checkAccessInOtherTx(IgniteCache<Integer, Integer> cache) throws IgniteCheckedException {
        checkAccessInOtherTx(cache, KEY);
    }

    /**
     * Checks that another transaction can access the cache.
     *
     * @param cache Cache.
     * @param key Key.
     * @throws IgniteCheckedException If failed.
     */
    private void checkAccessInOtherTx(IgniteCache<Integer, Integer> cache, int key) throws IgniteCheckedException {
        IgniteInternalFuture<Void> accessFut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() {
                try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 100, 1)) {
                    cache.get(key);

                    tx.commit();
                }

                return null;
            }
        });

        accessFut.get(10_000);
    }

    /**
     * Checks that another transaction cannot access the cache.
     *
     * @param cache Cache.
     * @throws IgniteCheckedException If failed.
     */
    private void checkInaccessInOtherTx(IgniteCache<Integer, Integer> cache) throws IgniteCheckedException {
        checkInaccessInOtherTx(cache, KEY);
    }

    /**
     * Checks that another transaction cannot access the cache.
     *
     * @param cache Cache.
     * @param key Key.
     * @throws IgniteCheckedException If failed.
     */
    private void checkInaccessInOtherTx(IgniteCache<Integer, Integer> cache, int key) throws IgniteCheckedException {
        IgniteInternalFuture<Void> accessFut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() {
                try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 100, 1)) {
                    cache.get(key);

                    tx.commit();
                }

                return null;
            }
        });

        GridTestUtils.assertThrowsWithCause(new Callable<Object>() {
            @Override public Object call() throws Exception {
                return accessFut.get();
            }
        }, TransactionTimeoutException.class);
    }

    /**
     * Acquires a transactional lock for an entry.
     *
     * @param entry Entry.
     * @param timeout Lock wait timeout.
     * @return {@code true} if the lock was acquired.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    private boolean acquireLockForEntry(
        CacheEntry<Integer, Integer> entry,
        long timeout
    ) throws IgniteCheckedException {
        return cache.unwrap(IgniteCacheProxy.class).internalProxy().lockTxEntry(entry, timeout);
    }

    /**
     * Applies cache operation inside transaction.
     *
     * @param op Operation.
     * @param key Key.
     */
    private void applyOperation(TxOperation op, int key) {
        switch (op) {
            case GET:
                assertEquals(INIT_VAL, cache.get(key).intValue());

                break;

            case PUT:
                cache.put(key, UPDATED_VAL);

                break;

            case GET_AND_PUT:
                assertEquals(INIT_VAL, cache.getAndPut(key, UPDATED_VAL).intValue());

                break;

            case REMOVE:
                assertTrue(cache.remove(key));

                break;

            case GET_AND_REMOVE:
                assertEquals(INIT_VAL, cache.getAndRemove(key).intValue());

                break;

            case INVOKE:
                cache.invoke(key, new EntryProcessor<Integer, Integer, Object>() {
                    @Override public Object process(MutableEntry<Integer, Integer> entry, Object... args)
                        throws EntryProcessorException {
                        entry.setValue(UPDATED_VAL);

                        return null;
                    }
                });

                break;

            default:
                fail("Unexpected operation: " + op);
        }
    }

    /**
     * @param op Operation.
     * @return Expected value after committed transaction.
     */
    private Integer expectedValue(TxOperation op) {
        switch (op) {
            case GET:
                return INIT_VAL;

            case PUT:
            case GET_AND_PUT:
            case INVOKE:
                return UPDATED_VAL;

            case REMOVE:
            case GET_AND_REMOVE:
                return null;

            default:
                fail("Unexpected operation: " + op);

                return null;
        }
    }
}
