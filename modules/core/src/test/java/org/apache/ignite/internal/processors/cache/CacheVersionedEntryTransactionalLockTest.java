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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests transactional locks acquired only for unchanged cache entry versions.
 */
@RunWith(Parameterized.class)
public class CacheVersionedEntryTransactionalLockTest extends GridCommonAbstractTest {
    /** */
    private static Ignite ignite0;

    /** */
    private static Ignite ignite1;

    /** */
    private static Ignite client;

    /** */
    @Parameterized.Parameter(0)
    public boolean useNearCache;

    /** */
    @Parameterized.Parameter(1)
    public int backups;

    /** */
    @Parameterized.Parameter(2)
    public boolean replicated;

    /** */
    @Parameterized.Parameter(3)
    public boolean batch;

    /**
     * Returns data for test.
     *
     * @return Test parameters.
     */
    @Parameterized.Parameters(name = "useNearCache={0}, backups={1}, replicated={2}, batch={3}")
    public static Collection<Object[]> testData() {
        return List.of(new Object[][] {
            {false, 0, false, false},
            {false, 0, false, true},
            {false, 0, true, false},
            {false, 0, true, true},
            {false, 1, false, false},
            {false, 1, false, true},
            {false, 1, true, false},
            {false, 1, true, true},
            {false, 2, false, false},
            {false, 2, false, true},
            {false, 2, true, false},
            {false, 2, true, true},
            {true, 0, false, false},
            {true, 0, false, true},
            {true, 0, true, false},
            {true, 0, true, true},
            {true, 1, false, false},
            {true, 1, false, true},
            {true, 1, true, false},
            {true, 1, true, true},
            {true, 2, false, false},
            {true, 2, false, true},
            {true, 2, true, false},
            {true, 2, true, true}
        });
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ignite0 = startGridsMultiThreaded(4);

        awaitPartitionMapExchange();

        ignite1 = grid(1);
        client = startClientGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        ignite0 = null;
        ignite1 = null;
        client = null;

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        ignite0.destroyCache(DEFAULT_CACHE_NAME);

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName);
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 120_000;
    }

    /**
     * Creates transactional cache.
     *
     * @param ignite Node.
     * @return Transactional cache.
     */
    private IgniteCache<Integer, Integer> transactionalCache(Ignite ignite) {
        CacheConfiguration<?, ?> ccfg =
            new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setNearConfiguration(useNearCache ? new NearCacheConfiguration<>() : null)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setCacheMode(replicated ? CacheMode.REPLICATED : CacheMode.PARTITIONED)
                .setBackups(backups);

        return (IgniteCache<Integer, Integer>)ignite.createCache(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLockBeforePutFromClientTest() throws Exception {
        IgniteCache<Integer, Integer> cache = transactionalCache(client);

        int key = 42;

        checkLockBeforePut(cache, key, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLockBeforePutLocalKeyTest() throws Exception {
        IgniteCache<Integer, Integer> cache = transactionalCache(ignite0);

        int key = primaryKey(cache);

        checkLockBeforePut(cache, key, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLockBeforePutRemoteKeyTest() throws Exception {
        IgniteCache<Integer, Integer> cache = transactionalCache(ignite0);

        int key = primaryKey(ignite1.cache(DEFAULT_CACHE_NAME));

        checkLockBeforePut(cache, key, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLockBeforePutLocalKeyRepeatableReadTest() throws Exception {
        IgniteCache<Integer, Integer> cache = transactionalCache(ignite0);

        int key = primaryKey(cache);

        checkLockBeforePut(cache, key, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLockBeforePutRemoteKeyRepeatableReadTest() throws Exception {
        IgniteCache<Integer, Integer> cache = transactionalCache(ignite0);

        int key = primaryKey(ignite1.cache(DEFAULT_CACHE_NAME));

        checkLockBeforePut(cache, key, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEntryVersionDoesNotChangeWhenEntryIsNotUpdated() throws Exception {
        IgniteCache<Integer, Integer> cache = transactionalCache(ignite0);

        int locKey = primaryKey(cache);
        int remoteKey = primaryKey(ignite1.cache(DEFAULT_CACHE_NAME));

        cache.put(locKey, 0);
        cache.put(remoteKey, 0);

        CacheEntry<Integer, Integer> locEntry = cache.getEntry(locKey);
        CacheEntry<Integer, Integer> remoteEntry = cache.getEntry(remoteKey);

        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            if (batch) {
                assertTrue(acquireLockForEntries(cache, List.of(locEntry, remoteEntry), 0));
            }
            else {
                assertTrue(acquireLockForEntry(cache, locEntry, 0));
                assertTrue(acquireLockForEntry(cache, remoteEntry, 0));
            }

            tx.commit();
        }

        assertEquals(locEntry.version(), cache.getEntry(locKey).version());
        assertEquals(remoteEntry.version(), cache.getEntry(remoteKey).version());

        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            if (batch) {
                assertTrue(acquireLockForEntries(cache, List.of(locEntry, remoteEntry), 0));
            }
            else {
                assertTrue(acquireLockForEntry(cache, locEntry, 0));
                assertTrue(acquireLockForEntry(cache, remoteEntry, 0));
            }

            tx.rollback();
        }

        assertEquals(locEntry.version(), cache.getEntry(locKey).version());
        assertEquals(remoteEntry.version(), cache.getEntry(remoteKey).version());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testVersionedEntryLockReturnsFalseWhenEntryIsLockedByAnotherTransactionNoWait() throws Exception {
        checkReturningWhenCanNotWaitForLock(-1, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testVersionedEntryLockReturnsFalseWhenEntryIsLockedByAnotherTransaction() throws Exception {
        checkReturningWhenCanNotWaitForLock(200, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testVersionedEntryLockReturnsFalseWhenLocalEntryIsLockedByAnotherTransaction() throws Exception {
        checkReturningWhenCanNotWaitForLock(200, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testVersionedEntryLockReturnsFalseWhenEntryIsLockedByAnotherTransactionAndCommit() throws Exception {
        checkReturningWhenCanNotWaitForLock(200, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testVersionedEntryLockReturnsFalseWhenLocalEntryIsLockedByAnotherTransactionAndCommit() throws Exception {
        checkReturningWhenCanNotWaitForLock(200, true, true);
    }

    /**
     * Checks that lock acquisition can be retried in the same transaction after the competing transaction finishes.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testVersionedEntryLockCanBeRetriedAfterWaitTimeout() throws Exception {
        Ignite holder = ignite0;
        Ignite initiator = ignite1;

        IgniteCache<Integer, Integer> holderCache = transactionalCache(holder);
        IgniteCache<Integer, Integer> cache = initiator.cache(DEFAULT_CACHE_NAME);

        int key = primaryKey(holderCache);

        holderCache.put(key, 0);

        CacheEntry<Integer, Integer> entry = cache.getEntry(key);

        try (Transaction holderTx = holder.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            holderCache.put(key, 42);

            try (Transaction tx = initiator.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                if (batch)
                    assertFalse(acquireLockForEntries(cache, List.of(entry), 200));
                else
                    assertFalse(acquireLockForEntry(cache, entry, 200));

                holderTx.rollback();

                if (batch)
                    assertTrue(acquireLockForEntries(cache, List.of(entry), 5_000));
                else
                    assertTrue(acquireLockForEntry(cache, entry, 5_000));

                cache.put(key, 1);

                tx.commit();
            }
        }

        assertEquals(1, cache.get(key).intValue());
    }

    /**
     * Checks that the lock entry method returns {@code false} when can not wait for lock.
     *
     * @param timeout Timeout.
     * @param commit Whether to commit the transaction.
     * @param locForLocal Whether the contended key should be local to the lock initiator.
     * @throws IgniteCheckedException If failed.
     */
    private void checkReturningWhenCanNotWaitForLock(long timeout, boolean commit, boolean locForLocal) throws IgniteCheckedException {
        Ignite holder = ignite0;
        Ignite initiator = ignite1;

        IgniteCache<Integer, Integer> holderCache = transactionalCache(holder);
        IgniteCache<Integer, Integer> cache = initiator.cache(DEFAULT_CACHE_NAME);

        List<Integer> firstKeySet = locForLocal ? primaryKeys(cache, 2) : primaryKeys(holderCache, 2);
        Integer concurentLockedKey = firstKeySet.get(0);
        Integer txKey1 = firstKeySet.get(1);
        Integer txKey2 = locForLocal ? primaryKey(holderCache) : primaryKey(cache);

        holderCache.put(concurentLockedKey, 0);
        holderCache.put(txKey1, 0);
        holderCache.put(txKey2, 0);

        CacheEntry<Integer, Integer> entry = cache.getEntry(concurentLockedKey);

        try (Transaction holderTx = holder.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            holderCache.put(concurentLockedKey, 42);

            try (Transaction tx = initiator.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                long startWaiting = System.currentTimeMillis();

                if (batch) {
                    assertFalse(acquireLockForEntries(cache, List.of(
                        cache.getEntry(txKey1),
                        entry,
                        cache.getEntry(txKey2)
                        ), timeout));

                    assertTrue(acquireLockForEntries(cache, List.of(
                        cache.getEntry(txKey1),
                        cache.getEntry(txKey2)
                    ), timeout));
                }
                else {
                    assertTrue(acquireLockForEntry(cache, cache.getEntry(txKey1), timeout));
                    assertTrue(acquireLockForEntry(cache, cache.getEntry(txKey2), timeout));

                    assertFalse(acquireLockForEntry(cache, entry, timeout));
                }

                long waitTime = System.currentTimeMillis() - startWaiting;

                assertTrue("Waited for lock for " + waitTime + " ms but timeout is " + timeout + " ms.",
                    waitTime > timeout - 50 && waitTime < timeout + 300);

                cache.put(txKey1, 42);
                cache.put(txKey2, 42);

                if (commit)
                    tx.commit();
            }

            holderTx.rollback();
        }

        assertEquals(0, holderCache.get(concurentLockedKey).intValue());
        assertEquals(0, cache.get(concurentLockedKey).intValue());

        if (commit) {
            assertEquals(42, holderCache.get(txKey1).intValue());
            assertEquals(42, cache.get(txKey2).intValue());
        }
        else {
            assertEquals(0, holderCache.get(txKey1).intValue());
            assertEquals(0, cache.get(txKey2).intValue());
        }
    }

    /**
     * Checks locking an entry before updating it.
     *
     * @param cache Cache.
     * @param key Key.
     * @param txIsolation Transaction isolation.
     * @throws IgniteCheckedException If failed.
     */
    private void checkLockBeforePut(
        IgniteCache<Integer, Integer> cache,
        int key,
        TransactionIsolation txIsolation
    ) throws IgniteCheckedException {
        cache.put(key, 0);

        CacheEntry<Integer, Integer> entry = cache.getEntry(key);

        assertNotNull(entry);
        assertNotNull(entry.version());

        Ignite ign = cache.unwrap(Ignite.class);

        try (Transaction tx = ign.transactions().txStart(PESSIMISTIC, txIsolation)) {
            if (batch)
                assertTrue(acquireLockForEntries(cache, List.of(entry), 0));
            else
                assertTrue(acquireLockForEntry(cache, entry, 0));

            assertEquals(0, cache.get(key).intValue());

            cache.put(key, 1);

            assertEquals(1, cache.get(key).intValue());

            tx.commit();
        }

        assertEquals(1, cache.get(key).intValue());
        assertTrue(cache.getEntry(key).version().compareTo(entry.version()) > 0);
    }

    /**
     * Acquires a transactional lock for an entry.
     *
     * @param cache Cache.
     * @param entry Entry.
     * @param timeout Lock wait timeout.
     * @return {@code true} if the lock was acquired.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    private static boolean acquireLockForEntry(
        IgniteCache<Integer, Integer> cache,
        CacheEntry<Integer, Integer> entry,
        long timeout
    ) throws IgniteCheckedException {
        return cache.unwrap(IgniteCacheProxy.class).internalProxy().lockTxEntry(entry, timeout);
    }

    /**
     * Acquires transactional locks for entries.
     *
     * @param cache Cache.
     * @param entries Entries.
     * @param timeout Lock wait timeout.
     * @return {@code true} if all locks were acquired.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    private static boolean acquireLockForEntries(
        IgniteCache<Integer, Integer> cache,
        List<CacheEntry<Integer, Integer>> entries,
        long timeout
    ) throws IgniteCheckedException {
        return cache.unwrap(IgniteCacheProxy.class).internalProxy().lockTxEntries(entries, timeout);
    }
}
