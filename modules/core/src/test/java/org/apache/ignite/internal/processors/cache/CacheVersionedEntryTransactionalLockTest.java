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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
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
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.GridNearUnlockRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockResponse;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
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
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
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

    /** A batch retains successful locks and reports changed, deleted and contended entries individually. */
    @Test
    public void testPerEntryResults() throws Exception {
        transactionalCache(ignite0);

        Ignite initiator = batch ? client : ignite0;
        IgniteCache<Integer, Integer> cache = initiator.cache(DEFAULT_CACHE_NAME);
        List<Integer> keys = primaryKeys(ignite0.cache(DEFAULT_CACHE_NAME), 5);

        for (int key : keys)
            cache.put(key, 0);

        List<CacheEntry<Integer, Integer>> entries = List.of(
            cache.getEntry(keys.get(0)), cache.getEntry(keys.get(1)), cache.getEntry(keys.get(2)),
            cache.getEntry(keys.get(3)), cache.getEntry(keys.get(4)));

        cache.put(keys.get(1), 1);
        cache.remove(keys.get(2));

        IgniteCache<Integer, Integer> holderCache = ignite1.cache(DEFAULT_CACHE_NAME);
        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(initiator);

        try (Transaction holder = ignite1.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            holderCache.put(keys.get(3), 1);
            spi.record(GridNearUnlockRequest.class);

            try (Transaction tx = initiator.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                Map<CacheEntry<Integer, Integer>, Boolean> res = batch
                    ? internalCache(cache).lockTxEntriesAsync(entries, -1).get()
                    : internalCache(cache).lockTxEntries(entries, -1);

                assertEquals(5, res.size());
                assertEquals(Boolean.TRUE, res.get(entries.get(0)));
                assertEquals(Boolean.FALSE, res.get(entries.get(1)));
                assertEquals(Boolean.FALSE, res.get(entries.get(2)));
                assertEquals(Boolean.FALSE, res.get(entries.get(3)));
                assertEquals(Boolean.TRUE, res.get(entries.get(4)));
                assertTrue("Rejected primary locks must not require an initiating-side unlock",
                    spi.recordedMessages(false).isEmpty());

                IgniteCache<Integer, Integer> other = grid(2).cache(DEFAULT_CACHE_NAME);

                try (Transaction competitor = grid(2).transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                    assertFalse(acquireLockForEntry(other, entries.get(0), -1));
                    assertFalse(acquireLockForEntry(other, entries.get(4), -1));
                    assertTrue(acquireLockForEntry(other, other.getEntry(keys.get(1)), -1));
                }

                CacheEntry<Integer, Integer> current = cache.getEntry(keys.get(1));
                Map<CacheEntry<Integer, Integer>, Boolean> repeated = internalCache(cache)
                    .lockTxEntries(List.of(current, entries.get(1)), -1);

                assertEquals(Boolean.TRUE, repeated.get(current));
                assertEquals(Boolean.FALSE, repeated.get(entries.get(1)));

                // Failed entries must not invalidate the transaction or release successful locks.
                cache.put(keys.get(0), 2);
                cache.put(keys.get(4), 2);
                tx.commit();
            }
            finally {
                spi.recordedMessages(true);
            }
        }

        assertEquals(Integer.valueOf(2), cache.get(keys.get(0)));
        assertEquals(Integer.valueOf(2), cache.get(keys.get(4)));
    }

    /** Each primary receives one batch with individual outcomes, including partial failure and wait expiry. */
    @Test
    public void testRequestsAreBatchedByPrimary() throws Exception {
        transactionalCache(ignite0);

        IgniteCache<Integer, Integer> cache = client.cache(DEFAULT_CACHE_NAME);
        List<CacheEntry<Integer, Integer>> entries = new ArrayList<>();
        List<Integer> contended = new ArrayList<>();

        for (int node = 0; node < 3; node++) {
            List<Integer> keys = primaryKeys(grid(node).cache(DEFAULT_CACHE_NAME), 3);

            for (Integer key : keys) {
                cache.put(key, 0);
                entries.add(cache.getEntry(key));
            }

            cache.put(keys.get(1), 1);

            // The first primary rejects its entire batch; the remaining primaries must still be contacted.
            if (node == 0)
                cache.put(keys.get(0), 1);

            contended.add(keys.get(2));
        }

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(client);

        try (Transaction holder = ignite0.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            IgniteCache<Integer, Integer> holderCache = ignite0.cache(DEFAULT_CACHE_NAME);

            for (Integer key : contended)
                holderCache.put(key, 1);

            spi.record(GridNearLockRequest.class);

            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                Map<CacheEntry<Integer, Integer>, Boolean> results = internalCache(cache)
                    .lockTxEntries(entries, batch ? 200 : -1);

                assertEquals(entries.size(), results.size());

                for (int i = 0; i < entries.size(); i++)
                    assertEquals(i >= 3 && i % 3 == 0, results.get(entries.get(i)).booleanValue());

                List<Object> requests = spi.recordedMessages(true);

                assertEquals("One batch per primary, rather than one request per key", 3, requests.size());

                for (Object msg : requests) {
                    GridNearLockRequest req = (GridNearLockRequest)msg;

                    assertEquals(3, req.keys().size());
                    assertEquals(3, req.expectedVersions().length);
                }

                for (Map.Entry<CacheEntry<Integer, Integer>, Boolean> result : results.entrySet()) {
                    if (result.getValue())
                        cache.put(result.getKey().getKey(), 2);
                }

                tx.commit();
            }
            finally {
                spi.recordedMessages(true);
            }
        }
    }

    /** A version changed by the preceding owner must be rejected after waiting, with the transaction still usable. */
    @Test
    public void testVersionChangedWhileWaiting() throws Exception {
        checkVersionAfterWaiting(true);
    }

    /** Waiting for a rollback succeeds because the protected data version has not changed. */
    @Test
    public void testVersionUnchangedAfterWaiting() throws Exception {
        checkVersionAfterWaiting(false);
    }

    /** A transaction timeout must remain an error, rather than a normal per-entry rejection. */
    @Test
    public void testTransactionTimeoutIsNotRejection() throws Exception {
        IgniteCache<Integer, Integer> cache = transactionalCache(ignite0);
        int key = primaryKey(cache);

        cache.put(key, 0);

        IgniteCache<Integer, Integer> remote = client.cache(DEFAULT_CACHE_NAME);
        CacheEntry<Integer, Integer> entry = remote.getEntry(key);

        try (Transaction holder = ignite0.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            cache.put(key, 1);

            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, READ_COMMITTED, 200, 0)) {
                GridTestUtils.assertThrows(log, () -> internalCache(remote).lockTxEntries(List.of(entry), 0),
                    IgniteTxTimeoutCheckedException.class, null);
            }
        }
    }

    /**
     * @param commit Whether the preceding owner commits its update.
     * @throws Exception If failed.
     */
    private void checkVersionAfterWaiting(boolean commit) throws Exception {
        IgniteCache<Integer, Integer> cache = transactionalCache(ignite0);
        int key = primaryKey(cache);

        cache.put(key, 0);

        CacheEntry<Integer, Integer> entry = client.<Integer, Integer>cache(DEFAULT_CACHE_NAME).getEntry(key);
        IgniteInternalFuture<?> waiter;

        try (Transaction holder = ignite0.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            cache.put(key, 1);

            waiter = GridTestUtils.runAsync(() -> {
                IgniteCache<Integer, Integer> remote = client.cache(DEFAULT_CACHE_NAME);

                try (Transaction tx = client.transactions().txStart(PESSIMISTIC, READ_COMMITTED, 10_000, 0)) {
                    // Exercise both an explicit wait limit and the transaction's remaining timeout.
                    long wait = batch ? 5_000 : 0;

                    assertEquals(!commit, acquireLockForEntry(remote, entry, wait));

                    if (commit)
                        assertTrue(acquireLockForEntry(remote, remote.getEntry(key), -1));

                    remote.put(key, 2);
                    tx.commit();
                }

                return null;
            });

            awaitBlockedLock(cache, key);

            if (commit)
                holder.commit();
            else
                holder.rollback();
        }

        waiter.get(10_000);

        assertEquals(Integer.valueOf(2), cache.get(key));
    }

    /** A successful primary check protects the version even while its reply has not reached the initiator. */
    @Test
    public void testVersionProtectedBeforeReply() throws Exception {
        IgniteCache<Integer, Integer> cache = transactionalCache(ignite0);
        int key = primaryKey(cache);

        cache.put(key, 0);

        CacheEntry<Integer, Integer> entry = client.<Integer, Integer>cache(DEFAULT_CACHE_NAME).getEntry(key);
        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(ignite0);

        spi.blockMessages((node, msg) -> node.id().equals(client.cluster().localNode().id())
            && msg instanceof GridNearLockResponse && ((GridNearLockResponse)msg).lockAcquired());

        IgniteInternalFuture<?> locker = GridTestUtils.runAsync(() -> {
            IgniteCache<Integer, Integer> remote = client.cache(DEFAULT_CACHE_NAME);

            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, READ_COMMITTED, 10_000, 0)) {
                assertTrue(acquireLockForEntry(remote, entry, -1));
                tx.commit();
            }

            return null;
        });

        IgniteInternalFuture<?> writer = null;

        try {
            assertTrue(spi.waitForBlocked(1, 5_000));

            IgniteCache<Integer, Integer> competitorCache = ignite1.cache(DEFAULT_CACHE_NAME);

            writer = GridTestUtils.runAsync(() -> competitorCache.put(key, 1));

            awaitBlockedLock(cache, key);

            assertFalse(writer.isDone());
            assertEquals(entry.version(), cache.getEntry(key).version());
        }
        finally {
            spi.stopBlock();
        }

        locker.get(10_000);
        assertNotNull(writer);
        writer.get(10_000);

        assertEquals(Integer.valueOf(1), cache.get(key));
        assertFalse(entry.version().equals(cache.getEntry(key).version()));
    }

    /** Waits for a second transaction to enqueue on the primary, without relying on a sleep. */
    private void awaitBlockedLock(IgniteCache<Integer, Integer> cache, int key) throws Exception {
        GridCacheContext<?, ?> ctx = internalCache(cache).context();

        if (ctx.isNear())
            ctx = ctx.near().dht().context();

        GridCacheEntryEx primaryEntry = ctx.dht().entryEx(ctx.toCacheKeyObject(key));

        assertTrue("The competing request must actually wait on the primary",
            GridTestUtils.waitForCondition(() -> {
                try {
                    return primaryEntry.localCandidates().size() >= 2;
                }
                catch (GridCacheEntryRemovedException e) {
                    return false;
                }
            }, 5_000));
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
                long startWaiting = U.currentTimeMillis();

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

                long waitTime = U.currentTimeMillis() - startWaiting;

                assertTrue("Waited for lock for " + waitTime + " ms but timeout is " + timeout + " ms.",
                    waitTime >= timeout);

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
        return !cache.unwrap(IgniteCacheProxy.class).internalProxy().lockTxEntries(entries, timeout).containsValue(false);
    }
}
