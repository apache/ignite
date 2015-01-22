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

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.kernal.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.internal.util.typedef.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;
import org.jdk8.backport.*;

import javax.cache.*;
import javax.cache.configuration.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.events.IgniteEventType.*;
import static org.apache.ignite.cache.GridCacheAtomicityMode.*;
import static org.apache.ignite.cache.GridCacheDistributionMode.*;
import static org.apache.ignite.transactions.IgniteTxConcurrency.*;
import static org.apache.ignite.transactions.IgniteTxIsolation.*;

/**
 * Test for group locking.
 */
public abstract class GridCacheGroupLockAbstractSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Event wait timeout. */
    private static final int WAIT_TIMEOUT = 3000;

    /** */
    private TestStore store;

    /** @return Grid count to run in test. */
    protected int gridCount() {
        return 1;
    }

    /** @return Whether near cache is enabled. */
    protected abstract boolean nearEnabled();

    /** @return Cache mode for test. */
    protected abstract GridCacheMode cacheMode();

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(cacheMode());
        cacheCfg.setDistributionMode(nearEnabled() ? NEAR_PARTITIONED : PARTITIONED_ONLY);
        cacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        cacheCfg.setCacheStoreFactory(new Factory<CacheStore<? super Object, ? super Object>>() {
            @Override public CacheStore<? super Object, ? super Object> create() {
                return store;
            }
        });
        cacheCfg.setReadThrough(true);
        cacheCfg.setWriteThrough(true);
        cacheCfg.setLoadPreviousValue(true);

        cfg.setCacheConfiguration(cacheCfg);
        cfg.setCacheSanityCheckEnabled(sanityCheckEnabled());

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

        commSpi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(commSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        store = new TestStore();

        startGridsMultiThreaded(gridCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        store = null;

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    public void testGroupLockPutOneKeyOptimistic() throws Exception {
        checkGroupLockPutOneKey(OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGroupLockPutOneKeyPessimistic() throws Exception {
        checkGroupLockPutOneKey(PESSIMISTIC);
    }

    /**
     * @param concurrency Transaction concurrency mode.
     * @throws Exception If failed.
     */
    private void checkGroupLockPutOneKey(IgniteTxConcurrency concurrency) throws Exception {
        CollectingEventListener locks = new CollectingEventListener();
        CollectingEventListener unlocks = new CollectingEventListener();

        grid(0).events().localListen(locks, EVT_CACHE_OBJECT_LOCKED);
        grid(0).events().localListen(unlocks, EVT_CACHE_OBJECT_UNLOCKED);

        UUID affinityKey = primaryKeyForCache(grid(0));

        GridCache<GridCacheAffinityKey<String>, String> cache = grid(0).cache(null);

        GridCacheAffinityKey<String> key1;
        GridCacheAffinityKey<String> key2;

        try (IgniteTx tx = cache.txStartAffinity(affinityKey, concurrency, READ_COMMITTED, 0, 2)) {
            if (concurrency == PESSIMISTIC)
                assertTrue("Failed to wait for lock events: " + affinityKey, locks.awaitKeys(WAIT_TIMEOUT, affinityKey));
            else
                assertEquals("Unexpected number of lock events: " + locks.affectedKeys(), 0, locks.affectedKeys().size());

            assertEquals("Unexpected number of unlock events: " + unlocks.affectedKeys(), 0, unlocks.affectedKeys().size());

            key1 = new GridCacheAffinityKey<>("key1", affinityKey);
            key2 = new GridCacheAffinityKey<>("key2", affinityKey);

            cache.putAll(F.asMap(
                key1, "val1",
                key2, "val2")
            );

            tx.commit();
        }

        // Check that there are no further locks after transaction commit.
        assertEquals("Unexpected number of lock events: " + locks.affectedKeys(), 1, locks.affectedKeys().size());
        assertTrue("Failed to wait for unlock events: " + affinityKey, unlocks.awaitKeys(WAIT_TIMEOUT, affinityKey));

        for (int i = 0; i < gridCount(); i++) {
            Ignite g = grid(i);

            GridCache<Object, Object> gCache = g.cache(null);

            if (gCache.affinity().isPrimaryOrBackup(g.cluster().localNode(), key1))
                assertEquals("For index: " + i, "val1", gCache.peek(key1));

            if (gCache.affinity().isPrimaryOrBackup(g.cluster().localNode(), key2))
                assertEquals("For index: " + i, "val2", gCache.peek(key2));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGroupLockRemoveOneKeyOptimistic() throws Exception {
        checkGroupLockRemoveOneKey(OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGroupLockRemoveOneKeyPessimistic() throws Exception {
        checkGroupLockRemoveOneKey(PESSIMISTIC);
    }

    /**
     * @param concurrency Transaction concurrency mode.
     * @throws Exception If failed.
     */
    private void checkGroupLockRemoveOneKey(IgniteTxConcurrency concurrency) throws Exception {
        CollectingEventListener locks = new CollectingEventListener();
        CollectingEventListener unlocks = new CollectingEventListener();

        UUID affinityKey = primaryKeyForCache(grid(0));

        GridCacheAffinityKey<String> key1 = new GridCacheAffinityKey<>("key1", affinityKey);
        GridCacheAffinityKey<String> key2 = new GridCacheAffinityKey<>("key2", affinityKey);

        GridCache<GridCacheAffinityKey<String>, String> cache = grid(0).cache(null);

        // Populate cache.
        cache.putAll(F.asMap(
            key1, "val1",
            key2, "val2")
        );

        for (int i = 0; i < gridCount(); i++) {
            Ignite g = grid(i);

            GridCache<Object, Object> gCache = g.cache(null);

            if (gCache.affinity().isPrimaryOrBackup(g.cluster().localNode(), key1))
                assertEquals("For index: " + i, "val1", gCache.peek(key1));

            if (gCache.affinity().isPrimaryOrBackup(g.cluster().localNode(), key2))
                assertEquals("For index: " + i, "val2", gCache.peek(key2));
        }

        grid(0).events().localListen(locks, EVT_CACHE_OBJECT_LOCKED);
        grid(0).events().localListen(unlocks, EVT_CACHE_OBJECT_UNLOCKED);

        try (IgniteTx tx = cache.txStartAffinity(affinityKey, concurrency, READ_COMMITTED, 0, 2)) {
            if (concurrency == PESSIMISTIC)
                assertTrue("Failed to wait for lock events: " + affinityKey, locks.awaitKeys(WAIT_TIMEOUT, affinityKey));
            else
                assertEquals("Unexpected number of lock events: " + locks.affectedKeys(), 0, locks.affectedKeys().size());

            assertEquals("Unexpected number of unlock events: " + unlocks.affectedKeys(), 0, unlocks.affectedKeys().size());


            cache.removeAll(F.asList(key1, key2));

            tx.commit();
        }

        // Check that there are no further locks after transaction commit.
        assertEquals("Unexpected number of lock events: " + locks.affectedKeys(), 1, locks.affectedKeys().size());
        assertTrue("Failed to wait for unlock events: " + affinityKey, unlocks.awaitKeys(WAIT_TIMEOUT, affinityKey));

        for (int i = 0; i < gridCount(); i++) {
            Ignite g = grid(i);

            GridCache<Object, Object> gCache = g.cache(null);

            if (gCache.affinity().isPrimaryOrBackup(g.cluster().localNode(), key1))
                assertNull("For index: " + i, gCache.peek(key1));

            if (gCache.affinity().isPrimaryOrBackup(g.cluster().localNode(), key2))
                assertNull("For index: " + i, gCache.peek(key2));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGroupLockGetOneKeyOptimistic() throws Exception {
        checkGroupLockGetOneKey(OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGroupLockGetOneKeyPessimistic() throws Exception {
        checkGroupLockGetOneKey(PESSIMISTIC);
    }

    /**
     * @param concurrency Transaction concurrency mode.
     * @throws Exception If failed.
     */
    private void checkGroupLockGetOneKey(IgniteTxConcurrency concurrency) throws Exception {
        CollectingEventListener locks = new CollectingEventListener();
        CollectingEventListener unlocks = new CollectingEventListener();

        UUID affinityKey = primaryKeyForCache(grid(0));

        GridCacheAffinityKey<String> key1 = new GridCacheAffinityKey<>("key1", affinityKey);
        GridCacheAffinityKey<String> key2 = new GridCacheAffinityKey<>("key2", affinityKey);

        GridCache<GridCacheAffinityKey<String>, String> cache = grid(0).cache(null);

        // Populate cache.
        cache.putAll(F.asMap(
            key1, "val1",
            key2, "val2")
        );

        for (int i = 0; i < gridCount(); i++) {
            Ignite g = grid(i);

            GridCache<Object, Object> gCache = g.cache(null);

            if (gCache.affinity().isPrimaryOrBackup(g.cluster().localNode(), key1))
                assertEquals("For index: " + i, "val1", gCache.peek(key1));

            if (gCache.affinity().isPrimaryOrBackup(g.cluster().localNode(), key2))
                assertEquals("For index: " + i, "val2", gCache.peek(key2));
        }

        grid(0).events().localListen(locks, EVT_CACHE_OBJECT_LOCKED);
        grid(0).events().localListen(unlocks, EVT_CACHE_OBJECT_UNLOCKED);

        try (IgniteTx tx = cache.txStartAffinity(affinityKey, concurrency, READ_COMMITTED, 0, 2)) {
            if (concurrency == PESSIMISTIC)
                assertTrue("Failed to wait for lock events: " + affinityKey, locks.awaitKeys(WAIT_TIMEOUT, affinityKey));
            else
                assertEquals("Unexpected number of lock events: " + locks.affectedKeys(), 0, locks.affectedKeys().size());

            assertEquals("Unexpected number of unlock events: " + unlocks.affectedKeys(), 0, unlocks.affectedKeys().size());

            assertEquals("val1", cache.get(key1));

            assertEquals("val2", cache.get(key2));

            tx.commit();
        }

        // Check that there are no further locks after transaction commit.
        assertEquals("Unexpected number of lock events: " + locks.affectedKeys(), 1, locks.affectedKeys().size());
        assertTrue("Failed to wait for unlock events: " + affinityKey, unlocks.awaitKeys(WAIT_TIMEOUT, affinityKey));
    }

    /**
     * @throws Exception If failed.
     */
    public void testGroupLockWithExternalLockOptimistic() throws Exception {
        checkGroupLockWithExternalLock(OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGroupLockWithExternalLockPessimistic() throws Exception {
        checkGroupLockWithExternalLock(PESSIMISTIC);
    }

    /**
     * @param concurrency Transaction concurrency mode.
     * @throws Exception If failed.
     */
    private void checkGroupLockWithExternalLock(final IgniteTxConcurrency concurrency) throws Exception {
        assert sanityCheckEnabled();

        final UUID affinityKey = primaryKeyForCache(grid(0));

        final GridCacheAffinityKey<String> key1 = new GridCacheAffinityKey<>("key1", affinityKey);

        final IgniteCache<GridCacheAffinityKey<String>, String> cache = grid(0).jcache(null);

        // Populate cache.
        cache.put(key1, "val1");

        for (int i = 0; i < gridCount(); i++) {
            Ignite g = grid(i);

            GridCache<Object, Object> gCache = g.cache(null);

            if (gCache.affinity().isPrimaryOrBackup(g.cluster().localNode(), key1))
                assertEquals("For index: " + i, "val1", gCache.peek(key1));
        }

        final CountDownLatch unlockLatch = new CountDownLatch(1);
        final CountDownLatch lockLatch = new CountDownLatch(1);

        IgniteFuture<?> fut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                try {
                    cache.lock(key1).lock();

                    try {
                        lockLatch.countDown();
                        unlockLatch.await();
                    }
                    finally {
                        cache.lock(key1).unlock();
                    }
                }
                catch (CacheException e) {
                    fail(e.getMessage());
                }
                catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
        }, 1);

        try {
            lockLatch.await();

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    try (IgniteTx tx = grid(0).transactions().txStartAffinity(null, affinityKey, concurrency,
                        READ_COMMITTED, 0, 1)) {
                        cache.put(key1, "val01");

                        tx.commit();
                    }

                    return null;
                }
            }, IgniteTxHeuristicException.class, null);
        }
        finally {
            unlockLatch.countDown();

            fut.get();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSanityCheckDisabledOptimistic() throws Exception {
        checkSanityCheckDisabled(OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSanityCheckDisabledPessimistic() throws Exception {
        checkSanityCheckDisabled(PESSIMISTIC);
    }

    /**
     * @param concurrency Transaction concurrency mode.
     * @throws Exception If failed.
     */
    private void checkSanityCheckDisabled(final IgniteTxConcurrency concurrency) throws Exception {
        assert !sanityCheckEnabled();

        GridEx grid = grid(0);

        final UUID affinityKey = primaryKeyForCache(grid);

        final GridCacheAffinityKey<String> key1 = new GridCacheAffinityKey<>("key1", affinityKey);

        final IgniteCache<GridCacheAffinityKey<String>, String> cache = grid.jcache(null);

        // Populate cache.
        cache.put(key1, "val1");

        for (int i = 0; i < gridCount(); i++) {
            Ignite g = grid(i);

            GridCache<Object, Object> gCache = g.cache(null);

            if (gCache.affinity().isPrimaryOrBackup(g.cluster().localNode(), key1))
                assertEquals("For index: " + i, "val1", gCache.peek(key1));
        }

        cache.lock(key1).lock();

        try {
            try (IgniteTx tx = grid.transactions().txStartAffinity(null, affinityKey, concurrency, READ_COMMITTED, 0, 1)) {
                cache.put(key1, "val01");

                tx.commit();
            }

            for (int i = 0; i < gridCount(); i++) {
                Ignite g = grid(i);

                GridCache<Object, Object> gCache = g.cache(null);

                if (gCache.affinity().isPrimaryOrBackup(g.cluster().localNode(), key1))
                    assertEquals("For index: " + i, "val01", gCache.peek(key1));
            }
        }
        finally {
            cache.lock(key1).unlock();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGroupPartitionLockOptimistic() throws Exception {
        checkGroupPartitionLock(OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGroupPartitionLockPessimistic() throws Exception {
        checkGroupPartitionLock(PESSIMISTIC);
    }

    /**
     * @param concurrency Transaction concurrency mode.
     * @throws Exception If failed.
     */
    private void checkGroupPartitionLock(IgniteTxConcurrency concurrency) throws Exception {
        CollectingEventListener locks = new CollectingEventListener();
        CollectingEventListener unlocks = new CollectingEventListener();

        grid(0).events().localListen(locks, EVT_CACHE_OBJECT_LOCKED);
        grid(0).events().localListen(unlocks, EVT_CACHE_OBJECT_UNLOCKED);

        UUID affinityKey = primaryKeyForCache(grid(0));

        GridCache<UUID, String> cache = grid(0).cache(null);

        UUID key1;
        UUID key2;

        try (IgniteTx tx = cache.txStartPartition(cache.affinity().partition(affinityKey), concurrency,
            READ_COMMITTED, 0, 2)) {
            // Note that events are not generated for internal keys.
            assertEquals("Unexpected number of lock events: " + locks.affectedKeys(), 0, locks.affectedKeys().size());
            assertEquals("Unexpected number of unlock events: " + unlocks.affectedKeys(), 0,
                unlocks.affectedKeys().size());

            GridCacheAdapter<Object, Object> cacheAdapter = ((GridKernal)grid(0)).internalCache();

            GridCacheAffinityManager<Object, Object> affMgr = cacheAdapter.context().affinity();

            GridPartitionLockKey partAffKey = affMgr.partitionAffinityKey(cache.affinity().partition(affinityKey));

            if (concurrency == PESSIMISTIC)
                assertTrue(cacheAdapter.entryEx(partAffKey).lockedByThread());

            do {
                key1 = UUID.randomUUID();
            }
            while (cache.affinity().partition(key1) != cache.affinity().partition(affinityKey));

            do {
                key2 = UUID.randomUUID();
            }
            while (cache.affinity().partition(key2) != cache.affinity().partition(affinityKey));

            cache.putAll(F.asMap(
                key1, "val1",
                key2, "val2")
            );

            tx.commit();
        }

        // Check that there are no further locks after transaction commit.
        assertEquals("Unexpected number of lock events: " + locks.affectedKeys(), 0, locks.affectedKeys().size());
        assertEquals("Unexpected number of unlock events: " + unlocks.affectedKeys(), 0, unlocks.affectedKeys().size());

        for (int i = 0; i < gridCount(); i++) {
            Ignite g = grid(i);

            GridCache<Object, Object> gCache = g.cache(null);

            if (gCache.affinity().isPrimaryOrBackup(g.cluster().localNode(), key1))
                assertEquals("For index: " + i, "val1", gCache.peek(key1));

            if (gCache.affinity().isPrimaryOrBackup(g.cluster().localNode(), key2))
                assertEquals("For index: " + i, "val2", gCache.peek(key2));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetPutOptimisticReadCommitted() throws Exception {
        checkGetPut(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetPutOptimisticRepeatableRead() throws Exception {
        checkGetPut(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetPutPessimisticReadCommitted() throws Exception {
        checkGetPut(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetPutPessimisticRepeatableRead() throws Exception {
        checkGetPut(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetPutEmptyCachePessimisticReadCommitted() throws Exception {
        checkGetPutEmptyCache(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetPutEmptyCachePessimisticRepeatableRead() throws Exception {
        checkGetPutEmptyCache(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetPutEmptyCacheOptimisticReadCommitted() throws Exception {
        checkGetPutEmptyCache(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetPutEmptyCacheOptimisticRepeatableRead() throws Exception {
        checkGetPutEmptyCache(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @param concurrency Transaction concurrency mode.
     * @param isolation Transaction isolation mode.
     * @throws Exception If failed.
     */
    private void checkGetPut(IgniteTxConcurrency concurrency, IgniteTxIsolation isolation) throws Exception {
        CollectingEventListener locks = new CollectingEventListener();
        CollectingEventListener unlocks = new CollectingEventListener();

        UUID affinityKey = primaryKeyForCache(grid(0));

        GridCacheAffinityKey<String> key1 = new GridCacheAffinityKey<>("key1", affinityKey);
        GridCacheAffinityKey<String> key2 = new GridCacheAffinityKey<>("key2", affinityKey);

        GridCache<GridCacheAffinityKey<String>, String> cache = grid(0).cache(null);

        // Populate cache.
        cache.putAll(F.asMap(
            key1, "val1",
            key2, "val2")
        );

        for (int i = 0; i < gridCount(); i++) {
            Ignite g = grid(i);

            GridCache<Object, Object> gCache = g.cache(null);

            if (gCache.affinity().isPrimaryOrBackup(g.cluster().localNode(), key1))
                assertEquals("For index: " + i, "val1", gCache.peek(key1));

            if (gCache.affinity().isPrimaryOrBackup(g.cluster().localNode(), key2))
                assertEquals("For index: " + i, "val2", gCache.peek(key2));
        }

        grid(0).events().localListen(locks, EVT_CACHE_OBJECT_LOCKED);
        grid(0).events().localListen(unlocks, EVT_CACHE_OBJECT_UNLOCKED);

        try (IgniteTx tx = cache.txStartAffinity(affinityKey, concurrency, isolation, 0, 2)) {
            if (concurrency == PESSIMISTIC)
                assertTrue("Failed to wait for lock events: " + affinityKey, locks.awaitKeys(WAIT_TIMEOUT, affinityKey));
            else
                assertEquals("Unexpected number of lock events: " + locks.affectedKeys(), 0, locks.affectedKeys().size());

            assertEquals("Unexpected number of unlock events: " + unlocks.affectedKeys(), 0, unlocks.affectedKeys().size());

            assertEquals("val1", cache.get(key1));

            assertEquals("val2", cache.get(key2));

            cache.put(key1, "val01");

            cache.put(key2, "val02");

            tx.commit();
        }

        // Check that there are no further locks after transaction commit.
        assertEquals("Unexpected number of lock events: " + locks.affectedKeys(), 1, locks.affectedKeys().size());
        assertTrue("Failed to wait for unlock events: " + affinityKey, unlocks.awaitKeys(WAIT_TIMEOUT, affinityKey));
    }

    /**
     * @param concurrency Transaction concurrency mode.
     * @param isolation Transaction isolation mode.
     * @throws Exception If failed.
     */
    private void checkGetPutEmptyCache(IgniteTxConcurrency concurrency, IgniteTxIsolation isolation) throws Exception {
        CollectingEventListener locks = new CollectingEventListener();
        CollectingEventListener unlocks = new CollectingEventListener();

        UUID affinityKey = primaryKeyForCache(grid(0));

        GridCacheAffinityKey<String> key1 = new GridCacheAffinityKey<>("key1", affinityKey);
        GridCacheAffinityKey<String> key2 = new GridCacheAffinityKey<>("key2", affinityKey);

        GridCache<GridCacheAffinityKey<String>, String> cache = grid(0).cache(null);

        grid(0).events().localListen(locks, EVT_CACHE_OBJECT_LOCKED);
        grid(0).events().localListen(unlocks, EVT_CACHE_OBJECT_UNLOCKED);

        try (IgniteTx tx = cache.txStartAffinity(affinityKey, concurrency, isolation, 0, 2)) {
            if (concurrency == PESSIMISTIC)
                assertTrue("Failed to wait for lock events: " + affinityKey, locks.awaitKeys(WAIT_TIMEOUT, affinityKey));
            else
                assertEquals("Unexpected number of lock events: " + locks.affectedKeys(), 0, locks.affectedKeys().size());

            assertEquals("Unexpected number of unlock events: " + unlocks.affectedKeys(), 0, unlocks.affectedKeys().size());

            assertEquals(null, cache.get(key1));

            assertEquals(null, cache.get(key2));

            cache.put(key1, "val01");

            cache.put(key2, "val02");

            tx.commit();
        }

        // Check that there are no further locks after transaction commit.
        assertEquals("Unexpected number of lock events: " + locks.affectedKeys(), 1, locks.affectedKeys().size());
        assertTrue("Failed to wait for unlock events: " + affinityKey, unlocks.awaitKeys(WAIT_TIMEOUT, affinityKey));

        for (int i = 0; i < gridCount(); i++) {
            Ignite g = grid(i);

            GridCache<Object, Object> gCache = g.cache(null);

            if (gCache.affinity().isPrimaryOrBackup(g.cluster().localNode(), key1))
                assertEquals("For index: " + i, "val01", gCache.peek(key1));

            if (gCache.affinity().isPrimaryOrBackup(g.cluster().localNode(), key2))
                assertEquals("For index: " + i, "val02", gCache.peek(key2));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetRemoveOptimisticReadCommitted() throws Exception {
        checkGetRemove(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetRemoveOptimisticRepeatableRead() throws Exception {
        checkGetRemove(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetRemovePessimisticReadCommitted() throws Exception {
        checkGetRemove(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetRemovePessimisticRepeatableRead() throws Exception {
        checkGetRemove(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @param concurrency Transaction concurrency mode.
     * @param isolation Transaction isolation mode.
     * @throws Exception If failed.
     */
    private void checkGetRemove(IgniteTxConcurrency concurrency, IgniteTxIsolation isolation) throws Exception {
        CollectingEventListener locks = new CollectingEventListener();
        CollectingEventListener unlocks = new CollectingEventListener();

        UUID affinityKey = primaryKeyForCache(grid(0));

        GridCacheAffinityKey<String> key1 = new GridCacheAffinityKey<>("key1", affinityKey);
        GridCacheAffinityKey<String> key2 = new GridCacheAffinityKey<>("key2", affinityKey);

        GridCache<GridCacheAffinityKey<String>, String> cache = grid(0).cache(null);

        // Populate cache.
        cache.putAll(F.asMap(
            key1, "val1",
            key2, "val2")
        );

        for (int i = 0; i < gridCount(); i++) {
            Ignite g = grid(i);

            GridCache<Object, Object> gCache = g.cache(null);

            if (gCache.affinity().isPrimaryOrBackup(g.cluster().localNode(), key1))
                assertEquals("For index: " + i, "val1", gCache.peek(key1));

            if (gCache.affinity().isPrimaryOrBackup(g.cluster().localNode(), key2))
                assertEquals("For index: " + i, "val2", gCache.peek(key2));
        }

        grid(0).events().localListen(locks, EVT_CACHE_OBJECT_LOCKED);
        grid(0).events().localListen(unlocks, EVT_CACHE_OBJECT_UNLOCKED);

        try (IgniteTx tx = cache.txStartAffinity(affinityKey, concurrency, isolation, 0, 2)) {
            if (concurrency == PESSIMISTIC)
                assertTrue("Failed to wait for lock events: " + affinityKey, locks.awaitKeys(WAIT_TIMEOUT, affinityKey));
            else
                assertEquals("Unexpected number of lock events: " + locks.affectedKeys(), 0, locks.affectedKeys().size());

            assertEquals("Unexpected number of unlock events: " + unlocks.affectedKeys(), 0, unlocks.affectedKeys().size());

            assertEquals("val1", cache.get(key1));

            assertEquals("val2", cache.get(key2));

            cache.remove(key1);

            cache.remove(key2);

            tx.commit();
        }

        for (int i = 0; i < gridCount(); i++) {
            assertNull("For cache [i=" + i + ", val=" + cache(i).peek(key1) + ']', cache(i).peek(key1));
            assertNull("For cache [i=" + i + ", val=" + cache(i).peek(key2) + ']', cache(i).peek(key2));

            assertTrue("For cache [idx=" + i + ", keySet=" + cache(i).keySet() + ']', cache(i).size() <= 1);
        }

        // Check that there are no further locks after transaction commit.
        assertEquals("Unexpected number of lock events: " + locks.affectedKeys(), 1, locks.affectedKeys().size());
        assertTrue("Failed to wait for unlock events: " + affinityKey, unlocks.awaitKeys(WAIT_TIMEOUT, affinityKey));
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAfterPutOptimistic() throws Exception {
        checkGetAfterPut(OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAfterPut() throws Exception {
        checkGetAfterPut(PESSIMISTIC);
    }

    /**
     * @param concurrency Transaction concurrency mode.
     * @throws Exception If failed.
     */
    private void checkGetAfterPut(IgniteTxConcurrency concurrency) throws Exception {
        CollectingEventListener locks = new CollectingEventListener();
        CollectingEventListener unlocks = new CollectingEventListener();

        UUID affinityKey = primaryKeyForCache(grid(0));

        GridCacheAffinityKey<String> key1 = new GridCacheAffinityKey<>("key1", affinityKey);
        GridCacheAffinityKey<String> key2 = new GridCacheAffinityKey<>("key2", affinityKey);

        GridCache<GridCacheAffinityKey<String>, String> cache = grid(0).cache(null);

        // Populate cache.
        cache.putAll(F.asMap(
            key1, "val1",
            key2, "val2")
        );

        for (int i = 0; i < gridCount(); i++) {
            Ignite g = grid(i);

            GridCache<Object, Object> gCache = g.cache(null);

            if (gCache.affinity().isPrimaryOrBackup(g.cluster().localNode(), key1))
                assertEquals("For index: " + i, "val1", gCache.peek(key1));

            if (gCache.affinity().isPrimaryOrBackup(g.cluster().localNode(), key2))
                assertEquals("For index: " + i, "val2", gCache.peek(key2));
        }

        grid(0).events().localListen(locks, EVT_CACHE_OBJECT_LOCKED);
        grid(0).events().localListen(unlocks, EVT_CACHE_OBJECT_UNLOCKED);

        try (IgniteTx tx = cache.txStartAffinity(affinityKey, concurrency, READ_COMMITTED, 0, 2)) {
            if (concurrency == PESSIMISTIC)
                assertTrue("Failed to wait for lock events: " + affinityKey, locks.awaitKeys(WAIT_TIMEOUT, affinityKey));
            else
                assertEquals("Unexpected number of lock events: " + locks.affectedKeys(), 0, locks.affectedKeys().size());

            assertEquals("Unexpected number of unlock events: " + unlocks.affectedKeys(), 0, unlocks.affectedKeys().size());

            assertEquals("val1", cache.get(key1));

            assertEquals("val2", cache.get(key2));

            cache.put(key1, "val01");

            cache.put(key2, "val02");

            assertEquals("val01", cache.get(key1));

            assertEquals("val02", cache.get(key2));

            tx.commit();
        }

        // Check that there are no further locks after transaction commit.
        assertEquals("Unexpected number of lock events: " + locks.affectedKeys(), 1, locks.affectedKeys().size());
        assertTrue("Failed to wait for unlock events: " + affinityKey, unlocks.awaitKeys(WAIT_TIMEOUT, affinityKey));

        assertEquals("val01", cache.get(key1));

        assertEquals("val02", cache.get(key2));
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetRepeatableReadOptimistic() throws Exception {
        checkGetRepeatableRead(OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetRepeatableReadPessimistic() throws Exception {
        checkGetRepeatableRead(PESSIMISTIC);
    }

    /**
     * @param concurrency Transaction concurrency mode.
     * @throws Exception If failed.
     */
    private void checkGetRepeatableRead(IgniteTxConcurrency concurrency) throws Exception {
        UUID key = primaryKeyForCache(grid(0));

        cache(0).put(key, "val");

        try (IgniteTx ignored = cache(0).txStartPartition(cache(0).affinity().partition(key), concurrency,
            REPEATABLE_READ, 0, 1)) {
            assertEquals("val", cache(0).get(key));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGroupLockPutWrongKeyOptimistic() throws Exception {
        checkGroupLockPutWrongKey(OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGroupLockPutWrongKeyPessimistic() throws Exception {
        checkGroupLockPutWrongKey(PESSIMISTIC);
    }

    /**
     * @param concurrency Transaction concurrency mode.
     * @throws Exception If failed.
     */
    private void checkGroupLockPutWrongKey(IgniteTxConcurrency concurrency) throws Exception {
        UUID affinityKey = primaryKeyForCache(grid(0));

        final GridCache<GridCacheAffinityKey<String>, String> cache = grid(0).cache(null);

        try (IgniteTx ignored = cache.txStartAffinity(affinityKey, concurrency, READ_COMMITTED, 0, 1)) {
            // Key with affinity key different from enlisted on tx start should raise exception.
            cache.put(new GridCacheAffinityKey<>("key1", UUID.randomUUID()), "val1");

            fail("Exception should be thrown");
        }
        catch (IgniteCheckedException ignored) {
            // Expected exception.
        }

        assertNull(cache.tx());
    }

    /**
     * @throws Exception If failed.
     */
    public void testGroupLockRemoveWrongKeyOptimistic() throws Exception {
        checkGroupLockRemoveWrongKey(OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGroupLockRemoveWrongKeyPessimistic() throws Exception {
        checkGroupLockRemoveWrongKey(PESSIMISTIC);
    }

    /**
     * @param concurrency Transaction concurrency mode.
     * @throws Exception If failed.
     */
    private void checkGroupLockRemoveWrongKey(IgniteTxConcurrency concurrency) throws Exception {
        UUID affinityKey = primaryKeyForCache(grid(0));

        final GridCache<GridCacheAffinityKey<String>, String> cache = grid(0).cache(null);

        final GridCacheAffinityKey<String> key = new GridCacheAffinityKey<>("key1", UUID.randomUUID());

        cache.put(key, "val");

        try (IgniteTx ignored = cache.txStartAffinity(affinityKey, concurrency, READ_COMMITTED, 0, 1)) {
            // Key with affinity key different from enlisted on tx start should raise exception.
            cache.remove(key);

            fail("Exception should be thrown.");
        }
        catch (IgniteCheckedException ignored) {
            // Expected exception.
        }

        assertNull(cache.tx());
    }

    /**
     * @throws Exception If failed.
     */
    public void testGroupLockReadAffinityKeyPessimitsticRepeatableRead() throws Exception {
        checkGroupLockReadAffinityKey(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGroupLockReadAffinityKeyPessimitsticReadCommitted() throws Exception {
        checkGroupLockReadAffinityKey(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGroupLockReadAffinityKeyOptimisticRepeatableRead() throws Exception {
        checkGroupLockReadAffinityKey(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGroupLockReadAffinityKeyOptimisticReadCommitted() throws Exception {
        checkGroupLockReadAffinityKey(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @throws Exception If failed.
     */
    private void checkGroupLockReadAffinityKey(IgniteTxConcurrency concurrency, IgniteTxIsolation isolation)
        throws Exception {
        UUID affinityKey = primaryKeyForCache(grid(0));

        final GridCache<Object, String> cache = grid(0).cache(null);

        final GridCacheAffinityKey<String> key1 = new GridCacheAffinityKey<>("key1", affinityKey);
        final GridCacheAffinityKey<String> key2 = new GridCacheAffinityKey<>("key2", affinityKey);

        cache.put(affinityKey, "0");
        cache.put(key1, "0");
        cache.put(key2, "0");

        try (IgniteTx tx = cache.txStartAffinity(affinityKey, concurrency, isolation, 0, 3)) {
            assertEquals("0", cache.get(affinityKey));
            assertEquals("0", cache.get(key1));
            assertEquals("0", cache.get(key2));

            cache.put(affinityKey, "1");
            cache.put(key1, "1");
            cache.put(key2, "1");

            assertEquals("1", cache.get(affinityKey));
            assertEquals("1", cache.get(key1));
            assertEquals("1", cache.get(key2));

            tx.commit();
        }

        assertEquals("1", cache.get(affinityKey));
        assertEquals("1", cache.get(key1));
        assertEquals("1", cache.get(key2));
    }

    /**
     * @throws Exception If failed.
     */
    public void testGroupLockWriteThroughBatchUpdateOptimistic() throws Exception {
        checkGroupLockWriteThrough(OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGroupLockWriteThroughBatchUpdatePessimistic() throws Exception {
        checkGroupLockWriteThrough(PESSIMISTIC);
    }

    /**
     * @param concurrency Transaction concurrency mode.
     * @throws Exception If failed.
     */
    private void checkGroupLockWriteThrough(IgniteTxConcurrency concurrency) throws Exception {
        UUID affinityKey = primaryKeyForCache(grid(0));

        GridCache<GridCacheAffinityKey<String>, String> cache = grid(0).cache(null);

        GridCacheAffinityKey<String> key1 = new GridCacheAffinityKey<>("key1", affinityKey);
        GridCacheAffinityKey<String> key2 = new GridCacheAffinityKey<>("key2", affinityKey);
        GridCacheAffinityKey<String> key3 = new GridCacheAffinityKey<>("key3", affinityKey);
        GridCacheAffinityKey<String> key4 = new GridCacheAffinityKey<>("key4", affinityKey);

        Map<GridCacheAffinityKey<String>, String> putMap = F.asMap(
            key1, "val1",
            key2, "val2",
            key3, "val3",
            key4, "val4");

        try (IgniteTx tx = cache.txStartAffinity(affinityKey, concurrency, READ_COMMITTED, 0, 4)) {
            cache.put(key1, "val1");
            cache.put(key2, "val2");
            cache.put(key3, "val3");
            cache.put(key4, "val4");

            tx.commit();
        }

        for (int i = 0; i < gridCount(); i++) {
            Ignite g = grid(i);

            GridCache<Object, Object> gCache = g.cache(null);

            if (gCache.affinity().isPrimaryOrBackup(g.cluster().localNode(), key1))
                assertEquals("For index: " + i, "val1", gCache.peek(key1));

            if (gCache.affinity().isPrimaryOrBackup(g.cluster().localNode(), key2))
                assertEquals("For index: " + i, "val2", gCache.peek(key2));

            if (gCache.affinity().isPrimaryOrBackup(g.cluster().localNode(), key3))
                assertEquals("For index: " + i, "val3", gCache.peek(key3));

            if (gCache.affinity().isPrimaryOrBackup(g.cluster().localNode(), key4))
                assertEquals("For index: " + i, "val4", gCache.peek(key4));
        }

        // Check the store.
        assertTrue(store.storeMap().equals(putMap));
        assertEquals(1, store.putCount());
    }

    /** @return {@code True} if sanity check should be enabled. */
    private boolean sanityCheckEnabled() {
        return !getName().contains("SanityCheckDisabled");
    }

    /**
     * @param primary Primary node for which key should be calculated.
     * @return Key for which given node is primary.
     * @throws IgniteCheckedException If affinity can not be calculated.
     */
    protected UUID primaryKeyForCache(Ignite primary) throws IgniteCheckedException {
        UUID res;

        int cnt = 0;

        UUID primaryId = primary.cluster().localNode().id();

        do {
            res = UUID.randomUUID();

            cnt++;

            if (cnt > 10000)
                throw new IllegalStateException("Cannot find key for primary node: " + primaryId);
        }
        while (!primary.cluster().mapKeyToNode(null, res).id().equals(primaryId));

        return res;
    }

    /**
     * @param primary Primary node for which keys should be calculated.
     * @param cnt Key count.
     * @return Collection of generated keys.
     * @throws IgniteCheckedException If affinity can not be calculated.
     */
    protected UUID[] primaryKeysForCache(Ignite primary, int cnt) throws IgniteCheckedException {
        Collection<UUID> keys = new LinkedHashSet<>();

        int iters = 0;

        do {
            keys.add(primaryKeyForCache(primary));

            iters++;

            if (iters > 10000)
                throw new IllegalStateException("Cannot find keys for primary node [nodeId=" +
                    primary.cluster().localNode().id() + ", cnt=" + cnt + ']');
        }
        while (keys.size() < cnt);

        UUID[] res = new UUID[keys.size()];

        return keys.toArray(res);
    }

    /** Event listener that collects all incoming events. */
    protected static class CollectingEventListener implements IgnitePredicate<IgniteEvent> {
        /** Collected events. */
        private final Collection<Object> affectedKeys = new GridConcurrentLinkedHashSet<>();

        /** {@inheritDoc} */
        @Override public boolean apply(IgniteEvent evt) {
            assert evt.type() == EVT_CACHE_OBJECT_LOCKED || evt.type() == EVT_CACHE_OBJECT_UNLOCKED;

            IgniteCacheEvent cacheEvt = (IgniteCacheEvent)evt;

            synchronized (this) {
                affectedKeys.add(cacheEvt.key());

                notifyAll();
            }

            return true;
        }

        /** @return Collection of affected keys. */
        public Collection<Object> affectedKeys() {
            return affectedKeys;
        }

        /**
         * Waits until events received for all supplied keys.
         *
         * @param timeout Timeout to wait.
         * @param keys Keys to wait for.
         * @return {@code True} if wait was successful, {@code false} if wait timed out.
         * @throws InterruptedException If thread was interrupted.
         */
        public boolean awaitKeys(long timeout, Object... keys) throws InterruptedException {
            long start = System.currentTimeMillis();

            Collection<Object> keysCol = Arrays.asList(keys);

            synchronized (this) {
                while (true) {
                    long now = System.currentTimeMillis();

                    if (affectedKeys.containsAll(keysCol))
                        return true;
                    else if (start + timeout > now)
                        wait(start + timeout - now);
                    else
                        return false;
                }
            }
        }
    }

    /** Test store that accumulates values into map. */
    private static class TestStore extends CacheStoreAdapter<Object, Object> {
        /** */
        private ConcurrentMap<Object, Object> storeMap = new ConcurrentHashMap8<>();

        /** */
        private AtomicInteger putCnt = new AtomicInteger();

        /** {@inheritDoc} */
        @Override public Object load(Object key) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void writeAll(Collection<Cache.Entry<?, ?>> entries) {
            for (Cache.Entry<?, ?> e : entries)
                storeMap.put(e.getKey(), e.getValue());

            putCnt.incrementAndGet();
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<?, ?> e) {
            storeMap.put(e.getKey(), e.getValue());

            putCnt.incrementAndGet();
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            storeMap.remove(key);
        }

        /** @return Stored values map. */
        public ConcurrentMap<Object, Object> storeMap() {
            return storeMap;
        }

        /** @return Number of calls to put(). */
        public int putCount() {
            return putCnt.get();
        }
    }
}
