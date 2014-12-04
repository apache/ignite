/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.events.IgniteEventType.*;
import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;

/**
 * Test for group locking.
 */
public abstract class GridCacheGroupLockAbstractSelfTest extends GridCommonAbstractTest {
    /** */
    private static final GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

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
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(cacheMode());
        cacheCfg.setDistributionMode(nearEnabled() ? NEAR_PARTITIONED : PARTITIONED_ONLY);
        cacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        boolean txBatchUpdate = batchUpdate();

        cacheCfg.setStore(store);

        cfg.setCacheConfiguration(cacheCfg);
        cfg.setCacheSanityCheckEnabled(sanityCheckEnabled());

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

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
     * @throws Exception If failed.
     */
    private void checkGroupLockPutOneKey(GridCacheTxConcurrency concurrency) throws Exception {
        CollectingEventListener locks = new CollectingEventListener();
        CollectingEventListener unlocks = new CollectingEventListener();

        grid(0).events().localListen(locks, EVT_CACHE_OBJECT_LOCKED);
        grid(0).events().localListen(unlocks, EVT_CACHE_OBJECT_UNLOCKED);

        UUID affinityKey = primaryKeyForCache(grid(0));

        GridCache<GridCacheAffinityKey<String>, String> cache = grid(0).cache(null);

        GridCacheAffinityKey<String> key1;
        GridCacheAffinityKey<String> key2;

        try (GridCacheTx tx = cache.txStartAffinity(affinityKey, concurrency, READ_COMMITTED, 0, 2)) {
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
     * @throws Exception If failed.
     */
    private void checkGroupLockRemoveOneKey(GridCacheTxConcurrency concurrency) throws Exception {
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

        try (GridCacheTx tx = cache.txStartAffinity(affinityKey, concurrency, READ_COMMITTED, 0, 2)) {
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
     * @throws Exception If failed.
     */
    private void checkGroupLockGetOneKey(GridCacheTxConcurrency concurrency) throws Exception {
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

        try (GridCacheTx tx = cache.txStartAffinity(affinityKey, concurrency, READ_COMMITTED, 0, 2)) {
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

    /** @throws GridException */
    public void testGroupLockWithExternalLockOptimistic() throws Exception {
        checkGroupLockWithExternalLock(OPTIMISTIC);
    }

    /** @throws GridException */
    public void testGroupLockWithExternalLockPessimistic() throws Exception {
        checkGroupLockWithExternalLock(PESSIMISTIC);
    }

    /** @throws GridException */
    private void checkGroupLockWithExternalLock(final GridCacheTxConcurrency concurrency) throws Exception {
        assert sanityCheckEnabled();

        final UUID affinityKey = primaryKeyForCache(grid(0));

        final GridCacheAffinityKey<String> key1 = new GridCacheAffinityKey<>("key1", affinityKey);

        final GridCache<GridCacheAffinityKey<String>, String> cache = grid(0).cache(null);

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
                    assertTrue(cache.lock(key1, 0));

                    try {
                        lockLatch.countDown();
                        unlockLatch.await();
                    }
                    finally {
                        cache.unlock(key1);
                    }
                }
                catch (GridException e) {
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
                    try (GridCacheTx tx = cache.txStartAffinity(affinityKey, concurrency, READ_COMMITTED, 0, 1)) {
                        cache.put(key1, "val01");

                        tx.commit();
                    }

                    return null;
                }
            }, GridCacheTxHeuristicException.class, null);
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
     * @throws Exception If failed.
     */
    private void checkSanityCheckDisabled(final GridCacheTxConcurrency concurrency) throws Exception {
        assert !sanityCheckEnabled();

        final UUID affinityKey = primaryKeyForCache(grid(0));

        final GridCacheAffinityKey<String> key1 = new GridCacheAffinityKey<>("key1", affinityKey);

        final GridCache<GridCacheAffinityKey<String>, String> cache = grid(0).cache(null);

        // Populate cache.
        cache.put(key1, "val1");

        for (int i = 0; i < gridCount(); i++) {
            Ignite g = grid(i);

            GridCache<Object, Object> gCache = g.cache(null);

            if (gCache.affinity().isPrimaryOrBackup(g.cluster().localNode(), key1))
                assertEquals("For index: " + i, "val1", gCache.peek(key1));
        }

        assertTrue(cache.lock(key1, 0));

        try {
            try (GridCacheTx tx = cache.txStartAffinity(affinityKey, concurrency, READ_COMMITTED, 0, 1)) {
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
            cache.unlock(key1);
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
     * @throws Exception If failed.
     */
    private void checkGroupPartitionLock(GridCacheTxConcurrency concurrency) throws Exception {
        CollectingEventListener locks = new CollectingEventListener();
        CollectingEventListener unlocks = new CollectingEventListener();

        grid(0).events().localListen(locks, EVT_CACHE_OBJECT_LOCKED);
        grid(0).events().localListen(unlocks, EVT_CACHE_OBJECT_UNLOCKED);

        UUID affinityKey = primaryKeyForCache(grid(0));

        GridCache<UUID, String> cache = grid(0).cache(null);

        UUID key1;
        UUID key2;

        try (GridCacheTx tx = cache.txStartPartition(cache.affinity().partition(affinityKey), concurrency,
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
     * @throws Exception If failed.
     */
    private void checkGetPut(GridCacheTxConcurrency concurrency, GridCacheTxIsolation isolation) throws Exception {
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

        try (GridCacheTx tx = cache.txStartAffinity(affinityKey, concurrency, isolation, 0, 2)) {
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
     * @throws Exception If failed.
     */
    private void checkGetPutEmptyCache(GridCacheTxConcurrency concurrency, GridCacheTxIsolation isolation) throws Exception {
        CollectingEventListener locks = new CollectingEventListener();
        CollectingEventListener unlocks = new CollectingEventListener();

        UUID affinityKey = primaryKeyForCache(grid(0));

        GridCacheAffinityKey<String> key1 = new GridCacheAffinityKey<>("key1", affinityKey);
        GridCacheAffinityKey<String> key2 = new GridCacheAffinityKey<>("key2", affinityKey);

        GridCache<GridCacheAffinityKey<String>, String> cache = grid(0).cache(null);

        grid(0).events().localListen(locks, EVT_CACHE_OBJECT_LOCKED);
        grid(0).events().localListen(unlocks, EVT_CACHE_OBJECT_UNLOCKED);

        try (GridCacheTx tx = cache.txStartAffinity(affinityKey, concurrency, isolation, 0, 2)) {
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
     * @throws Exception If failed.
     */
    private void checkGetRemove(GridCacheTxConcurrency concurrency, GridCacheTxIsolation isolation) throws Exception {
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

        try (GridCacheTx tx = cache.txStartAffinity(affinityKey, concurrency, isolation, 0, 2)) {
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
            assertNull("For cache: " + i, cache(i).peek("val1"));
            assertNull("For cache: " + i, cache(i).peek("val2"));

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
     * @throws Exception If failed.
     */
    private void checkGetAfterPut(GridCacheTxConcurrency concurrency) throws Exception {
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

        try (GridCacheTx tx = cache.txStartAffinity(affinityKey, concurrency, READ_COMMITTED, 0, 2)) {
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
     * @throws Exception If failed.
     */
    private void checkGetRepeatableRead(GridCacheTxConcurrency concurrency) throws Exception {
        UUID key = primaryKeyForCache(grid(0));

        cache(0).put(key, "val");

        try (GridCacheTx tx = cache(0).txStartPartition(cache(0).affinity().partition(key), concurrency,
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
     * @throws Exception If failed.
     */
    private void checkGroupLockPutWrongKey(GridCacheTxConcurrency concurrency) throws Exception {
        UUID affinityKey = primaryKeyForCache(grid(0));

        final GridCache<GridCacheAffinityKey<String>, String> cache = grid(0).cache(null);

        try (GridCacheTx tx = cache.txStartAffinity(affinityKey, concurrency, READ_COMMITTED, 0, 1)) {
            // Key with affinity key different from enlisted on tx start should raise exception.
            cache.put(new GridCacheAffinityKey<>("key1", UUID.randomUUID()), "val1");

            fail("Exception should be thrown");
        }
        catch (GridException ignored) {
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
     * @throws Exception If failed.
     */
    private void checkGroupLockRemoveWrongKey(GridCacheTxConcurrency concurrency) throws Exception {
        UUID affinityKey = primaryKeyForCache(grid(0));

        final GridCache<GridCacheAffinityKey<String>, String> cache = grid(0).cache(null);

        final GridCacheAffinityKey<String> key = new GridCacheAffinityKey<>("key1", UUID.randomUUID());

        cache.put(key, "val");

        try (GridCacheTx tx = cache.txStartAffinity(affinityKey, concurrency, READ_COMMITTED, 0, 1)) {
            // Key with affinity key different from enlisted on tx start should raise exception.
            cache.remove(key);

            fail("Exception should be thrown.");
        }
        catch (GridException ignored) {
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
    private void checkGroupLockReadAffinityKey(GridCacheTxConcurrency concurrency, GridCacheTxIsolation isolation)
        throws Exception {
        UUID affinityKey = primaryKeyForCache(grid(0));

        final GridCache<Object, String> cache = grid(0).cache(null);

        final GridCacheAffinityKey<String> key1 = new GridCacheAffinityKey<>("key1", affinityKey);
        final GridCacheAffinityKey<String> key2 = new GridCacheAffinityKey<>("key2", affinityKey);

        cache.put(affinityKey, "0");
        cache.put(key1, "0");
        cache.put(key2, "0");

        try (GridCacheTx tx = cache.txStartAffinity(affinityKey, concurrency, isolation, 0, 3)) {
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
        // Configuration changed according to test name.
        assert batchUpdate();

        checkGroupLockWriteThrough(OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGroupLockWriteThroughBatchUpdatePessimistic() throws Exception {
        // Configuration changed according to test name.
        assert batchUpdate();

        checkGroupLockWriteThrough(PESSIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGroupLockWriteThroughSingleUpdateOptimistic() throws Exception {
        // Configuration changed according to test name.
        checkGroupLockWriteThrough(OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGroupLockWriteThroughSingleUpdatePessimistic() throws Exception {
        // Configuration changed according to test name.
        checkGroupLockWriteThrough(PESSIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    private void checkGroupLockWriteThrough(GridCacheTxConcurrency concurrency) throws Exception {
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

        try (GridCacheTx tx = cache.txStartAffinity(affinityKey, concurrency, READ_COMMITTED, 0, 4)) {
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
        assertEquals(batchUpdate() ? 1 : 4, store.putCount());
    }

    /** @return {@code True} if batch update should be enabled. */
    private boolean batchUpdate() {
        return getName().contains("testGroupLockWriteThroughBatchUpdate");
    }

    /** @return {@code True} if sanity check should be enabled. */
    private boolean sanityCheckEnabled() {
        return !getName().contains("SanityCheckDisabled");
    }

    /**
     * @param primary Primary node for which key should be calculated.
     * @return Key for which given node is primary.
     * @throws GridException If affinity can not be calculated.
     */
    protected UUID primaryKeyForCache(Ignite primary) throws GridException {
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
     * @throws GridException If affinity can not be calculated.
     */
    protected UUID[] primaryKeysForCache(Ignite primary, int cnt) throws GridException {
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
    private static class TestStore extends GridCacheStoreAdapter<Object, Object> {
        /** */
        private ConcurrentMap<Object, Object> storeMap = new ConcurrentHashMap8<>();

        /** */
        private AtomicInteger putCnt = new AtomicInteger();

        /** {@inheritDoc} */
        @Override public Object load(@Nullable GridCacheTx tx, Object key)
            throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void putAll(GridCacheTx tx,
            Map<?, ?> map) throws GridException {
            storeMap.putAll(map);

            putCnt.incrementAndGet();
        }

        /** {@inheritDoc} */
        @Override public void put(@Nullable GridCacheTx tx, Object key,
            @Nullable Object val) throws GridException {
            storeMap.put(key, val);

            putCnt.incrementAndGet();
        }

        /** {@inheritDoc} */
        @Override public void remove(@Nullable GridCacheTx tx, Object key)
            throws GridException {
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
