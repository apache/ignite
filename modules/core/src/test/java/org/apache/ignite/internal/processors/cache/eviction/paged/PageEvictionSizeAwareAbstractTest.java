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

package org.apache.ignite.internal.processors.cache.eviction.paged;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.freelist.AbstractFreeList;
import org.apache.ignite.internal.processors.cache.persistence.freelist.FreeList;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/** Tests size-aware page eviction on in-memory (non-persistent) data regions. */
public abstract class PageEvictionSizeAwareAbstractTest extends PageEvictionAbstractTest {
    /** Name of the rebalance supplier node, which has a twice larger data region. */
    private static final String SUPPLIER = "supplier";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (SUPPLIER.equals(gridName)) {
            DataRegionConfiguration regCfg = cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration();

            regCfg.setMaxSize(regCfg.getMaxSize() * 2);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Rebalancing inserts the received rows in batches reserved as a whole: the demander, which receives more data than
     * fits below its threshold, evicts at the threshold and releases the reservations.
     */
    @Test(timeout = 300_000)
    public void testRebalanceHonorsThreshold() throws Exception {
        IgniteEx supplier = startGrid(SUPPLIER);

        IgniteCache<Integer, Object> cache = supplier.createCache(new CacheConfiguration<Integer, Object>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setBackups(1));

        byte[] val = new byte[pageSize(supplier) - 200];

        // One page per entry: 1.5 times the demander region.
        int keys = (int)(totalPages(supplier) * 0.75);

        for (int base = 0; base < keys; base += 100) {
            Map<Integer, Object> batch = new HashMap<>();

            for (int i = base; i < Math.min(base + 100, keys); i++)
                batch.put(i, val);

            cache.putAll(batch);
        }

        IgniteEx demander = startGrid(1);

        awaitPartitionMapExchange();

        assertHonorsThreshold(demander);

        assertEquals(0, ((AtomicLong)GridTestUtils.getFieldValue(defaultFreeList(demander), "reservedPages")).get());
    }

    /** */
    @Test
    public void testRecordLargerThanRegionOom() throws Exception {
        IgniteEx ignite = startGrid(1);

        IgniteCache<Integer, Object> cache = createCache(ignite, DEFAULT_CACHE_NAME);

        long regionSize = regionMaxSize(ignite);

        GridTestUtils.assertThrowsWithCause(
            () -> cache.put(1, new byte[(int)(regionSize * 2)]),
            IgniteOutOfMemoryException.class
        );

        GridTestUtils.assertThrowsWithCause(
            () -> {
                Map<Integer, Object> batch = new HashMap<>();

                Object val = new byte[(int)(regionSize / 4)];

                for (int i = 0; i < 4; i++)
                    batch.put(i, val);

                cache.putAll(batch);
            },
            IgniteOutOfMemoryException.class
        );
    }

    /** */
    @Test
    public void testPutAllLargeRows() throws Exception {
        IgniteEx ignite = startGrid(1);

        IgniteCache<Integer, Object> cache = createCache(ignite, DEFAULT_CACHE_NAME);

        int smallEntries = (int)(totalPages(ignite) * 0.85);

        byte[] small = new byte[pageSize(ignite) - 200];

        for (int i = 0; i < smallEntries; i++)
            cache.put(smallEntries + i, small);

        assertTrue("Pre-fill must load >80% of pages", loadedPages(ignite) > totalPages(ignite) * 0.80);
        assertFalse("Eviction must not start during pre-fill", isEvictionsStarted(ignite));

        int putAllLargeRows = 3;
        Map<Integer, Object> large = new HashMap<>();

        Object val = new byte[(int)(regionMaxSize(ignite) / 4)];

        for (int i = 0; i < putAllLargeRows; i++)
            large.put(i, val);

        cache.putAll(large);

        assertTrue("Eviction must start after putAll of large rows near capacity", isEvictionsStarted(ignite));

        for (int i = 0; i < putAllLargeRows; i++)
            assertNotNull("Large row " + i + " must be readable after putAll", cache.get(i));
    }

    /** */
    @Test
    public void testUpdateRowGrows() throws Exception {
        IgniteEx ignite = startGrid(1);

        IgniteCache<Integer, Object> cache = createCache(ignite, DEFAULT_CACHE_NAME);

        int smallEntries = (int)(totalPages(ignite) * 0.85);
        byte[] small = new byte[pageSize(ignite) - 200];

        for (int i = 0; i < smallEntries; i++)
            cache.put(smallEntries + i, small);

        cache.put(1, new byte[1024]);

        byte[] big = new byte[(int)(regionMaxSize(ignite) / 4)];

        Arrays.fill(big, (byte)7);

        cache.put(1, big);

        assertTrue("Eviction must have started after growing a row near capacity", isEvictionsStarted(ignite));

        byte[] read = (byte[])cache.get(1);

        assertNotNull("Updated large value must be readable", read);
        assertTrue("Updated value must equal the stored value", Arrays.equals(big, read));
    }

    /** */
    @Test
    public void testLargeRowBelowThresholdUsesHeadroomNoEviction() throws Exception {
        IgniteEx ignite = startGrid(1);

        IgniteCache<Integer, Object> cache = createCache(ignite, DEFAULT_CACHE_NAME);

        int preFill = fillRegion(ignite, cache, 0.4);

        long loadedPages = loadedPages(ignite);
        long totalPages = totalPages(ignite);

        assertTrue("Pre-fill must load ~40% of pages", loadedPages > totalPages * 0.30 && loadedPages < totalPages * 0.50);
        assertFalse("Eviction must not have started during pre-fill below threshold", isEvictionsStarted(ignite));

        byte[] val = new byte[(int)(regionMaxSize(ignite) * 0.3)];

        Arrays.fill(val, (byte)1);

        cache.put(preFill, val);

        assertFalse("Eviction must not start when the region stays below the threshold", isEvictionsStarted(ignite));

        for (int i = 0; i < preFill; i++)
            assertNotNull("Pre-filled entry " + i + " must not be evicted below threshold", cache.get(i));

        byte[] read = (byte[])cache.get(preFill);

        assertNotNull("Large row must be readable", read);
        assertTrue("Value read back must equal the stored value", Arrays.equals(val, read));
    }

    /** */
    @Test
    public void testLargeRowAboveThresholdEvictsAndSucceeds() throws Exception {
        IgniteEx ignite = startGrid(1);

        IgniteCache<Integer, Object> cache = createCache(ignite, DEFAULT_CACHE_NAME);

        int smallEntries = fillRegion(ignite, cache, 0.85);

        assertTrue("Pre-fill must load >80% of pages", loadedPages(ignite) > totalPages(ignite) * 0.80);

        byte[] val = new byte[(int)(regionMaxSize(ignite) / 4)];

        Arrays.fill(val, (byte)1);

        int largeKey = smallEntries + 1;

        cache.put(largeKey, val);

        assertTrue("Eviction must have started after a large put near capacity", isEvictionsStarted(ignite));

        byte[] read = (byte[])cache.get(largeKey);

        assertNotNull("Large row must be readable after eviction", read);
        assertTrue("Value read back must equal the stored value", Arrays.equals(val, read));
    }

    /** Atomic cache with a backup: primary batch and backup updates must evict at the same threshold. */
    @Test(timeout = 300_000)
    public void testAtomicBackupHonorsThreshold() throws Exception {
        IgniteEx ignite = startGrids(2);

        IgniteCache<Integer, Object> cache = ignite.createCache(new CacheConfiguration<Integer, Object>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setBackups(1)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC));

        awaitPartitionMapExchange();

        byte[] val = new byte[pageSize(ignite) - 200];

        int keys = (int)(totalPages(ignite) * 3);

        for (int base = 0; base < keys; base += 100) {
            Map<Integer, Object> batch = new HashMap<>();

            for (int i = base; i < base + 100; i++)
                batch.put(i, val);

            cache.putAll(batch);
        }

        assertHonorsThreshold(grid(0));
        assertHonorsThreshold(grid(1));
    }

    /** Transactional cache: commits must evict at the threshold. */
    @Test(timeout = 300_000)
    public void testTransactionalHonorsThreshold() throws Exception {
        IgniteEx ignite = startGrid(0);

        IgniteCache<Integer, Object> cache = createCache(ignite, DEFAULT_CACHE_NAME, 0, true);

        byte[] val = new byte[pageSize(ignite) - 200];

        int keys = (int)(totalPages(ignite) * 3);

        for (int i = 0; i < keys; i++)
            cache.put(i, val);

        assertHonorsThreshold(ignite);
    }

    /** A row that does not fit below the threshold together with the pool may use the space above it. */
    @Test
    public void testRowLargerThanThresholdFits() throws Exception {
        IgniteEx ignite = startGrid(0);

        IgniteCache<Integer, Object> cache = createCache(ignite, DEFAULT_CACHE_NAME);

        fillRegion(ignite, cache, 0.5);

        int largeValSize = (int)(regionMaxSize(ignite) * EVICTION_THRESHOLD);

        cache.put(-1, new byte[largeValSize]);

        assertEquals(largeValSize, ((byte[])cache.get(-1)).length);
    }

    /**
     * When no page can be evicted (all resident entries are locked), the eviction progress guard refuses an insert that
     * can be refused (a primary atomic update) with an out-of-memory, while a best-effort reservation (backup update,
     * transaction commit, rebalancing) gives up without an error. Once the stall is detected, the already waiting and
     * the following best-effort reservations give up without waiting for the timeout, until the eviction makes progress
     * again.
     */
    @Test(timeout = 180_000)
    public void testGuardWhenAllEntriesLocked() throws Exception {
        IgniteEx ignite = startGrid(1);

        IgniteCache<Integer, Object> txCache = createCache(ignite, "tx-cache", 0, true);
        IgniteCache<Integer, Object> atomicCache = createCache(ignite, DEFAULT_CACHE_NAME);

        int fillEntries = fillRegion(ignite, txCache, 0.85);

        Collection<Integer> keys = new ArrayList<>(fillEntries);

        for (int i = 0; i < fillEntries; i++)
            keys.add(i);

        CountDownLatch ready = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        IgniteInternalFuture<?> lockerFut = GridTestUtils.runAsync(() -> {
            Lock lock = txCache.lockAll(keys);

            lock.lock();

            try {
                ready.countDown();

                release.await();
            }
            finally {
                lock.unlock();
            }
        }, "size-aware-guard-locker");

        try {
            assertTrue("Timed out waiting for entries to be locked", ready.await(60, TimeUnit.SECONDS));

            int largeValSize = 4 * 1024 * 1024;

            GridTestUtils.assertThrowsWithCause(
                () -> atomicCache.put(0, new byte[largeValSize]),
                IgniteOutOfMemoryException.class
            );

            IgniteCacheDatabaseSharedManager db = ignite.context().cache().context().database();

            DataRegion region = defaultRegion(ignite);

            FreeList<?> freeList = db.freeList(region.config().getName());

            long timeout = AbstractFreeList.DFLT_EVICTION_NO_PROGRESS_TIMEOUT_MILLIS;

            assertFalse(GridTestUtils.<Boolean>getFieldValue(freeList, "stalled"));

            IgniteInternalFuture<Long> firstFut = GridTestUtils.runAsync(() -> db.reserveInRegion(region, largeValSize, true),
                "size-aware-guard-first");

            U.sleep(timeout / 2);

            // Starts waiting before the stall is detected and must be released once the first reservation detects it.
            long start = System.currentTimeMillis();

            assertEquals(0, db.reserveInRegion(region, largeValSize, true));

            assertTrue("Waiting best-effort reservation must give up once the stall is detected",
                System.currentTimeMillis() - start < timeout * 3 / 4);

            assertEquals(0, (long)firstFut.get(timeout, TimeUnit.MILLISECONDS));

            assertTrue(GridTestUtils.<Boolean>getFieldValue(freeList, "stalled"));

            // The stall is not a time window: once the timeout passes, best-effort reservations still do not wait.
            U.sleep(timeout + 500);

            start = System.currentTimeMillis();

            assertEquals(0, db.reserveInRegion(region, largeValSize, true));

            assertTrue("Best-effort reservation must not wait for the timeout while the eviction is stalled",
                System.currentTimeMillis() - start < timeout / 2);

            release.countDown();

            lockerFut.get(10, TimeUnit.SECONDS);

            // The unlocked entries can be evicted again: the reservation succeeds and clears the stall. While stalled, a
            // best-effort reservation gives up after a single eviction attempt, which may free no page, so it is retried.
            long reserved = 0;

            long deadline = System.currentTimeMillis() + timeout;

            while (reserved == 0 && System.currentTimeMillis() < deadline)
                reserved = db.reserveInRegion(region, largeValSize, true);

            try {
                assertTrue("Reservation must succeed once the entries are unlocked", reserved > 0);

                assertFalse(GridTestUtils.<Boolean>getFieldValue(freeList, "stalled"));
            }
            finally {
                db.releaseInRegion(region, reserved);
            }
        }
        finally {
            release.countDown();

            lockerFut.get(10, TimeUnit.SECONDS);
        }
    }

    /**
     * @param ignite Node.
     * @return Free list of the default data region.
     */
    private static FreeList<?> defaultFreeList(IgniteEx ignite) throws IgniteCheckedException {
        return ignite.context().cache().context().database().freeList(defaultRegion(ignite).config().getName());
    }

    /**
     * Asserts that the node evicted at the threshold: loaded pages reached it but do not exceed it by more than the
     * empty pages pool and 1% of non-data pages (B+tree, free list metadata).
     *
     * @param ignite Node.
     */
    private static void assertHonorsThreshold(IgniteEx ignite) throws IgniteCheckedException {
        assertTrue("Evictions must have started", isEvictionsStarted(ignite));

        long total = totalPages(ignite);
        long loaded = loadedPages(ignite);
        long limit = (long)(EVICTION_THRESHOLD * total) + defaultRegion(ignite).config().getEmptyPagesPoolSize() + total / 100;

        assertTrue("Loaded pages must reach the threshold [loaded=" + loaded + ", total=" + total + ']',
            loaded >= total * 0.85);
        assertTrue("Loaded pages exceed the threshold [node=" + ignite.name() + ", loaded=" + loaded +
            ", limit=" + limit + ']', loaded <= limit);
    }
}
