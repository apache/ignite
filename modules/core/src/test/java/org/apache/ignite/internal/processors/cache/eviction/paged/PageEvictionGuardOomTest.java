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
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_PAGE_SIZE;

/**
 * Negative test for the size-aware eviction progress guard.
 * <p>
 * When every resident entry is locked by another thread/transaction, page eviction cannot free any page: the guarded
 * {@code tryLockEntry} in {@code evictInternal} fails for every candidate, so {@link
 * IgniteCacheDatabaseSharedManager#ensureFreeSpaceForEviction} makes no progress and must fail with an
 * {@code IgniteOutOfMemoryException} within bounded time instead of busy-spinning forever (deadlock).
 * <p>
 * The lock timeout is reduced via {@code -DENTRY_LOCK_TIMEOUT=1} (applied through {@code @WithSystemProperty} before
 * the node starts) so that each non-blocking lock attempt fails quickly and the whole guard run stays within a few
 * seconds. The test is self-guarded by {@code @Test(timeout = ...)}: a deadlock or unbounded busy-spin would fail the
 * deadline.
 */
public class PageEvictionGuardOomTest extends GridCommonAbstractTest {
    /** Off-heap region size. */
    private static final int SIZE = 12 * 1024 * 1024;

    /** Partition count (kept low so that index-tree structures do not exhaust the region). */
    private static final int PARTITIONS = 32;

    /** Empty pages pool size. */
    private static final int POOL_SIZE = 100;

    /** Small record size chosen to occupy roughly one data page ({@link DFLT_PAGE_SIZE}) each. */
    private static final int FILL_VALUE_SIZE = 3_800;

    /**
     * Number of resident entries (each ~one page) filling the region to ~55% of its capacity. This keeps the region
     * comfortably below the eviction threshold (so the ordinary threshold-based {@code ensureFreeSpace} path is a
     * no-op) while leaving less free space than a single large record needs, so the size-aware eviction guard is
     * exercised.
     */
    private static final int FILL_ENTRIES = 1_600;

    /** Large record size that does not fit into the remaining free space (requires eviction to be stored). */
    private static final int LARGE_RECORD_SIZE = 8 * 1024 * 1024;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return super.getConfiguration(gridName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setInitialSize(SIZE)
                    .setMaxSize(SIZE)
                    .setEmptyPagesPoolSize(POOL_SIZE)
                    .setPageEvictionMode(DataPageEvictionMode.RANDOM_LRU)
                )
                .setPageSize(DFLT_PAGE_SIZE)
            );
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @param ignite Ignite node.
     * @return Cache with a small partition count (reduces structural page overhead).
     */
    private IgniteCache<Integer, Object> createCache(IgniteEx ignite) {
        // TRANSACTIONAL is required so that cache.lockAll(...) can hold entry locks (the root cause of the
        // "no evictable page" scenario this test exercises).
        return ignite.createCache(new CacheConfiguration<Integer, Object>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, PARTITIONS))
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));
    }

    /**
     * Filling the region with locked entries and then writing a row that needs more free pages than remain must fail
     * with OOM (bounded time), not hang: eviction cannot free any page because every candidate entry is locked.
     *
     * @throws Exception If failed.
     */
    @Test(timeout = 180_000)
    @WithSystemProperty(key = "ENTRY_LOCK_TIMEOUT", value = "1")
    public void testGuardOomWhenAllEntriesLocked() throws Exception {
        IgniteEx ignite = startGrid(1);

        IgniteCache<Integer, Object> cache = createCache(ignite);

        // Pre-fill the region so that less than one large record of free space remains, without overflowing it.
        byte[] fillVal = new byte[FILL_VALUE_SIZE];

        for (int i = 1; i <= FILL_ENTRIES; i++)
            cache.put(i, fillVal);

        Collection<Integer> keys = new ArrayList<>(FILL_ENTRIES);

        for (int i = 1; i <= FILL_ENTRIES; i++)
            keys.add(i);

        CountDownLatch ready = new CountDownLatch(1);

        CountDownLatch release = new CountDownLatch(1);

        AtomicReference<Throwable> lockerErr = new AtomicReference<>();

        // Hold entry locks on every resident key from a background thread so that eviction has no evictable page.
        Thread locker = new Thread(() -> {
            try {
                Lock lock = cache.lockAll(keys);

                lock.lock();

                ready.countDown();

                release.await();

                lock.unlock();
            }
            catch (Throwable e) {
                lockerErr.set(e);

                ready.countDown();
            }
        }, "size-aware-guard-locker");

        locker.start();

        assertTrue("Timed out waiting for entries to be locked", ready.await(60, TimeUnit.SECONDS));

        assertNull("Unexpected error while locking entries: " + lockerErr.get(), lockerErr.get());

        try {
            cache.put(FILL_ENTRIES + 1, new byte[LARGE_RECORD_SIZE]);

            fail("Expected out-of-memory because all resident entries are locked, but put succeeded");
        }
        catch (Exception e) {
            assertTrue("Expected an out-of-memory (progress guard) failure, but got: " + e, isOutOfMemory(e));
        }
        finally {
            release.countDown();

            locker.join(TimeUnit.SECONDS.toMillis(10));
        }
    }

    /**
     * @param t Throwable.
     * @return {@code True} if {@code t} or any of its causes is an out-of-memory.
     */
    private static boolean isOutOfMemory(Throwable t) {
        for (Throwable cur = t; cur != null; cur = cur.getCause()) {
            if (cur instanceof IgniteOutOfMemoryException)
                return true;
        }

        return false;
    }
}
