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

import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_DATA_REG_DEFAULT_NAME;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_PAGE_SIZE;

/**
 * Tests the synergy between ExpiryPolicy (TTL cleanup) and size-aware page eviction on an in-memory data region.
 * Verifies that concurrent TTL cleanup and eviction do not deadlock, that a large row larger than the
 * empty-pages pool is still written when eviction is enabled, and that TTL-freed space is accounted for by eviction
 * (a row that only fits after expired entries are removed is still written without OOM).
 */
public abstract class PageEvictionWithExpiryPolicyAbstractTest extends GridCommonAbstractTest {
    /** Off-heap region size. */
    private static final int SIZE = 128 * 1024 * 1024;

    /** Partition count (kept low so that index-tree structures do not exhaust the region). */
    private static final int PARTITIONS = 32;

    /** Large record size (much larger than the empty-pages pool, and larger than the space left free when the region
     * is held at the eviction threshold {@code (1 - threshold) * totalPages}) so that a large put cannot take the
     * fast path of {@code ensureFreeSpaceForInsert} and must actually run the size-aware eviction reserve. */
    private static final int RECORD_SIZE = 32 * 1024 * 1024;

    /** Empty pages pool size. */
    private static final int POOL_SIZE = 100;

    /** Size of a single non-expiring pre-fill record (well below a data-page payload so each record surely occupies
     * exactly one data page). */
    private static final int SMALL_RECORD_SIZE = 1024;

    /** Number of non-expiring pre-fill records. Deliberately conservative: the pre-fill keeps a comfortable margin to
     * the region capacity so that the large short-TTL records are guaranteed not to be evicted before they expire. */
    private static final int SMALL_ENTRIES = 6_000;

    /** Fresh large records written after TTL expiry. Together with the pre-fill ({@link #SMALL_ENTRIES} small entries,
     * each occupying roughly one data page) plus the index structures, they exceed the region capacity (~32768 pages
     * for a 128 MiB region with 4 KiB pages), forcing size-aware eviction that accounts for the TTL-freed space. */
    private static final int FRESH_RECORDS = 12;

    /** Short TTL applied to some entries. */
    private static final long TTL = 8000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return super.getConfiguration(gridName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setInitialSize(SIZE)
                    .setMaxSize(SIZE)
                    .setEmptyPagesPoolSize(POOL_SIZE))
                .setPageSize(DFLT_PAGE_SIZE));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @param ignite Ignite node.
     * @param cacheName Cache name.
     * @param ttl TTL in milliseconds ({@code 0} for no expiry).
     * @return Cache with a small partition count and, if {@code ttl > 0}, eager TTL expiry.
     */
    private IgniteCache<Integer, Object> createCache(IgniteEx ignite, String cacheName, long ttl) {
        CacheConfiguration<Integer, Object> ccfg = new CacheConfiguration<Integer, Object>(cacheName)
            .setAffinity(new RendezvousAffinityFunction(false, PARTITIONS));

        if (ttl > 0) {
            ccfg.setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(MILLISECONDS, ttl)))
                .setEagerTtl(true);
        }

        return ignite.createCache(ccfg);
    }

    /**
     * Concurrent TTL cleanup and eviction must not deadlock, and a large record (larger than the empty-pages pool)
     * must still be stored on a region with enabled eviction even in the presence of short-TTL entries.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLargePutWithExpiryNoDeadlock() throws Exception {
        IgniteEx ignite = startGrid(1);

        // Short-TTL entries keep the TTL worker actively freeing pages while eviction runs.
        IgniteCache<Integer, Object> cache = createCache(ignite, DEFAULT_CACHE_NAME, TTL);

        Object val = new byte[RECORD_SIZE];

        // Writing more data than the region can hold forces eviction; concurrent expiry of short-TTL entries must not
        // deadlock with it. The test itself is protected against a hang by the framework test timeout.
        for (int i = 0; i < 30; i++)
            cache.put(i, val);

        // The most recently written entry cannot have expired yet (TTL is far larger than this read), so a non-null
        // read both verifies the cache is responsive after concurrent expiry/eviction (the test's goal) and is not
        // racy. Reading the first written key would be racy (it may already have expired under the TTL).
        assertNotNull("Cache must remain responsive after concurrent expiry and eviction", cache.get(29));
    }

    /**
     * Space freed by TTL cleanup must be taken into account by size-aware eviction: large records written after some
     * entries have expired must be accepted (no OOM) because their pages become available.
     * <p>
     * The region is pre-filled with non-expiring small entries and large short-TTL entries that later expire and free
     * their pages. After expiry, fresh large records are written — totalling more than the space freed by TTL, so that
     * the pre-fill plus the fresh records exceed the region size. The writes can only succeed because size-aware
     * eviction accounts for the TTL-freed pages (as available) and frees further pages for the rest.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTtlFreedSpaceAccountedForByEviction() throws Exception {
        IgniteEx ignite = startGrid(1);

        // Non-expiring cache for prefill and the final large put.
        IgniteCache<Integer, Object> plainCache = createCache(ignite, "plain-cache", 0);

        // Short-TTL cache for entries that will expire and free pages.
        IgniteCache<Integer, Object> ttlCache = createCache(ignite, "ttl-cache", TTL);

        // Pre-fill the region with small non-expiring entries, but leave enough room for the large TTL entries to be
        // written without evicting them (so they are guaranteed to be present until they expire).
        byte[] small = new byte[SMALL_RECORD_SIZE];

        for (int i = 0; i < SMALL_ENTRIES; i++)
            plainCache.put(i, small);

        info("Pre-fill done [entries=" + SMALL_ENTRIES + ", plainSize=" + plainCache.size() +
            ", loadedPages=" + ignite.dataRegionMetrics(DFLT_DATA_REG_DEFAULT_NAME).getTotalAllocatedPages() + ']');

        // Add large short-TTL entries that occupy significant space and will expire. They fit in the remaining free
        // space, and being the freshest entries they are not evicted while they are being stored.
        Object val = new byte[RECORD_SIZE];

        for (int i = 0; i < 2; i++)
            ttlCache.put(i, val);

        // Verify the TTL entries are present before expiry.
        assertNotNull("TTL entry must be present before expiry", ttlCache.get(0));

        // Wait for the TTL worker to expire and free the short-TTL entries. Polled instead of a fixed sleep so that a
        // slow CI machine does not proceed before the entries have actually expired.
        long expiryDeadline = System.currentTimeMillis() + 30_000;

        while (ttlCache.get(0) != null && System.currentTimeMillis() < expiryDeadline)
            U.sleep(200);

        // Verify the TTL entries have expired.
        assertNull("TTL entry must be expired", ttlCache.get(0));

        // Now write large records to the non-expiring cache, totalling more than the space freed by TTL (the prefill
        // plus the fresh large records exceed the region size). The put can only succeed because size-aware eviction
        // accounts for the pages freed by TTL (as available) and frees further pages for the rest; this verifies the
        // synergy between TTL cleanup and size-aware eviction — a row that only fits once expired entries are removed
        // is written without OOM.
        for (int i = 0; i < FRESH_RECORDS; i++)
            plainCache.put(100 + i, val);

        // The most recently written large record is still present: it was accepted without OOM thanks to the space
        // freed by TTL and the additional pages freed by size-aware eviction. (Earlier large records may already have
        // been evicted to make room for the subsequent ones, so only the freshest is asserted.)
        assertNotNull("Fresh large record must be present after TTL-assisted eviction",
            plainCache.get(100 + FRESH_RECORDS - 1));
    }
}
