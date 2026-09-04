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
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
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

    /** Large record size (much larger than the empty-pages pool). */
    private static final int RECORD_SIZE = 8 * 1024 * 1024;

    /** Empty pages pool size. */
    private static final int POOL_SIZE = 100;

    /** Short TTL applied to some entries. */
    private static final long TTL = 1500;

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

        cache.get(0);
    }

    /**
     * Space freed by TTL cleanup must be taken into account by size-aware eviction: a large record written after some
     * entries have expired must be accepted (no OOM) because their pages become available.
     * <p>
     * The region is filled to near capacity with non-expiring small entries, then large short-TTL entries are added
     * (evicting some small entries). After the TTL entries expire and their pages are freed, a fresh large record is
     * written — it can only fit if size-aware eviction accounts for the space freed by TTL.
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

        // Pre-fill the region to near capacity with small non-expiring entries.
        byte[] small = new byte[4096];

        for (int i = 0; i < 28_000; i++)
            plainCache.put(i, small);

        // Add large short-TTL entries that occupy significant space and will expire.
        Object val = new byte[RECORD_SIZE];

        for (int i = 0; i < 4; i++)
            ttlCache.put(i, val);

        // Verify the TTL entries are present before expiry.
        assertNotNull("TTL entry must be present before expiry", ttlCache.get(0));

        // Wait for the TTL worker to expire and free the short-TTL entries.
        Thread.sleep(TTL + 1500);

        // Verify the TTL entries have expired.
        assertNull("TTL entry must be expired", ttlCache.get(0));

        // A fresh large record must now be accepted: space freed by TTL counts as available for eviction.
        plainCache.put(100, val);

        assertNotNull(plainCache.get(100));
    }
}
