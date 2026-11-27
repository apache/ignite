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

package org.apache.ignite.internal.processors.cache.index;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRowCache;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRowImpl;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.testframework.GridTestUtils;
import org.jsr166.ConcurrentLinkedHashMap;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests that the group-level SQL onheap index row cache holds only rows that exist:
 * a link is only an address, so a row cached for a link that a partition destroy freed
 * (and a row of the new generation of the same or another cache of the group reused)
 * serves the row of the old generation to index operations.
 */
public class IndexRowCachePartitionDestroySelfTest extends AbstractIndexingCommonTest {
    /** Cache group shared by the test caches. */
    private static final String GRP_NAME = "rowCacheChurnGroup";

    /** Cache with the SQL onheap row cache enabled. */
    private static final String CACHE_A = "cacheA";

    /** Cache with the SQL onheap row cache disabled. */
    private static final String CACHE_B = "cacheB";

    /** Key offset for cache B entries, to distinguish rows of the caches by key value. */
    private static final int B_KEY_OFF = 1_000_000;

    /** Number of entries per cache. */
    private static final int ENTRIES = 2_000;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Rows of a cache with the SQL onheap row cache disabled must not be cached in the row
     * cache of the group, even though the row cache exists for the group (another cache of
     * the group enables it): a cached row of the disabled cache is resolved by index trees
     * of any cache of the group, which is how a stale entry of one cache poisons index
     * operations of another one.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRowsOfCacheWithDisabledSqlOnheapCacheAreNotCached() throws Exception {
        startGrid();

        IgniteCache<Integer, Object> cacheA = grid().getOrCreateCache(cacheConfiguration(CACHE_A, ValueA.class, true));
        IgniteCache<Integer, Object> cacheB = grid().getOrCreateCache(cacheConfiguration(CACHE_B, ValueB.class, false));

        for (int i = 0; i < ENTRIES; i++) {
            cacheA.put(i, new ValueA("a_" + i));
            cacheB.put(B_KEY_OFF + i, new ValueB("b_" + i));
        }

        // Warm up the group row cache: a range scan over the indexed field resolves every
        // row through the sorted index tree of its cache.
        assertEquals("Scan of cache A must resolve every entry", ENTRIES, scan(cacheA, ValueA.class, "a_").size());
        assertEquals("Scan of cache B must resolve every entry", ENTRIES, scan(cacheB, ValueB.class, "b_").size());

        CacheGroupContext grp = grid().cachex(CACHE_A).context().group();

        IndexRowCache rowCache = grid().context().indexProcessor().rowCacheCleaner(grp.groupId());

        assertNotNull("Row cache must exist for the group", rowCache);

        ConcurrentLinkedHashMap<Long, IndexRowImpl> rows = GridTestUtils.getFieldValue(rowCache, "rows");

        assertTrue("Row cache must contain rows of cache A", rows.size() > 0);

        for (Map.Entry<Long, IndexRowImpl> e : rows.entrySet()) {
            Object key = e.getValue().cacheDataRow().key().value(null, false);

            if (key instanceof Integer && (Integer)key >= B_KEY_OFF)
                fail("Row cache must not contain rows of cache B with disabled SQL onheap cache [link=" +
                    e.getKey() + ", key=" + key + ']');
        }
    }

    /**
     * Stale row cache entries of a partition destroyed on rebalancing must not be served to index
     * operations of rows of the new generation of the partition: a link is only an address, so when
     * the partition returns to the node after another rebalance, rows of the new generation reuse
     * the links of the old one, and a stale entry for such a link resolves the row of the old
     * generation for a link of the new one.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionReturnAfterRebalanceServesFreshRows() throws Exception {
        startGrid(0);
        startGrid(1);

        awaitPartitionMapExchange(true, true, null);

        IgniteCache<Integer, Object> cacheA = grid(0).getOrCreateCache(cacheConfiguration(CACHE_A, ValueA.class, true));

        awaitPartitionMapExchange(true, true, null);

        Map<Integer, String> exp = new HashMap<>();

        for (int i = 0; i < ENTRIES; i++) {
            cacheA.put(i, new ValueA("gen0_" + i));

            exp.put(i, "gen0_" + i);
        }

        // Warm the group row cache on the first node: a range scan resolves every row of every
        // primary partition of the node through its sorted index tree.
        assertEquals(exp, scan(cacheA, ValueA.class, "gen0_"));

        // Concurrent index row resolution on the first node races with the per-row row-cache
        // cleanup of the partitions the node loses; a row resolved and cached in the window the
        // cleanup has already passed is a stale entry of a destroyed partition.
        churnPartitions(() -> cacheA.query(new SqlQuery<>(ValueA.class, "pid >= 'gen0_'")).getAll());

        // Rows of the new generation of the returned partitions reuse the links of the old one.
        for (int i = ENTRIES; i < ENTRIES + ENTRIES / 2; i++) {
            cacheA.put(i, new ValueA("gen1_" + i));

            exp.put(i, "gen1_" + i);
        }

        Map<Integer, String> res = scan(cacheA, ValueA.class, "gen");

        assertEquals("Scan must resolve the exact set of entries of both generations", exp, res);
    }

    /**
     * Index operations of a cache of a group must resolve only rows of that cache through the
     * group row cache when partitions of the group are destroyed on rebalancing and their links
     * are reused by rows of another cache of the group.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRowsOfOtherCacheAreNotServedOnPartitionChurn() throws Exception {
        startGrid(0);
        startGrid(1);

        awaitPartitionMapExchange(true, true, null);

        IgniteCache<Integer, Object> cacheA = grid(0).getOrCreateCache(cacheConfiguration(CACHE_A, ValueA.class, true));
        IgniteCache<Integer, Object> cacheB = grid(0).getOrCreateCache(cacheConfiguration(CACHE_B, ValueB.class, true));

        awaitPartitionMapExchange(true, true, null);

        Map<Integer, String> expA = new HashMap<>();
        Map<Integer, String> expB = new HashMap<>();

        for (int i = 0; i < ENTRIES; i++) {
            cacheA.put(i, new ValueA("a_" + i));
            cacheB.put(B_KEY_OFF + i, new ValueB("b_" + i));

            expA.put(i, "a_" + i);
            expB.put(B_KEY_OFF + i, "b_" + i);
        }

        assertEquals(expA, scan(cacheA, ValueA.class, "a_"));
        assertEquals(expB, scan(cacheB, ValueB.class, "b_"));

        churnPartitions(
            () -> cacheA.query(new SqlQuery<>(ValueA.class, "pid >= 'a_'")).getAll(),
            () -> cacheB.query(new SqlQuery<>(ValueB.class, "pid >= 'b_'")).getAll());

        // Rows of the new generation of cache B reuse the links freed by the churn.
        for (int i = 0; i < ENTRIES / 2; i++) {
            cacheB.put(2 * B_KEY_OFF + i, new ValueB("b2_" + i));

            expB.put(2 * B_KEY_OFF + i, "b2_" + i);
        }

        assertEquals("Scan of cache A must resolve exactly the entries of cache A", expA, scan(cacheA, ValueA.class, "a_"));
        assertEquals("Scan of cache B must resolve exactly the entries of cache B", expB, scan(cacheB, ValueB.class, "b"));
    }

    /**
     * The partition destroy cleanup must remove exactly the entries of the destroyed partition
     * from the group row cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClearForPartitionRemovesEntriesOfDestroyedPartition() throws Exception {
        startGrid();

        IgniteCache<Integer, Object> cacheA = grid().getOrCreateCache(cacheConfiguration(CACHE_A, ValueA.class, true));

        for (int i = 0; i < ENTRIES; i++)
            cacheA.put(i, new ValueA("a_" + i));

        assertEquals(ENTRIES, scan(cacheA, ValueA.class, "a_").size());

        CacheGroupContext grp = grid().cachex(CACHE_A).context().group();

        IndexRowCache rowCache = grid().context().indexProcessor().rowCacheCleaner(grp.groupId());

        assertNotNull("Row cache must exist for the group", rowCache);

        Affinity<Integer> aff = grid().affinity(CACHE_A);

        int part0 = -1;
        int part1 = -1;

        for (int i = 0; part1 == -1 && i < ENTRIES; i++) {
            int p = aff.partition(i);

            if (part0 == -1)
                part0 = p;
            else if (p != part0)
                part1 = p;
        }

        assertTrue("Test needs entries of at least two partitions", part1 != -1);

        rowCache.clearForPartition(part0);

        ConcurrentLinkedHashMap<Long, IndexRowImpl> rows = GridTestUtils.getFieldValue(rowCache, "rows");

        boolean hasOtherPart = false;

        for (Map.Entry<Long, IndexRowImpl> e : rows.entrySet()) {
            int p = PageIdUtils.partId(e.getKey());

            assertTrue("Row cache must contain no entry of the cleared partition [link=" + e.getKey() + ']', p != part0);

            if (p == part1)
                hasOtherPart = true;
        }

        assertTrue("Row cache must keep entries of other partitions", hasOtherPart);
    }

    /**
     * Moves partitions of the first node away on rebalancing and back, twice, while concurrent
     * scans of the given caches keep resolving index rows on the node.
     *
     * @param scans Scans to run concurrently to the churn.
     * @throws Exception If failed.
     */
    private void churnPartitions(final Runnable... scans) throws Exception {
        final AtomicBoolean hammer = new AtomicBoolean();

        Collection<IgniteInternalFuture<?>> hammers = new ArrayList<>();

        for (int i = 0; i < 4; i++) {
            final Runnable scan = scans[i % scans.length];

            hammers.add(GridTestUtils.runAsync(() -> {
                while (!hammer.get()) {
                    try {
                        scan.run();
                    }
                    catch (Exception e) {
                        // No assertions on concurrent scans: partitions are moving.
                    }
                }
            }, "scan-hammer"));
        }

        try {
            // Two rounds of a partition leave and return on the first node.
            for (int round = 0; round < 2; round++) {
                startGrid(2);

                awaitPartitionMapExchange(true, true, null);

                stopGrid(2);

                awaitPartitionMapExchange(true, true, null);
            }
        }
        finally {
            hammer.set(true);

            for (IgniteInternalFuture<?> f : hammers)
                f.get();
        }
    }

    /**
     * @param cache Cache.
     * @param valCls Indexed value class.
     * @param pfx Common prefix of the indexed field of the entries.
     * @return Rows of the cache resolved through the sorted index tree by key.
     */
    private <V> Map<Integer, String> scan(IgniteCache<Integer, Object> cache, Class<V> valCls, String pfx) {
        Map<Integer, String> res = new HashMap<>();

        try (QueryCursor<Cache.Entry<Integer, V>> cur = cache.query(
            new SqlQuery<>(valCls, "pid >= '" + pfx + '\''))
        ) {
            for (Cache.Entry<Integer, V> e : cur)
                res.put(e.getKey(), pid(e.getValue()));
        }

        return res;
    }

    /**
     * @param val Indexed value.
     * @return Value of the indexed field.
     */
    private String pid(Object val) {
        return val instanceof ValueA ? ((ValueA)val).pid : ((ValueB)val).pid;
    }

    /**
     * @param name Cache name.
     * @param valCls Indexed value class.
     * @param onheap {@code True} to enable the SQL onheap row cache for the cache.
     * @return Cache configuration of the shared group.
     */
    private CacheConfiguration<Integer, Object> cacheConfiguration(String name, Class<?> valCls, boolean onheap) {
        CacheConfiguration<Integer, Object> ccfg = new CacheConfiguration<>(name);

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setBackups(1);
        ccfg.setGroupName(GRP_NAME);
        ccfg.setSqlOnheapCacheEnabled(onheap);
        ccfg.setSqlOnheapCacheMaxSize(100_000);
        ccfg.setIndexedTypes(Integer.class, valCls);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 16));

        return ccfg;
    }

    /** */
    private static class ValueA {
        /** Indexed field, inline size 0 forces every index comparison to resolve the full row. */
        @QuerySqlField(index = true, inlineSize = 0) private String pid;

        /**
         * @param pid Value of the indexed field.
         */
        private ValueA(String pid) {
            this.pid = pid;
        }
    }

    /** */
    private static class ValueB {
        /** Indexed field, inline size 0 forces every index comparison to resolve the full row. */
        @QuerySqlField(index = true, inlineSize = 0) private String pid;

        /**
         * @param pid Value of the indexed field.
         */
        private ValueB(String pid) {
            this.pid = pid;
        }
    }
}
