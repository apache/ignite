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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRowCache;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRowImpl;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.testframework.GridTestUtils;
import org.jsr166.ConcurrentLinkedHashMap;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

/**
 * Tests that a stale {@link IndexRowCache} entry left by a row of one cache is not served to index
 * operations of another cache of the same cache group.
 * <p>
 * The index row cache is group-level and is keyed by a bare data row link, while a link is only
 * an address: after a bulk free that bypasses per-row row-cache cleanup (e.g. a partition destroy
 * on rebalancing) and a subsequent link reuse by a row of another table of the group, a stale entry
 * is resolved for the reused link. {@code InlineIndexTree#createIndexRow} re-wraps such an entry
 * with the current index's row handler without any row ownership validation, so the comparator
 * reads a field of the foreign key/value with the current table's {@code BinaryField}: up to 2.17
 * this fails with BinaryObjectException (corrupted-tree failure, node restart); since IGNITE-26261
 * the binary property re-binds to the type of the passed object, so the index of one table is
 * silently maintained with the field values of another table's rows, and a range scan of one table
 * returns rows of another table.
 * <p>
 * The stale row cache state is injected directly: it is exactly the state the node ends up in
 * after a bulk free followed by link reuse. With the ownership validation in place, the poisoned
 * entries must be evicted on first access and re-read from the link, so that index operations of
 * cache A observe only rows of cache A.
 */
public class IndexRowCacheCrossTablePoisonTest extends AbstractIndexingCommonTest {
    /** Number of entries per cache. */
    private static final int ENTRIES = 50;

    /** Common prefix of the indexed field. */
    private static final String PID_PFX = "common_pid_prefix_";

    /** Cache group shared by both caches. */
    private static final String GRP_NAME = "poisonGroup";

    /** Cache with the SQL onheap row cache enabled. */
    private static final String CACHE_A = "cacheA";

    /** Cache with the SQL onheap row cache disabled. */
    private static final String CACHE_B = "cacheB";

    /** Key offset for cache B entries, to distinguish rows of the caches by key value. */
    private static final int B_KEY_OFF = 100;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * A stale row cache entry of cache B resolved by an index tree of cache A of the same cache
     * group must be evicted and re-read from the link, not served to index operations of cache A.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testStaleEntryOfOtherCacheIsNotServedToIndexOperations() throws Exception {
        // Cache A enables the SQL onheap row cache for the whole group. Cache B has it disabled,
        // but its rows are cached anyway, because the row cache and its wiring are group-level.
        IgniteCache<Integer, Object> cacheA = grid().getOrCreateCache(cacheConfiguration(CACHE_A, ValueA.class, true));
        IgniteCache<Integer, Object> cacheB = grid().getOrCreateCache(cacheConfiguration(CACHE_B, ValueB.class, false));

        for (int i = 0; i < ENTRIES; i++) {
            cacheA.put(i, new ValueA(PID_PFX + i));
            cacheB.put(B_KEY_OFF + i, new ValueB(PID_PFX + i));
        }

        // Warm up the group row cache with rows of both caches: a range scan over the indexed
        // field resolves every row through its sorted index tree.
        cacheA.query(new SqlQuery(ValueA.class, "pid >= '" + PID_PFX + "0'")).getAll();
        cacheB.query(new SqlQuery(ValueB.class, "pid >= '" + PID_PFX + "0'")).getAll();

        IndexRowCache rowCache = rowCache(grid(), grid().cachex(CACHE_A).context().groupId());

        assertNotNull("Row cache must exist for the group", rowCache);

        ConcurrentLinkedHashMap<Long, IndexRowImpl> rowsMap = GridTestUtils.getFieldValue(rowCache, "rows");

        List<Long> linksA = new ArrayList<>(ENTRIES);
        IndexRowImpl rowOfB = null;

        for (Map.Entry<Long, IndexRowImpl> e : rowsMap.entrySet()) {
            IndexRowImpl row = e.getValue();
            Object key = row.cacheDataRow().key() == null ? null : row.cacheDataRow().key().value(null, false);

            if (key instanceof Integer) {
                int k = (Integer)key;

                if (k >= 0 && k < ENTRIES)
                    linksA.add(e.getKey());
                else if (k >= B_KEY_OFF && k < B_KEY_OFF + ENTRIES && rowOfB == null)
                    rowOfB = row;
            }
        }

        assertFalse("Row cache must contain rows of cache A", linksA.isEmpty());
        assertNotNull("Row cache must contain rows of cache B despite its disabled SQL onheap cache", rowOfB);

        // Baseline: the same insert succeeds before the row cache is poisoned.
        cacheA.put(ENTRIES, new ValueA(PID_PFX + ENTRIES));

        // Simulate the stale state after a bulk free (e.g. a partition destroy on rebalancing)
        // followed by link reuse: every link of cache A now resolves to a materialized row
        // of cache B.
        CacheDataRow dataRowB = rowOfB.cacheDataRow();
        int cacheIdB = grid().cachex(CACHE_B).context().cacheId();

        for (long link : linksA)
            rowCache.put(new IndexRowImpl(rowOfB.rowHandler(), new ForeignRow(dataRowB, link)), cacheIdB);

        // Probe: a poisoned entry must be reachable through the same row cache the index trees use.
        assertNotNull(rowCache.get(linksA.get(0)));

        // Find path: a range scan resolves stored rows through the index row cache; it must
        // return only rows of cache A.
        List<?> res = cacheA.query(new SqlQuery(ValueA.class, "pid >= '" + PID_PFX + "0'")).getAll();

        int rowsOfCacheB = 0;

        for (Object o : res) {
            Object key = ((Cache.Entry<?, ?>)o).getKey();

            if (key instanceof Integer && (Integer)key >= B_KEY_OFF)
                rowsOfCacheB++;
        }

        log.info("Range scan on cache A [total=" + res.size() + ", rowsOfCacheB=" + rowsOfCacheB + ']');

        assertEquals("Scan of cache A must not return rows of cache B through stale row cache entries",
            0, rowsOfCacheB);
        assertEquals("Scan of cache A must return all rows of cache A", ENTRIES + 1, res.size());

        // Put path: index maintenance on insert must not observe stale entries either.
        cacheA.put(ENTRIES + 1, new ValueA(PID_PFX + (ENTRIES + 1)));
    }

    /**
     * @param name Cache name.
     * @param valCls Value class.
     * @param onheapCache SQL onheap row cache enabled.
     * @return Cache configuration.
     */
    private static CacheConfiguration<Integer, Object> cacheConfiguration(String name, Class<?> valCls,
        boolean onheapCache) {
        return new CacheConfiguration<Integer, Object>(name)
            .setGroupName(GRP_NAME)
            .setSqlOnheapCacheEnabled(onheapCache)
            .setQueryEntities(Collections.singleton(new QueryEntity(Integer.class, valCls)));
    }

    /**
     * @param ig Ignite node.
     * @param grpId Cache group ID.
     * @return Index row cache of the group.
     */
    private IndexRowCache rowCache(IgniteEx ig, int grpId) {
        return (IndexRowCache)ig.context().indexProcessor().rowCacheCleaner(grpId);
    }

    /** */
    private static class ForeignRow extends CacheDataRowAdapter {
        /** Link of a row of another cache of the group. */
        private final long foreignLink;

        /**
         * @param src Row to take the data from.
         * @param foreignLink Link to report.
         */
        ForeignRow(CacheDataRow src, long foreignLink) {
            super(src.key(), src.value(), src.version(), src.expireTime());

            this.foreignLink = foreignLink;
        }

        /** {@inheritDoc} */
        @Override public long link() {
            return foreignLink;
        }
    }

    /** */
    private static class ValueA {
        /** Indexed field with disabled inlining, so that every index comparison resolves the full row. */
        @QuerySqlField(index = true, inlineSize = 0)
        private String pid;

        /**
         * @param pid Indexed value.
         */
        ValueA(String pid) {
            this.pid = pid;
        }
    }

    /** */
    private static class ValueB {
        /** Indexed field with disabled inlining, so that every index comparison resolves the full row. */
        @QuerySqlField(index = true, inlineSize = 0)
        private String pid;

        /**
         * @param pid Indexed value.
         */
        ValueB(String pid) {
            this.pid = pid;
        }
    }
}
