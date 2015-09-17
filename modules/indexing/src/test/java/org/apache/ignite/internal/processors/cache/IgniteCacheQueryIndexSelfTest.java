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
import java.util.HashMap;
import java.util.Map;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CachePeekMode.ALL;

/**
 * Tests for cache query index.
 */
public class IgniteCacheQueryIndexSelfTest extends GridCacheAbstractSelfTest {
    /** Grid count. */
    private static final int GRID_CNT = 2;

    /** Entry count. */
    private static final int ENTRY_CNT = 10;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /**
     * @throws Exception If failed.
     */
    public void testWithoutStoreLoad() throws Exception {
        IgniteCache<Integer, CacheValue> cache = grid(0).cache(null);

        for (int i = 0; i < ENTRY_CNT; i++)
            cache.put(i, new CacheValue(i));

        checkCache(cache);
        checkQuery(cache);
    }

    /** {@inheritDoc} */
    @Override protected Class<?>[] indexedTypes() {
        return new Class<?>[]{Integer.class, CacheValue.class};
    }

    /**
     * @throws Exception If failed.
     */
    public void testWithStoreLoad() throws Exception {
        for (int i = 0; i < ENTRY_CNT; i++)
            putToStore(i, new CacheValue(i));

        IgniteCache<Integer, CacheValue> cache0 = grid(0).cache(null);

        cache0.loadCache(null);

        checkCache(cache0);
        checkQuery(cache0);
    }

    /**
     * @param cache Cache.
     * @throws Exception If failed.
     */
    private void checkCache(IgniteCache<Integer, CacheValue> cache) throws Exception {
        Map<Integer, CacheValue> map = new HashMap<>();

        for (Cache.Entry<Integer, CacheValue> entry : cache)
            map.put(entry.getKey(), entry.getValue());

        assert map.entrySet().size() == ENTRY_CNT : "Expected: " + ENTRY_CNT + ", but was: " + cache.size();
        assert map.keySet().size() == ENTRY_CNT : "Expected: " + ENTRY_CNT + ", but was: " + cache.size();
        assert map.values().size() == ENTRY_CNT : "Expected: " + ENTRY_CNT + ", but was: " + cache.size();
        assert cache.localSize(ALL) == ENTRY_CNT : "Expected: " + ENTRY_CNT + ", but was: " + cache.localSize();
    }

    /**
     * @param cache Cache.
     * @throws Exception If failed.
     */
    private void checkQuery(IgniteCache<Integer, CacheValue> cache) throws Exception {
        QueryCursor<Cache.Entry<Integer, CacheValue>> qry =
            cache.query(new SqlQuery(CacheValue.class, "val >= 5"));

        Collection<Cache.Entry<Integer, CacheValue>> queried = qry.getAll();

        assertEquals("Unexpected query result: " + queried, 5, queried.size());
    }

    /**
     * Test cache value.
     */
    private static class CacheValue {
        @QuerySqlField
        private final int val;

        CacheValue(int val) {
            this.val = val;
        }

        int value() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CacheValue.class, this);
        }
    }
}