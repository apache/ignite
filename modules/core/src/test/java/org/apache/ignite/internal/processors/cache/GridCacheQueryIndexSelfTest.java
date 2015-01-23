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

import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.util.*;

import static org.apache.ignite.cache.CacheMode.*;

/**
 * Tests for cache query index.
 */
public class GridCacheQueryIndexSelfTest extends GridCacheAbstractSelfTest {
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
        GridCache<Integer, CacheValue> cache = grid(0).cache(null);

        for (int i = 0; i < ENTRY_CNT; i++)
            cache.put(i, new CacheValue(i));

        checkCache(cache);
        checkQuery(cache, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testWithStoreLoad() throws Exception {
        for (int i = 0; i < ENTRY_CNT; i++)
            putToStore(i, new CacheValue(i));

        GridCache<Integer, CacheValue> cache0 = grid(0).cache(null);

        cache0.loadCache(null, 0);

        checkCache(cache0);
        checkQuery(cache0, true);
    }

    /**
     * @param cache Cache.
     * @throws Exception If failed.
     */
    private void checkCache(CacheProjection<Integer,CacheValue> cache) throws Exception {
        assert cache.entrySet().size() == ENTRY_CNT : "Expected: " + ENTRY_CNT + ", but was: " + cache.size();
        assert cache.keySet().size() == ENTRY_CNT : "Expected: " + ENTRY_CNT + ", but was: " + cache.size();
        assert cache.values().size() == ENTRY_CNT : "Expected: " + ENTRY_CNT + ", but was: " + cache.size();
        assert cache.size() == ENTRY_CNT : "Expected: " + ENTRY_CNT + ", but was: " + cache.size();
    }

    /**
     * @param cache Cache.
     * @param backups Include backups flag.
     * @throws Exception If failed.
     */
    private void checkQuery(CacheProjection<Integer, CacheValue> cache, boolean backups) throws Exception {
        CacheQuery<Map.Entry<Integer, CacheValue>> qry = cache.queries().createSqlQuery(
            CacheValue.class, "val >= 5");

        if (backups)
            qry.includeBackups(true);

        Collection<Map.Entry<Integer, CacheValue>> queried = qry.execute().get();

        assertEquals("Unexpected query result: " + queried, 5, queried.size());
    }

    /**
     * Test cache value.
     */
    private static class CacheValue {
        @CacheQuerySqlField
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
