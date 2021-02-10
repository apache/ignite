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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.query.h2.H2RowCache;
import org.apache.ignite.internal.processors.query.h2.opt.H2CacheRow;
import org.apache.ignite.testframework.GridTestUtils;
import org.jsr166.ConcurrentLinkedHashMap;
import org.junit.Test;

/**
 * Tests H2RowCacheRegistry.
 */
@SuppressWarnings({"unchecked", "ConstantConditions"})
public class H2RowCacheSelfTest extends AbstractIndexingCommonTest {
    /** Keys count. */
    private static final int ENTRIES = 1_000;

    /** Random generator. */
    private static final Random RND = new Random(System.currentTimeMillis());

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
     * @param name Cache name.
     * @param enableOnheapCache Enable on-heal SQL rows cache.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String name, boolean enableOnheapCache) {
        return new CacheConfiguration()
            .setName(name)
            .setSqlOnheapCacheEnabled(enableOnheapCache)
            .setGroupName("group")
            .setQueryEntities(Collections.singleton(
                new QueryEntity(Integer.class, Value.class)));
    }

    /**
     */
    @Test
    public void testDestroyCacheCreation() {
        final String cacheName0 = "cache0";
        final String cacheName1 = "cache1";

        grid().getOrCreateCache(cacheConfiguration(cacheName0, false));

        int grpId = grid().cachex(cacheName0).context().groupId();

        assertNull(rowCache(grid(), grpId));

        grid().getOrCreateCache(cacheConfiguration(cacheName1, true));

        assertEquals(grpId, grid().cachex(cacheName1).context().groupId());

        assertNotNull(rowCache(grid(), grpId));
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testDestroyCacheSingleCacheInGroup() throws IgniteCheckedException {
        checkDestroyCache();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testDestroyCacheWithOtherCacheInGroup() throws IgniteCheckedException {
        grid().getOrCreateCache(cacheConfiguration("cacheWithoutOnheapCache", false));

        checkDestroyCache();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeleteEntryCacheSingleCacheInGroup() throws Exception {
        checkDeleteEntry();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeleteEntryWithOtherCacheInGroup() throws Exception {
        grid().getOrCreateCache(cacheConfiguration("cacheWithoutOnheapCache", false));

        checkDeleteEntry();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUpdateEntryCacheSingleCacheInGroup() throws Exception {
        checkDeleteEntry();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUpdateEntryWithOtherCacheInGroup() throws Exception {
        grid().getOrCreateCache(cacheConfiguration("cacheWithoutOnheapCache", false));

        checkUpdateEntry();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFixedSize() throws Exception {
        int maxSize = 100;
        String cacheName = "cacheWithLimitedSize";

        CacheConfiguration ccfg = cacheConfiguration(cacheName, true).setSqlOnheapCacheMaxSize(maxSize);

        IgniteCache cache = grid().getOrCreateCache(ccfg);

        int grpId = grid().cachex(cacheName).context().groupId();

        // Fill half.
        for (int i = 0; i < maxSize / 2; i++)
            cache.put(i, new Value(1));

        H2RowCache rowCache = rowCache(grid(), grpId);

        assertEquals(0, rowCache.size());

        // Warmup cache.
        cache.query(new SqlFieldsQuery("SELECT * FROM Value")).getAll();

        assertEquals(maxSize / 2, rowCache.size());

        // Query again - are there any leaks?
        cache.query(new SqlFieldsQuery("SELECT * FROM Value")).getAll();

        assertEquals(maxSize / 2, rowCache.size());

        // Fill up to limit.
        for (int i = maxSize / 2; i < maxSize; i++)
            cache.put(i, new Value(1));

        assertEquals(maxSize / 2, rowCache.size());

        cache.query(new SqlFieldsQuery("SELECT * FROM Value")).getAll();

        assertEquals(maxSize, rowCache.size());

        // Out of limit.
        for (int i = maxSize; i < maxSize * 2; i++)
            cache.put(i, new Value(1));

        assertEquals(maxSize, rowCache.size());

        cache.query(new SqlFieldsQuery("SELECT * FROM Value")).getAll();

        assertEquals(maxSize, rowCache.size());

        // Delete all.
        cache.query(new SqlFieldsQuery("DELETE FROM Value")).getAll();

        assertEquals(0, rowCache.size());

        cache.query(new SqlFieldsQuery("SELECT * FROM Value")).getAll();

        assertEquals(0, rowCache.size());
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void checkDestroyCache() throws IgniteCheckedException {
        final String cacheName0 = "cache0";
        final String cacheName1 = "cache1";

        grid().getOrCreateCache(cacheConfiguration(cacheName0, true));
        grid().getOrCreateCache(cacheConfiguration(cacheName1, true));

        int grpId = grid().cachex(cacheName0).context().groupId();

        assertEquals(grpId, grid().cachex(cacheName1).context().groupId());

        try (IgniteDataStreamer<Integer, Value> streamer = grid().dataStreamer(cacheName0)) {
            for (int i = 0; i < ENTRIES / 2; ++i)
                streamer.addData(i, new Value(i));
        }

        try (IgniteDataStreamer<Integer, Value> streamer = grid().dataStreamer(cacheName1)) {
            for (int i = ENTRIES / 2; i < ENTRIES; ++i)
                streamer.addData(i, new Value(i));
        }

        H2RowCache rowCache = rowCache(grid(), grpId);

        assertNotNull(rowCache);

        Set<Long> linksOfCache0 = new HashSet<>(ENTRIES / 2);
        Set<Long> linksOfCache1 = new HashSet<>(ENTRIES / 2);

        for (int i = 0; i < ENTRIES / 2; ++i)
            linksOfCache0.add(getLinkForKey(cacheName0, rowCache(grid(), grpId), i));

        for (int i = ENTRIES / 2; i < ENTRIES; ++i)
            linksOfCache1.add(getLinkForKey(cacheName1, rowCache(grid(), grpId), i));

        grid().destroyCache(cacheName0);

        assertNotNull(rowCache(grid(), grpId));

        for (long link : linksOfCache0)
            assertNull(rowCache(grid(), grpId).get(link));

        grid().destroyCache(cacheName1);

        assertNull(rowCache(grid(), grpId));
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void checkDeleteEntry() throws Exception {
        final String cacheName = "cache";

        grid().getOrCreateCache(cacheConfiguration(cacheName, true));

        int grpId = grid().cachex(cacheName).context().groupId();

        assertEquals(grpId, grid().cachex(cacheName).context().groupId());

        fillCache(cacheName);

        H2RowCache rowCache = rowCache(grid(), grpId);

        fillRowCache(cacheName);

        assertNotNull(rowCache);

        int key = RND.nextInt(ENTRIES);

        grid().cache(cacheName)
            .query(new SqlQuery(Value.class, "_key = " + key)).getAll();

        int rowCacheSize = rowCache.size();

        long rowLink = getLinkForKey(cacheName, rowCache, key);

        assertNotNull(rowCache.get(rowLink));

        // Remove
        grid().cache(cacheName).remove(key);

        assertNull(rowCache.get(rowLink));

        int rowCacheSizeAfterUpdate = rowCache.size();

        assertEquals(rowCacheSize - 1, rowCacheSizeAfterUpdate);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void checkUpdateEntry() throws Exception {
        final String cacheName = "cache";

        grid().getOrCreateCache(cacheConfiguration(cacheName, true));

        int grpId = grid().cachex(cacheName).context().groupId();

        assertEquals(grpId, grid().cachex(cacheName).context().groupId());

        fillCache(cacheName);

        H2RowCache rowCache = rowCache(grid(), grpId);

        fillRowCache(cacheName);

        assertNotNull(rowCache);

        int key = RND.nextInt(ENTRIES);

        long rowLink = getLinkForKey(cacheName, rowCache, key);

        int rowCacheSize = rowCache.size();

        assertNotNull(rowCache.get(rowLink));

        // Update row
        grid().cache(cacheName).put(key, new Value(key + 1));

        assertNull(rowCache.get(rowLink));

        int rowCacheSizeAfterUpdate = rowCache.size();

        assertEquals(rowCacheSize - 1, rowCacheSizeAfterUpdate);

        // Check updated value.
        List<Cache.Entry<Integer, Value>> res = grid().<Integer, Value>cache(cacheName)
            .query(new SqlQuery(Value.class, "_key = " + key)).getAll();

        assertEquals(1, res.size());
        assertEquals(key + 1, (int)res.get(0).getValue().lVal);
    }

    /**
     * @param cacheName Cache name.
     * @param rowCache Row cache.
     * @param key Key to find.
     * @return Row's link.
     */
    private long getLinkForKey(String cacheName, H2RowCache rowCache, int key) {
        grid().cache(cacheName)
            .query(new SqlQuery(Value.class, "_key = " + key)).getAll().size();

        ConcurrentLinkedHashMap<Long, H2CacheRow> rowsMap = GridTestUtils.getFieldValue(rowCache, "rows");

        for (Map.Entry<Long, H2CacheRow> e : rowsMap.entrySet()) {
            H2CacheRow val = e.getValue();

            KeyCacheObject rowKey = val.key();

            if ((Integer)rowKey.value(null, false) == key)
                return e.getKey();
        }

        fail("Row cache doesn't contain key [key=" + key + ']');

        return -1;
    }

    /**
     * @param ig Ignite node.
     * @param grpId Cache group ID.
     * @return H2RowCache for checks.
     */
    private H2RowCache rowCache(IgniteEx ig, int grpId) {
        return (H2RowCache)ig.context().query().getIndexing().rowCacheCleaner(grpId);
    }

    /**
     * @param name Cache name.
     */
    private void fillCache(String name) {
        try (IgniteDataStreamer<Integer, Value> streamer = grid().dataStreamer(name)) {
            for (int i = 0; i < ENTRIES; ++i)
                streamer.addData(i, new Value(i));
        }
    }

    /**
     * @param name Cache name.
     */
    @SuppressWarnings("unchecked")
    private void fillRowCache(String name) {
        for (int i = 0; i < ENTRIES; ++i)
            grid().cache(name).query(new SqlQuery(Value.class, "_key = " + i)).getAll().size();
    }

    /**
     *
     */
    private static class Value {
        /** Long value. */
        @QuerySqlField
        private long lVal;

        /** String value. */
        @QuerySqlField
        private String strVal;

        /**
         * @param k Key.
         */
        Value(int k) {
            lVal = k;
            strVal = "val_" + k;
        }
    }
}
