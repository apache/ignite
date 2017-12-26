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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.h2.H2RowCache;
import org.apache.ignite.internal.processors.query.h2.H2RowCacheRegistry;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2KeyValueRowOnheap;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests H2RowCacheRegistry
 */
public class H2RowCachePageEvictionTest extends GridCommonAbstractTest {
    /** Entries count. */
    private static final int ENTRIES = 30_000;

    /** Offheap size for memory policy. */
    private static final int SIZE = 32 * 1024 * 1024;

    /** Test time. */
    private static final int TEST_TIME = 3 * 60_000;

    /** Default policy name. */
    private static final String DATA_REGION_NAME = "default";

    /** Default policy name. */
    private static final String CACHE_NAME = "cache";

    /** Random generator. */
    private static final ThreadLocalRandom RND = ThreadLocalRandom.current();

    /** Default policy name. */
    private static boolean persistenceEnabled;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", true);
        U.resolveWorkDirectory(U.defaultWorkDirectory(), "wal", true);
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIME + 3 * 60_000;
    }

    /**
     * @param name Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String name) {
        return new CacheConfiguration()
            .setName(name)
            .setSqlOnheapCacheEnabled(true)
            .setDataRegionName(DATA_REGION_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 2))
            .setQueryEntities(Collections.singleton(
                new QueryEntity(Integer.class, Value.class)));
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(persistenceEnabled)
                    .setMaxSize(SIZE)
                    .setInitialSize(SIZE)
                    .setPageEvictionMode(persistenceEnabled ? DataPageEvictionMode.DISABLED
                        : DataPageEvictionMode.RANDOM_LRU)
                    .setName(DATA_REGION_NAME)));

        return cfg;
    }

    /**
     */
    private void checkRowCacheOnPageEviction() {
        grid().getOrCreateCache(cacheConfiguration(CACHE_NAME));

        int grpId = grid().cachex(CACHE_NAME).context().groupId();

        assertEquals(grpId, grid().cachex(CACHE_NAME).context().groupId());

        for (int i = 0; i < ENTRIES; ++i)
            grid().cache(CACHE_NAME).put(i, new Value(i));

        H2RowCache rowCache = rowCache(grid()).forGroup(grpId);

        fillRowCache(CACHE_NAME);

        assertNotNull(rowCache);

        int rowCacheSizeBeforeEvict = rowCache.size();

        for (int i = ENTRIES; i < 2 * ENTRIES; ++i)
            grid().cache(CACHE_NAME).put(i, new Value(i));

        assertTrue("rowCache size before evictions: " + rowCacheSizeBeforeEvict +
                ", after evictions: " + rowCache.size(),
            rowCacheSizeBeforeEvict > rowCache.size());
    }

    /**
     * @throws Exception On error.
     */
    public void testEvictPagesWithDiskStorage() throws Exception {
        persistenceEnabled = true;

        startGrid();

        grid().active(true);

        checkRowCacheOnPageEviction();
    }

    /**
     * @throws Exception On error.
     */
    public void testEvcitPagesWithoutDiskStorage() throws Exception {
        persistenceEnabled = false;

        startGrid();

        checkRowCacheOnPageEviction();
    }

    /**
     * Test invalid. One cache.get touches more then 5 pages.
     * @throws Exception On error.
     */
    public void _testTouchPageOnRead() throws Exception {
        persistenceEnabled = true;

        startGrid();

        grid().active(true);

        grid().getOrCreateCache(cacheConfiguration(CACHE_NAME));

        int grpId = grid().cachex(CACHE_NAME).context().groupId();

        assertEquals(grpId, grid().cachex(CACHE_NAME).context().groupId());

        try (IgniteDataStreamer<Integer, Value> stream  = grid().dataStreamer(CACHE_NAME)) {
            for (int i = 0; i < ENTRIES * 10; ++i)
                stream.addData(i, new Value(i));
        }

        H2RowCache rowCache = rowCache(grid()).forGroup(grpId);

        assertNotNull(rowCache);

        int touchKey = RND.nextInt(ENTRIES);
        long touchLink = getLinkForKey(rowCache, touchKey);

        long endTime = System.currentTimeMillis() + TEST_TIME;

        int key = 0;

        long iter = 0;
        while (System.currentTimeMillis() < endTime) {
            // Touch hot key
            grid().cache(CACHE_NAME).query(new SqlQuery(Value.class, "_key = " + touchKey)).getAll();

            // Touch other
            grid().cache(CACHE_NAME).get(key);

            // Check rows cache (and touch)
            assertNotNull ("Iter=" + iter, rowCache.get(touchLink));

            key++;

            if (key > ENTRIES * 10)
                key = 0;

            iter++;
        }
    }

    /**
     * @param rowCache Row cache.
     * @param key Key to find.
     * @return Row's link.
     */
    private long getLinkForKey(H2RowCache rowCache, int key) {
        // Touch key.
        grid().cache(CACHE_NAME).query(new SqlQuery(Value.class, "_key = " + key)).getAll();

        ConcurrentHashMap<Long, GridH2KeyValueRowOnheap> rowsMap = GridTestUtils.getFieldValue(rowCache, "rows");

        for (Map.Entry<Long, GridH2KeyValueRowOnheap> e : rowsMap.entrySet()) {
            GridH2KeyValueRowOnheap val = e.getValue();

            if ((Integer)val.key().value(null, false) == key)
                return e.getKey();
        }

        fail("Row cache doesn't contain key [key=" + key + ']');

        return -1;
    }

    /**
     * @param ig Ignite node.
     * @return H2RowCacheRegistry for checks.
     */
    private H2RowCacheRegistry rowCache(IgniteEx ig) {
        IgniteH2Indexing indexing = (IgniteH2Indexing)ig.context().query().getIndexing();

        return GridTestUtils.getFieldValue(indexing, "rowCache");
    }

    /**
     * @param name Cache name.
     */
    @SuppressWarnings("unchecked")
    private void fillRowCache(String name) {
        for (int i = 0; i < ENTRIES; ++i)
            grid().cache(name).query(new SqlQuery(Value.class, "_key = " + i)).getAll();
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
        private byte bytes[] = new byte[1024];

        /**
         * @param k Key.
         */
        Value(int k) {
            lVal = k;
            RND.nextBytes(bytes);
        }
    }
}
