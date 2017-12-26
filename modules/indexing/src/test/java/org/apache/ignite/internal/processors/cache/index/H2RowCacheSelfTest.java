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
import java.util.Random;
import javax.cache.Cache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.h2.H2RowCacheRegistry;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests H2RowCacheRegistry
 */
public class H2RowCacheSelfTest extends GridCommonAbstractTest {
    /** Keys count. */
    private static final int ENTRIES = 10_000;

    /** Rows count. */
    private static final int ROWS_COUNT = ENTRIES / 100;

    /** Random generator. */
    private static final Random RND = new Random(System.currentTimeMillis());

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid(0).destroyCaches(new ArrayList<>(grid(0).cacheNames()));

        super.afterTest();
    }

    /**
     * @param name Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String name) {
        return new CacheConfiguration()
            .setName(name)
            .setSqlOnheapCacheEnabled(true)
            .setGroupName("group")
            .setQueryEntities(Collections.singleton(
                new QueryEntity(Integer.class, Value.class)));
    }

    /**
     */
    public void testDestroyCache() {
        final String cacheName0 = "cache0";
        final String cacheName1 = "cache1";

        grid(0).getOrCreateCache(cacheConfiguration(cacheName0));
        grid(0).getOrCreateCache(cacheConfiguration(cacheName1));

        int grpId = grid(0).cachex(cacheName0).context().groupId();

        assertEquals(grpId, grid(0).cachex(cacheName1).context().groupId());

        fillCache(cacheName0);
        fillCache(cacheName1);

        H2RowCacheRegistry rowCache0 = rowCache(grid(0));
        H2RowCacheRegistry rowCache1 = rowCache(grid(1));

        fillRowCache(cacheName0, ROWS_COUNT);

        assertNotNull(rowCache0.forGroup(grpId));
        assertNotNull(rowCache1.forGroup(grpId));

        // On both nodes
        int rowCacheSize = rowCache0.forGroup(grpId).size() + rowCache1.forGroup(grpId).size();

        assertTrue("Cache size: " + rowCacheSize,
            ROWS_COUNT < rowCacheSize);

        fillRowCache(cacheName1, ROWS_COUNT);

        rowCacheSize = rowCache0.forGroup(grpId).size() + rowCache1.forGroup(grpId).size();

        assertTrue("Cache size: " + rowCacheSize,
             2 * ROWS_COUNT < rowCacheSize);

        grid(0).destroyCache(cacheName0);

        assertNotNull(rowCache0.forGroup(grpId));
        assertNotNull(rowCache1.forGroup(grpId));

        // On both nodes
        rowCacheSize = rowCache0.forGroup(grpId).size() + rowCache1.forGroup(grpId).size();

        assertTrue("Cache size: " + rowCacheSize,
            ROWS_COUNT < rowCacheSize);

        grid(0).destroyCache(cacheName1);

        assertNull(rowCache0.forGroup(grpId));
        assertNull(rowCache1.forGroup(grpId));
    }

    /**
     */
    @SuppressWarnings("unchecked")
    public void testDeleteEntry() {
        final String cacheName = "cache";

        grid(0).getOrCreateCache(cacheConfiguration(cacheName));

        int grpId = grid(0).cachex(cacheName).context().groupId();

        assertEquals(grpId, grid(0).cachex(cacheName).context().groupId());

        fillCache(cacheName);

        H2RowCacheRegistry rowCache0 = rowCache(grid(0));
        H2RowCacheRegistry rowCache1 = rowCache(grid(1));

        fillRowCache(cacheName, ROWS_COUNT);

        assertNotNull(rowCache0.forGroup(grpId));
        assertNotNull(rowCache1.forGroup(grpId));

        int key = RND.nextInt(ENTRIES);

        grid(0).cache(cacheName)
            .query(new SqlQuery(Value.class, "_key = " + key)).getAll();

        // On both nodes
        int rowCacheSize = rowCache0.forGroup(grpId).size() + rowCache1.forGroup(grpId).size();

        grid(0).cache(cacheName).remove(key);

        int rowCacheSizeAfterUpdate = rowCache0.forGroup(grpId).size() + rowCache1.forGroup(grpId).size();

        assertEquals(rowCacheSize - 1, rowCacheSizeAfterUpdate);
    }

    /**
     */
    @SuppressWarnings("unchecked")
    public void testUpdateEntry() {
        final String cacheName = "cache";

        grid(0).getOrCreateCache(cacheConfiguration(cacheName));

        int grpId = grid(0).cachex(cacheName).context().groupId();

        assertEquals(grpId, grid(0).cachex(cacheName).context().groupId());

        fillCache(cacheName);

        H2RowCacheRegistry rowCache0 = rowCache(grid(0));
        H2RowCacheRegistry rowCache1 = rowCache(grid(1));

        fillRowCache(cacheName, ROWS_COUNT);

        assertNotNull(rowCache0.forGroup(grpId));
        assertNotNull(rowCache1.forGroup(grpId));

        int key = RND.nextInt(ENTRIES);

        grid(0).cache(cacheName)
            .query(new SqlQuery(Value.class, "_key = " + key)).getAll();

        // On both nodes
        int rowCacheSize = rowCache0.forGroup(grpId).size() + rowCache1.forGroup(grpId).size();

        grid(0).cache(cacheName).put(key, new Value(key + 1));

        int rowCacheSizeAfterUpdate = rowCache0.forGroup(grpId).size() + rowCache1.forGroup(grpId).size();

        assertEquals(rowCacheSize - 1, rowCacheSizeAfterUpdate);

        List<Cache.Entry<Integer, Value>> res = grid(0).<Integer, Value>cache(cacheName)
            .query(new SqlQuery(Value.class, "_key = " + key)).getAll();

        assertEquals(1, res.size());
        assertEquals(key + 1, (int)res.get(0).getValue().lVal);

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
    private void fillCache(String name) {
        try(IgniteDataStreamer<Integer, Value> streamer = grid(0).dataStreamer(name)) {

            for (int i = 0; i < ENTRIES; ++i)
                streamer.addData(i, new Value(i));
        }
    }

    /**
     * @param name Cache name.
     * @param cnt Row count.
     */
    @SuppressWarnings("unchecked")
    private void fillRowCache(String name, int cnt) {
        int begIdx = RND.nextInt(ENTRIES - cnt);

        for (int i = begIdx; i < begIdx + cnt; ++i) {
            int resSize = grid(0).cache(name)
                .query(new SqlQuery(Value.class, "_key = " + i)).getAll().size();

            assertEquals(1, resSize);
        }
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
