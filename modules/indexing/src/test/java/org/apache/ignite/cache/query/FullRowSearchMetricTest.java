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

package org.apache.ignite.cache.query;

import java.util.Collections;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexImpl;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexTree;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.gte;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.in;

/** Tests full index row search metric. */
public class FullRowSearchMetricTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "TEST_CACHE";

    /** */
    private static final String TABLE_NAME = "TEST_TABLE";

    /** */
    private static final String VALUE_TYPE = "TEST_VALUE";

    /** */
    private static final String IDX_NAME = "IDX_NAME";

    /** */
    private IgniteEx ignite;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** */
    @Test
    public void testMetricForSqlQuery() throws Exception {
        IgniteCache<Integer, BinaryObject> cache = createCache(1);

        String target = "common-prefix-target";

        put(cache, 1, target);
        put(cache, 2, "common-prefix-other");

        long before = fullRowSearchCount();

        assertEquals(1, cache.query(new SqlFieldsQuery("SELECT _key FROM " + TABLE_NAME + " WHERE name = ?")
            .setArgs(target)).getAll().size());

        assertTrue(fullRowSearchCount() > before);
    }

    /** */
    @Test
    public void testMetricIsNotIncrementedWhenInlineDataIsSufficient() throws Exception {
        IgniteCache<Integer, BinaryObject> cache = createCache(128);

        String target = "short-value";

        put(cache, 1, target);
        put(cache, 2, "other-value");

        long before = fullRowSearchCount();

        assertEquals(1, cache.query(new SqlFieldsQuery("SELECT _key FROM " + TABLE_NAME + " WHERE name = ?")
            .setArgs(target)).getAll().size());

        assertEquals(before, fullRowSearchCount());
    }

    /** */
    @Test
    public void testMetricForIndexQuery() throws Exception {
        IgniteCache<Integer, BinaryObject> cache = createCache(1);

        String target = "common-prefix-target";

        put(cache, 1, target);
        put(cache, 2, "common-prefix-other");

        long before = fullRowSearchCount();

        IndexQuery<Integer, BinaryObject> qry = new IndexQuery<Integer, BinaryObject>(VALUE_TYPE, IDX_NAME)
            .setCriteria(in("name", Collections.singleton(target)));

        assertEquals(1, cache.withKeepBinary().query(qry).getAll().size());

        assertTrue(fullRowSearchCount() > before);

        before = fullRowSearchCount();

        qry = new IndexQuery<Integer, BinaryObject>(VALUE_TYPE, IDX_NAME)
            .setCriteria(gte("name", target));

        assertEquals(1, cache.withKeepBinary().query(qry).getAll().size());

        assertTrue(fullRowSearchCount() > before);
    }

    /** */
    private IgniteCache<Integer, BinaryObject> createCache(int inlineSize) throws Exception {
        ignite = startGrid();

        QueryEntity qryEntity = new QueryEntity()
            .setTableName(TABLE_NAME)
            .setKeyType(Integer.class.getName())
            .setValueType(VALUE_TYPE)
            .addQueryField("name", String.class.getName(), null);

        CacheConfiguration<Integer, BinaryObject> cacheCfg = new CacheConfiguration<Integer, BinaryObject>(CACHE_NAME)
            .setSqlSchema("PUBLIC")
            .setStatisticsEnabled(true)
            .setQueryEntities(Collections.singleton(qryEntity));

        IgniteCache<Integer, BinaryObject> cache = ignite.createCache(cacheCfg);

        cache.query(new SqlFieldsQuery(
            "CREATE INDEX " + IDX_NAME + " ON " + TABLE_NAME + "(name) INLINE_SIZE " + inlineSize)).getAll();

        return cache;
    }

    /** */
    private void put(IgniteCache<Integer, BinaryObject> cache, int key, String name) {
        cache.put(key, ignite.binary().builder(VALUE_TYPE).setField("name", name).build());
    }

    /** */
    private long fullRowSearchCount() {
        for (ReadOnlyMetricRegistry mreg : ignite.context().metric()) {
            if (mreg.name().startsWith(InlineIndexImpl.INDEX_METRIC_PREFIX) && mreg.name().contains(IDX_NAME)) {
                LongMetric metric = mreg.findMetric(InlineIndexTree.FULL_ROW_SEARCH_COUNT);

                if (metric != null)
                    return metric.value();
            }
        }

        throw new AssertionError("Full row search metric is not found");
    }
}
