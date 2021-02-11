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

package org.apache.ignite.internal.processors.query.h2;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.junit.Test;

/** Test for SQL min() and max() optimization */
public class IgniteSqlQueryMinMaxTest extends AbstractIndexingCommonTest {
    /** Name of the cache for test */
    private static final String CACHE_NAME = "intCache";

    /** Name of the second test cache */
    private static final String CACHE_NAME_2 = "valCache";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrids(4);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        awaitPartitionMapExchange(true, false, null);

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);
        ccfg.setIndexedTypes(Integer.class, Integer.class);
        ccfg.setName(CACHE_NAME);

        CacheConfiguration<?, ?> ccfg2 = new CacheConfiguration<>(DEFAULT_CACHE_NAME);
        ccfg2.setIndexedTypes(Integer.class, ValueObj.class);
        ccfg2.setName(CACHE_NAME_2);

        cfg.setCacheConfiguration(ccfg, ccfg2);

        return cfg;
    }

    /** Check min() and max() functions in queries */
    @Test
    public void testQueryMinMax() throws Exception {
        try (Ignite client = startClientGrid("client")) {
            IgniteCache<Integer, ValueObj> cache = client.cache(CACHE_NAME_2);

            int count = 1_000;
            for (int idx = 0; idx < count; ++idx)
                cache.put(idx, new ValueObj(count - idx - 1, 0));

            long start = System.currentTimeMillis();
            QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery("select min(_key), max(_key) from ValueObj"));
            List<List<?>> result = cursor.getAll();
            assertEquals(1, result.size());
            assertEquals(0, result.get(0).get(0));
            assertEquals(count - 1, result.get(0).get(1));
            if (log.isDebugEnabled())
                log.debug("Elapsed(1): " + (System.currentTimeMillis() - start));

            start = System.currentTimeMillis();
            cursor = cache.query(new SqlFieldsQuery("select min(idxVal), max(idxVal) from ValueObj"));
            result = cursor.getAll();
            assertEquals(1, result.size());
            assertEquals(0, result.get(0).get(0));
            assertEquals(count - 1, result.get(0).get(1));
            if (log.isDebugEnabled())
                log.debug("Elapsed(2): " + (System.currentTimeMillis() - start));

            start = System.currentTimeMillis();
            cursor = cache.query(new SqlFieldsQuery("select min(nonIdxVal), max(nonIdxVal) from ValueObj"));
            result = cursor.getAll();
            assertEquals(1, result.size());
            assertEquals(0, result.get(0).get(0));
            assertEquals(count - 1, result.get(0).get(1));
            if (log.isDebugEnabled())
                log.debug("Elapsed(3): " + (System.currentTimeMillis() - start));
        }
    }

    /** Check min() and max() on empty cache */
    @Test
    public void testQueryMinMaxEmptyCache() throws Exception {
        try (Ignite client = startClientGrid("client")) {
            IgniteCache<Integer, ValueObj> cache = client.cache(CACHE_NAME_2);

            QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery("select min(idxVal), max(idxVal) from ValueObj"));
            List<List<?>> result = cursor.getAll();
            assertEquals(1, result.size());
            assertEquals(2, result.get(0).size());
            assertNull(result.get(0).get(0));
            assertNull(result.get(0).get(1));
        }
    }

    /**
     * Check min() and max() over _key use correct index
     * Test uses value object cache
     */
    @Test
    public void testMinMaxQueryPlanOnKey() throws Exception {
        try (Ignite client = startClientGrid("client")) {
            IgniteCache<Integer, ValueObj> cache = client.cache(CACHE_NAME_2);

            QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery("explain select min(_key), max(_key) from ValueObj"));
            List<List<?>> result = cursor.getAll();
            assertEquals(2, result.size());
            assertTrue(((String) result.get(0).get(0)).toLowerCase().contains("_key_pk"));
            assertTrue(((String) result.get(0).get(0)).toLowerCase().contains("direct lookup"));
        }
    }

    /**
     * Check min() and max() over value fields use correct index.
     * Test uses value object cache
     */
    @Test
    public void testMinMaxQueryPlanOnFields() throws Exception {
        try (Ignite client = startClientGrid("client")) {
            IgniteCache<Integer, ValueObj> cache = client.cache(CACHE_NAME_2);

            QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery("explain select min(idxVal), max(idxVal) from ValueObj"));
            List<List<?>> result = cursor.getAll();
            assertEquals(2, result.size());
            assertTrue(((String)result.get(0).get(0)).toLowerCase().contains("idxval_idx"));
            assertTrue(((String)result.get(0).get(0)).toLowerCase().contains("direct lookup"));
        }
    }

    /**
     * Check min() and max() over _key uses correct index
     * Test uses primitive cache
     */
    @Test
    public void testSimpleMinMaxQueryPlanOnKey() throws Exception {
        try (Ignite client = startClientGrid("client")) {
            IgniteCache<Integer, Integer> cache = client.cache(CACHE_NAME);

            QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery("explain select min(_key), max(_key) from Integer"));
            List<List<?>> result = cursor.getAll();
            assertEquals(2, result.size());
            String res = ((String)result.get(0).get(0)).toLowerCase();
            assertTrue(res, res.contains("_key_pk"));
            assertTrue(res, res.contains("direct lookup"));
        }
    }

    /**
     * Check min() and max() over _val uses correct index.
     * Test uses primitive cache
     */
    @Test
    public void testSimpleMinMaxQueryPlanOnValue() throws Exception {
        try (Ignite client = startClientGrid("client")) {
            IgniteCache<Integer, Integer> cache = client.cache(CACHE_NAME);

            QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery("explain select min(_val), max(_val) from Integer"));
            List<List<?>> result = cursor.getAll();
            assertEquals(2, result.size());
            assertTrue(((String)result.get(0).get(0)).toLowerCase().contains("_val_idx"));
            assertTrue(((String)result.get(0).get(0)).toLowerCase().contains("direct lookup"));
        }
    }

    /** Check min() and max() over group */
    @Test
    public void testGroupMinMax() throws Exception {
        try (Ignite client = startClientGrid("client")) {
            IgniteCache<Integer, ValueObj> cache = client.cache(CACHE_NAME_2);

            int count = 1_000;
            int groupSize = 100;
            for (int idx = 0; idx < count; ++idx)
                cache.put(idx, new ValueObj(count - idx - 1, groupSize));

            QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery(
                    "select groupVal, min(idxVal), max(idxVal), min(nonIdxVal), max(nonIdxVal) " +
                    " from ValueObj group by groupVal order by groupVal"));
            List<List<?>> result = cursor.getAll();

            assertEquals(count / groupSize, result.size());

            for (int idx = 0; idx < result.size(); ++idx) {
                assertEquals(idx, result.get(idx).get(0));//groupVal
                int min = idx * groupSize;
                int max = (idx + 1) * groupSize - 1;
                assertEquals(min, result.get(idx).get(1));//min(idxVal)
                assertEquals(max, result.get(idx).get(2));//max(idxVal)
                assertEquals(min, result.get(idx).get(3));//min(nonIdxVal)
                assertEquals(max, result.get(idx).get(4));//max(nonIdxVal)
            }
        }
    }

    /** Check min() and max() over group with having clause */
    @Test
    public void testGroupHavingMinMax() throws Exception {
        try (Ignite client = startClientGrid("client")) {
            IgniteCache<Integer, ValueObj> cache = client.cache(CACHE_NAME_2);

            int count = 1_000;
            int groupSize = 100;
            for (int idx = 0; idx < count; ++idx)
                cache.put(idx, new ValueObj(count - idx - 1, groupSize));

            QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery(
                    "select groupVal, min(idxVal), max(idxVal), min(nonIdxVal), max(nonIdxVal) " +
                         "from ValueObj group by groupVal having min(idxVal) = ?" ).setArgs(0));

            List<List<?>> result = cursor.getAll();
            assertEquals(1, result.size());
            assertEquals(0, result.get(0).get(0));//groupVal
            assertEquals(0, result.get(0).get(1));//min(idxVal)
            assertEquals(groupSize - 1, result.get(0).get(2));//max(idxVal)
            assertEquals(0, result.get(0).get(3));//min(nonIdxVal)
            assertEquals(groupSize - 1, result.get(0).get(4));//max(nonIdxVal)

            cursor = cache.query(new SqlFieldsQuery(
                    "select groupVal, min(idxVal), max(idxVal), min(nonIdxVal), max(nonIdxVal) " +
                         "from ValueObj group by groupVal having max(idxVal) = ?" ).setArgs(count - 1));

            result = cursor.getAll();
            assertEquals(1, result.size());
            assertEquals((count - 1) / groupSize, result.get(0).get(0));//groupVal
            assertEquals(count - groupSize, result.get(0).get(1));//min(idxVal)
            assertEquals(count - 1, result.get(0).get(2));//max(idxVal)
            assertEquals(count - groupSize, result.get(0).get(3));//min(nonIdxVal)
            assertEquals(count - 1, result.get(0).get(4));//max(nonIdxVal)
        }
    }

    /** Check min() and max() over group with joins */
    @Test
    public void testJoinGroupMinMax() throws Exception {
        try (Ignite client = startClientGrid("client")) {
            IgniteCache<Integer, Integer> cache = client.cache(CACHE_NAME);
            IgniteCache<Integer, ValueObj> cache2 = client.cache(CACHE_NAME_2);

            int count = 1_000;
            int groupSize = 100;
            for (int idx = 0; idx < count; ++idx) {
                cache.put(idx, idx);
                cache2.put(idx, new ValueObj(count - idx - 1, groupSize));
            }

            //join a.key = b.key, collocated
            QueryCursor<List<?>> cursor = cache.query(
                    new SqlFieldsQuery("select b.groupVal, min(a._key), max(a._key), min(a._val), max(a._val), " +
                            "min(b._key), max(b._key), min(b.idxVal), max(b.idxVal), min(b.nonIdxVal), max(b.nonIdxVal) " +
                            "from \"intCache\".Integer a, \"valCache\".ValueObj b where a._key = b._key " +
                            "group by b.groupVal order by b.groupVal"));

            List<List<?>> result = cursor.getAll();
            assertEquals(count / groupSize, result.size());
            for (int idx = 0; idx < result.size(); ++idx) {
                assertEquals(idx, result.get(idx).get(0));
                int min = idx * groupSize;
                int max = (idx + 1) * groupSize - 1;
                int revMin = count - max - 1;
                int revMax = count - min - 1;

                assertEquals(revMin, result.get(idx).get(1));//min(a._key)
                assertEquals(revMax, result.get(idx).get(2));//max(a._key)
                assertEquals(revMin, result.get(idx).get(3));//min(a._val)
                assertEquals(revMax, result.get(idx).get(4));//max(a._val)
                assertEquals(revMin, result.get(idx).get(5));//min(b._key)
                assertEquals(revMax, result.get(idx).get(6));//max(b_key)
                assertEquals(min, result.get(idx).get(7));//min(b.idxVal)
                assertEquals(max, result.get(idx).get(8));//max(b.idxVal),
                assertEquals(min, result.get(idx).get(9));//min(b.nonIdxVal)
                assertEquals(max, result.get(idx).get(10));//max(b.nonIdxVal)
            }

            //join a.key = b.val, non-collocated
            cursor = cache.query(
                    new SqlFieldsQuery("select b.groupVal, min(a._key), max(a._key), min(a._val), max(a._val), " +
                            "min(b._key), max(b._key), min(b.idxVal), max(b.idxVal), min(b.nonIdxVal), max(b.nonIdxVal) " +
                            "from \"intCache\".Integer a, \"valCache\".ValueObj b where a._key = b.idxVal " +
                            "group by b.groupVal order by b.groupVal")
                            .setDistributedJoins(true));

            result = cursor.getAll();

            assertEquals(count / groupSize, result.size());
            for (int idx = 0; idx < result.size(); ++idx) {
                assertEquals(idx, result.get(idx).get(0));
                int min = idx * groupSize;
                int max = (idx + 1) * groupSize - 1;
                int revMin = count - max - 1;
                int revMax = count - min - 1;

                assertEquals(min, result.get(idx).get(1));//min(a._key)
                assertEquals(max, result.get(idx).get(2));//max(a._key)
                assertEquals(min, result.get(idx).get(3));//min(a._val)
                assertEquals(max, result.get(idx).get(4));//max(a._val)
                assertEquals(revMin, result.get(idx).get(5));//min(b._key)
                assertEquals(revMax, result.get(idx).get(6));//max(b_key)
                assertEquals(min, result.get(idx).get(7));//min(b.idxVal)
                assertEquals(max, result.get(idx).get(8));//max(b.idxVal),
                assertEquals(min, result.get(idx).get(9));//min(b.nonIdxVal)
                assertEquals(max, result.get(idx).get(10));//max(b.nonIdxVal)
            }
        }
    }

    /** Value object for test cache */
    public class ValueObj {
        /** */
        @QuerySqlField(index = true)
        private final int idxVal;

        /** */
        @QuerySqlField
        private final int nonIdxVal;

        /** used for grouping */
        @QuerySqlField
        private final int groupVal;

        /** */
        public ValueObj(int v, int g) {
            this.idxVal = v;
            this.nonIdxVal = v;
            this.groupVal = (g == 0) ? v : v / g;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return idxVal;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof ValueObj))
                return false;

            ValueObj other = (ValueObj)o;
            return idxVal == other.idxVal &&
                   nonIdxVal == other.nonIdxVal &&
                   groupVal == other.groupVal;
        }
    }
}
