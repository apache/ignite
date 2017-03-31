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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.List;

/** Test for SQL min() and max() optimization */
public class IgniteSqlQueryMinMaxTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Name of the cache for test */
    private static final String CACHE_NAME = "min_max";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrid("server1");
        startGrid("server2");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi spi = (TcpDiscoverySpi)cfg.getDiscoverySpi();

        spi.setIpFinder(IP_FINDER);

        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>();
        ccfg.setIndexedTypes(Integer.class, Integer.class, Integer.class, ValueObj.class);
        ccfg.setName(CACHE_NAME);

        cfg.setCacheConfiguration(ccfg);

        if ("client".equals(gridName))
            cfg.setClientMode(true);

        return cfg;
    }

    /** Check min() and max() functions in queries */
    public void testQueryMinMax() throws Exception {
        try (Ignite client = startGrid("client")) {
            IgniteCache<Integer, ValueObj> cache = client.cache(CACHE_NAME);

            int count = 1_000;
            for (int idx = 0; idx < count; ++idx)
                cache.put(idx, new ValueObj(count - idx - 1));

            long start = System.currentTimeMillis();
            QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery("select MIN(_key), MAX(_key) from ValueObj"));
            List<List<?>> result = cursor.getAll();
            assertEquals(1, result.size());
            assertEquals(0, result.get(0).get(0));
            assertEquals(count - 1, result.get(0).get(1));
            if (log.isDebugEnabled())
                log.debug("Elapsed(1): " + (System.currentTimeMillis() - start));

            start = System.currentTimeMillis();
            cursor = cache.query(new SqlFieldsQuery("select MIN(idxVal), MAX(idxVal) from ValueObj"));
            result = cursor.getAll();
            assertEquals(1, result.size());
            assertEquals(0, result.get(0).get(0));
            assertEquals(count - 1, result.get(0).get(1));
            if (log.isDebugEnabled())
                log.debug("Elapsed(2): " + (System.currentTimeMillis() - start));

            start = System.currentTimeMillis();
            cursor = cache.query(new SqlFieldsQuery("select MIN(nonIdxVal), MAX(nonIdxVal) from ValueObj"));
            result = cursor.getAll();
            assertEquals(1, result.size());
            assertEquals(0, result.get(0).get(0));
            assertEquals(count - 1, result.get(0).get(1));
            if (log.isDebugEnabled())
                log.debug("Elapsed(3): " + (System.currentTimeMillis() - start));
        }
    }

    /** Check min() and max() on empty cache */
    public void testQueryMinMaxEmptyCache() throws Exception {
        try (Ignite client = startGrid("client")) {
            IgniteCache<Integer, ValueObj> cache = client.cache(CACHE_NAME);

            QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery("select MIN(idxVal), MAX(idxVal) from ValueObj"));
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
     *
     * This test currently fails due to H2 issue: it selects wrong index.
     */
    public void testMinMaxQueryPlanOnKey() throws Exception {
        try (Ignite client = startGrid("client")) {
            IgniteCache<Integer, ValueObj> cache = client.cache(CACHE_NAME);

            QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery("explain select MIN(_key), MAX(_key) from ValueObj"));
            List<List<?>> result = cursor.getAll();
            assertEquals(2, result.size());
            assertTrue(((String) result.get(0).get(0)).contains("_key_PK"));
            assertTrue(((String) result.get(0).get(0)).contains("direct lookup"));
        }
    }

    /**
     * Check min() and max() over value fields use correct index.
     * Test uses value object cache
     */
    public void testMinMaxQueryPlanOnFields() throws Exception {
        try (Ignite client = startGrid("client")) {
            IgniteCache<Integer, ValueObj> cache = client.cache(CACHE_NAME);

            QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery("explain select MIN(idxVal), MAX(idxVal) from ValueObj"));
            List<List<?>> result = cursor.getAll();
            assertEquals(2, result.size());
            assertTrue(((String)result.get(0).get(0)).contains("idxVal_idx"));
            assertTrue(((String)result.get(0).get(0)).contains("direct lookup"));
        }
    }

    /**
     * Check min() and max() over _key uses correct index
     * Test uses primitive cache
     *
     * This test currently fails due to H2 issue: it selects wrong index.
     */
    public void testSimpleMinMaxQueryPlanOnKey() throws Exception {
        try (Ignite client = startGrid("client")) {
            IgniteCache<Integer, ValueObj> cache = client.cache(CACHE_NAME);

            QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery("explain select MIN(_key), MAX(_key) from Integer"));
            List<List<?>> result = cursor.getAll();
            assertEquals(2, result.size());
            assertTrue(((String)result.get(0).get(0)).contains("_key_PK"));
            assertTrue(((String)result.get(0).get(0)).contains("direct lookup"));
        }
    }

    /**
     * Check min() and max() over _val uses correct index.
     * Test uses primitive cache
     */
    public void testSimpleMinMaxQueryPlanOnValue() throws Exception {
        try (Ignite client = startGrid("client")) {
            IgniteCache<Integer, ValueObj> cache = client.cache(CACHE_NAME);

            QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery("explain select MIN(_val), MAX(_val) from Integer"));
            List<List<?>> result = cursor.getAll();
            assertEquals(2, result.size());
            assertTrue(((String)result.get(0).get(0)).contains("_val_idx"));
            assertTrue(((String)result.get(0).get(0)).contains("direct lookup"));
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

        /** */
        public ValueObj(int v) {
            this.idxVal = v;
            this.nonIdxVal = v;
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
            return idxVal == other.idxVal && nonIdxVal == other.nonIdxVal;
        }
    }
}
