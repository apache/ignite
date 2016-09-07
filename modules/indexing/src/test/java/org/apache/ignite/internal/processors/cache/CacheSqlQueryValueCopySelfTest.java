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

import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests modification of values returned by query iterators with enabled copy on read.
 */
public class CacheSqlQueryValueCopySelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int KEYS = 100;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if ("client".equals(cfg.getGridName()))
            cfg.setClientMode(true);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        CacheConfiguration<Integer, Value> cc = new CacheConfiguration<>();

        cc.setCopyOnRead(true);
        cc.setIndexedTypes(Integer.class, Value.class);

        cfg.setCacheConfiguration(cc);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(3);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        IgniteCache<Integer, Value> cache = grid(0).cache(null);

        for (int i = 0; i < KEYS; i++)
            cache.put(i, new Value("before"));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        IgniteCache<Integer, Value> cache = grid(0).cache(null);

        cache.removeAll();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * Tests two step query from dedicated client.
     *
     * @throws Exception If failed.
     */
    public void testTwoStepSqlClientQuery() throws Exception {
        try (Ignite client = startGrid("client")) {
            IgniteCache<Integer, Value> cache = client.cache(null);

            List<Cache.Entry<Integer, Value>> all = cache.query(
                new SqlQuery<Integer, Value>(Value.class, "select * from Value")).getAll();

            assertEquals(KEYS, all.size());

            for (Cache.Entry<Integer, Value> entry : all)
                entry.getValue().str = "after";

            check(cache);

            QueryCursor<List<?>> qry = cache.query(new SqlFieldsQuery("select _val from Value"));

            List<List<?>> all0 = qry.getAll();

            assertEquals(KEYS, all0.size());

            for (List<?> entry : all0)
                ((Value)entry.get(0)).str = "after";

            check(cache);
        }
    }

    /**
     * Test two step query without local reduce phase.
     */
    public void testTwoStepSkipReduceSqlQuery() {
        IgniteCache<Integer, Value> cache = grid(0).cache(null);

        List<Cache.Entry<Integer, Value>> all = cache.query(
            new SqlQuery<Integer, Value>(Value.class, "select * from Value").setPageSize(3)).getAll();

        assertEquals(KEYS, all.size());

        for (Cache.Entry<Integer, Value> entry : all)
            entry.getValue().str = "after";

        check(cache);
    }

    /**
     * Test two step query value copy.
     */
    public void testTwoStepReduceSqlQuery() {
        IgniteCache<Integer, Value> cache = grid(0).cache(null);

        QueryCursor<List<?>> qry = cache.query(new SqlFieldsQuery("select _val from Value order by _key"));

        List<List<?>> all = qry.getAll();

        assertEquals(KEYS, all.size());

        for (List<?> entry : all)
            ((Value)entry.get(0)).str = "after";

        check(cache);
    }

    /**
     * Tests local sql query.
     */
    public void testLocalSqlQuery() {
        IgniteCache<Integer, Value> cache = grid(0).cache(null);

        SqlQuery<Integer, Value> qry = new SqlQuery<>(Value.class.getSimpleName(), "select * from Value");
        qry.setLocal(true);

        List<Cache.Entry<Integer, Value>> all = cache.query(qry).getAll();

        assertFalse(all.isEmpty());

        for (Cache.Entry<Integer, Value> entry : all)
            entry.getValue().str = "after";

        check(cache);
    }

    /**
     * Tests local sql query.
     */
    public void testLocalSqlFieldsQuery() {
        IgniteCache<Integer, Value> cache = grid(0).cache(null);

        QueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery("select _val from Value").setLocal(true));

        List<List<?>> all = cur.getAll();

        assertFalse(all.isEmpty());

        for (List<?> entry : all)
            ((Value)entry.get(0)).str = "after";

        check(cache);
    }

    /** */
    private static class Value {
        /** */
        private String str;

        /**
         * @param str String.
         */
        public Value(String str) {
            this.str = str;
        }
    }

    /**
     * @param cache Cache.
     */
    private void check(IgniteCache<Integer, Value> cache) {
        int cnt = 0;

        // Value should be not modified by previous assignment.
        for (Cache.Entry<Integer, Value> entry : cache) {
            cnt++;

            assertEquals("before", entry.getValue().str);
        }

        assertEquals(KEYS, cnt);
    }
}