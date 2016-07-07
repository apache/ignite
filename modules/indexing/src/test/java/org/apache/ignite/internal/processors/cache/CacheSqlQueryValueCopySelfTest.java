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
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests modification of values returned by query iterators with enabled copy on read.
 */
public class CacheSqlQueryValueCopySelfTest extends GridCommonAbstractTest {
    /** */
    public CacheSqlQueryValueCopySelfTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration configuration = super.getConfiguration(gridName);

        if ("client".equals(configuration.getGridName()))
            configuration.setClientMode(true);

        ((TcpDiscoverySpi)configuration.getDiscoverySpi()).setIpFinder(LOCAL_IP_FINDER);

        CacheConfiguration<Integer, Value> cc = new CacheConfiguration<>();
        cc.setIndexedTypes(Integer.class, Value.class);

        configuration.setCacheConfiguration(cc);

        return configuration;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        IgniteCache<Integer, Value> cache = grid().cache(null);

        cache.removeAll();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * Test two step query from dedicated client.
     */
    public void testTwoStepSqlClientQuery() throws Exception {
        try(Ignite client = startGrid("client")) {

            IgniteCache<Integer, Value> cache = client.cache(null);

            cache.put(0, new Value("before"));

            List<Cache.Entry<Integer, Value>> all = cache.query(
                new SqlQuery<Integer, Value>(Value.class, "select * from Value")).getAll();

            for (Cache.Entry<Integer, Value> entry : all)
                entry.getValue().str = "after";

            check(cache);
        }
    }

    /**
     * Test two step query without local reduce phase.
     */
    public void testTwoStepSkipRdcSqlQuery() {
        IgniteCache<Integer, Value> cache = grid().cache(null);

        cache.put(0, new Value("before"));

        List<Cache.Entry<Integer, Value>> all = cache.query(
            new SqlQuery<Integer, Value>(Value.class, "select * from Value")).getAll();

        for (Cache.Entry<Integer, Value> entry : all)
            entry.getValue().str = "after";

        check(cache);
    }

    /**
     * Test two step query value copy.
     */
    public void testTwoStepRdcSqlQuery() {
        IgniteCache<Integer, Value> cache = grid().cache(null);

        cache.put(0, new Value("before"));

        QueryCursor<List<?>> qry = cache.query(new SqlFieldsQuery("select _val from Value order by _key"));

        for (List<?> entry : qry.getAll())
            ((Value)entry.get(0)).str = "after";

        check(cache);
    }

    /**
     * Tests local sql query.
     */
    public void testLocalSqlQuery() {
        IgniteCache<Integer, Value> cache = grid().cache(null);

        Value before = new Value("before");
        cache.put(0, before);

        SqlQuery<Integer, Value> qry = new SqlQuery<>(Value.class, "select * from Value");
        qry.setLocal(true);

        List<Cache.Entry<Integer, Value>> all = cache.query(qry).getAll();

        for (Cache.Entry<Integer, Value> entry : all)
            entry.getValue().str = "after";

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
        // Value should be not modified by previous assignment.
        for (Cache.Entry<Integer, Value> entry : cache)
            assertEquals("before", entry.getValue().str);
    }
}