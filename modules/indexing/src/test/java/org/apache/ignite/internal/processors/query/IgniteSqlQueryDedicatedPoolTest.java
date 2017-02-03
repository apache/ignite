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

package org.apache.ignite.internal.processors.query;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import java.util.List;

/**
 * Ensures that SQL queries are executed in a dedicated thread pool.
 */
public class IgniteSqlQueryDedicatedPoolTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Name of the cache for test */
    private static final String CACHE_NAME = "query_pool_test";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrid("server");
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi spi = (TcpDiscoverySpi)cfg.getDiscoverySpi();

        spi.setIpFinder(IP_FINDER);

        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>();

        ccfg.setIndexedTypes(Integer.class, Integer.class);
        ccfg.setSqlFunctionClasses(IgniteSqlQueryDedicatedPoolTest.class);
        ccfg.setName(CACHE_NAME);

        cfg.setCacheConfiguration(ccfg);

        if ("client".equals(gridName))
            cfg.setClientMode(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Test that SQL queries are executed in dedicated pool
     */
    public void testSqlQueryUsesDedicatedThreadPool() throws Exception {
        try (Ignite client = startGrid("client")) {
            IgniteCache<Integer, Integer> cache = client.cache(CACHE_NAME);

            QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery("select currentPolicy()"));

            List<List<?>> result = cursor.getAll();

            cursor.close();

            assertEquals(1, result.size());

            Byte plc = (Byte)result.get(0).get(0);

            assert plc != null;
            assert plc == GridIoPolicy.QUERY_POOL;
        }
    }

    /**
     * Custom SQL function to return current thread name from inside query executor
     */
    @SuppressWarnings("unused")
    @QuerySqlFunction(alias = "currentPolicy")
    public static Byte currentPolicy() {
         return GridIoManager.currentPolicy();
    }
}
