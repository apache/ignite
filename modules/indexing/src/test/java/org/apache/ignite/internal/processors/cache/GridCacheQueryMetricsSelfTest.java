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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 * Tests for cache query metrics.
 */
public class GridCacheQueryMetricsSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int GRID_CNT = 2;

    /** */
    private static final CacheMode CACHE_MODE = REPLICATED;

    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGridsMultiThreaded(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        CacheConfiguration<String, Integer> cacheCfg1 = defaultCacheConfiguration();

        cacheCfg1.setName("A");
        cacheCfg1.setCacheMode(CACHE_MODE);
        cacheCfg1.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg1.setIndexedTypes(String.class, Integer.class);

        CacheConfiguration<String, Integer> cacheCfg2 = defaultCacheConfiguration();

        cacheCfg2.setName("B");
        cacheCfg2.setCacheMode(CACHE_MODE);
        cacheCfg2.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg2.setIndexedTypes(String.class, Integer.class);

        cfg.setCacheConfiguration(cacheCfg1, cacheCfg2);

        return cfg;
    }

    /**
     * Test metrics for SQL queries.
     *
     * @throws Exception In case of error.
     */
    public void testSqlFieldsQueryMetrics() throws Exception {
        IgniteCache<String, Integer> cache = grid(0).context().cache().jcache("A");

        // Execute query.
        SqlFieldsQuery qry = new SqlFieldsQuery("select * from Integer");

        cache.query(qry).getAll();

        QueryMetrics m = cache.queryMetrics();

        assert m != null;

        info("Metrics: " + m);

        assertEquals(1, m.executions());
        assertEquals(0, m.fails());
        assertTrue(m.averageTime() >= 0);
        assertTrue(m.maximumTime() >= 0);
        assertTrue(m.minimumTime() >= 0);

        // Execute again with the same parameters.
        cache.query(qry).getAll();

        m = cache.queryMetrics();

        assert m != null;

        info("Metrics: " + m);

        assertEquals(2, m.executions());
        assertEquals(0, m.fails());
        assertTrue(m.averageTime() >= 0);
        assertTrue(m.maximumTime() >= 0);
        assertTrue(m.minimumTime() >= 0);
    }

    /**
     * Test metrics for Scan queries.
     *
     * @throws Exception In case of error.
     */
    public void testScanQueryMetrics() throws Exception {
        IgniteCache<String, Integer> cache = grid(0).context().cache().jcache("A");

        // Execute query.
        ScanQuery<String, Integer> qry = new ScanQuery<>();

        cache.query(qry).getAll();

        QueryMetrics m = cache.queryMetrics();

        assert m != null;

        info("Metrics: " + m);

        assertEquals(1, m.executions());
        assertEquals(0, m.fails());
        assertTrue(m.averageTime() >= 0);
        assertTrue(m.maximumTime() >= 0);
        assertTrue(m.minimumTime() >= 0);

        // Execute again with the same parameters.
        cache.query(qry).getAll();

        m = cache.queryMetrics();

        assert m != null;

        info("Metrics: " + m);

        assertEquals(2, m.executions());
        assertEquals(0, m.fails());
        assertTrue(m.averageTime() >= 0);
        assertTrue(m.maximumTime() >= 0);
        assertTrue(m.minimumTime() >= 0);
    }

    /**
     * Test metrics for SQL cross cache queries.
     *
     * @throws Exception In case of error.
     */
    public void testSqlCrossCacheQueryMetrics() throws Exception {
        IgniteCache<String, Integer> cache = grid(0).context().cache().jcache("A");

        // Execute query.
        SqlFieldsQuery qry = new SqlFieldsQuery("select * from \"B\".Integer");

        cache.query(qry).getAll();

        QueryMetrics m = cache.queryMetrics();

        assert m != null;

        info("Metrics: " + m);

        assertEquals(1, m.executions());
        assertEquals(0, m.fails());
        assertTrue(m.averageTime() >= 0);
        assertTrue(m.maximumTime() >= 0);
        assertTrue(m.minimumTime() >= 0);

        // Execute again with the same parameters.
        cache.query(qry).getAll();

        m = cache.queryMetrics();

        assert m != null;

        info("Metrics: " + m);

        assertEquals(2, m.executions());
        assertEquals(0, m.fails());
        assertTrue(m.averageTime() >= 0);
        assertTrue(m.maximumTime() >= 0);
        assertTrue(m.minimumTime() >= 0);
    }
}
