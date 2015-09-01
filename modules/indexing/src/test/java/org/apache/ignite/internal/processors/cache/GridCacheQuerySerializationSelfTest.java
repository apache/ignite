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
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests for cache query results serialization.
 */
public class GridCacheQuerySerializationSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int GRID_CNT = 2;

    /** */
    private static final String CACHE_NAME = "A";

    /** */
    private static final CacheMode CACHE_MODE = PARTITIONED;

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

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName(CACHE_NAME);
        cacheCfg.setCacheMode(CACHE_MODE);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setIndexedTypes(Integer.class, GridCacheQueryTestValue.class);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /**
     * @return Test value.
     */
    private GridCacheQueryTestValue value(String f1, int f2, long f3) {
        GridCacheQueryTestValue val = new GridCacheQueryTestValue();

        val.setField1(f1);
        val.setField2(f2);
        val.setField3(f3);

        return val;
    }

    /**
     * Test that query result could be returned from remote node.
     *
     * @throws Exception In case of error.
     */
    public void testSerialization() throws Exception {
        IgniteEx g0 = grid(0);

        IgniteCache<Integer, GridCacheQueryTestValue> c0 = g0.cache(CACHE_NAME);
        c0.put(1, value("A", 1, 1));
        c0.put(2, value("B", 2, 2));

        IgniteEx g1 = grid(1);
        IgniteCache<Integer, GridCacheQueryTestValue> c1 = g1.cache(CACHE_NAME);
        c1.put(3, value("C", 3, 3));
        c1.put(4, value("D", 4, 4));

        List<Cache.Entry<Integer, GridCacheQueryTestValue>> qryRes =
            g0.compute(g0.cluster().forNode(g1.localNode())).withNoFailover().call(new QueryCallable());

        assert !qryRes.isEmpty();

        info(">>>> Query result:");

        for (Cache.Entry<Integer, GridCacheQueryTestValue> entry : qryRes)
            info(">>>>>>>" + entry.getKey() + " " + entry.getValue().getField1());
    }

    /** */
    private static class QueryCallable implements IgniteCallable<List<Cache.Entry<Integer, GridCacheQueryTestValue>>> {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public List<Cache.Entry<Integer, GridCacheQueryTestValue>> call() throws Exception {
            IgniteCache<Integer, GridCacheQueryTestValue> c = ignite.cache(CACHE_NAME);

            String sqlStr = "FROM GridCacheQueryTestValue WHERE fieldname = ?";
            SqlQuery<Integer, GridCacheQueryTestValue> sql = new SqlQuery<>(GridCacheQueryTestValue.class, sqlStr);
            sql.setArgs("C");

            return c.query(sql.setSql(sqlStr)).getAll();
        }
    }
}