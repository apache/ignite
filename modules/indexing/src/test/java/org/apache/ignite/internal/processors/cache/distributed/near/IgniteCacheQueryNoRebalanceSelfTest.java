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

package org.apache.ignite.internal.processors.cache.distributed.near;

import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test added to check for https://issues.apache.org/jira/browse/IGNITE-3326.
 */
@RunWith(JUnit4.class)
public class IgniteCacheQueryNoRebalanceSelfTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /**
     *
     */
    public IgniteCacheQueryNoRebalanceSelfTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);
        ccfg.setBackups(0);
        ccfg.setIndexedTypes(Integer.class, Integer.class);
        ccfg.setRebalanceMode(CacheRebalanceMode.NONE);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * Tests correct query execution with disabled re-balancing.
     */
    @Test
    public void testQueryNoRebalance() {
        IgniteCache<Object, Object> cache = grid().cache(DEFAULT_CACHE_NAME);

        cache.put(1, 1);

        QueryCursor<Cache.Entry<Integer, Integer>> qry =
            cache.query(new SqlQuery<Integer, Integer>(Integer.class, "_key >= 0"));

        assertEquals("Bad results count", 1, qry.getAll().size());
    }
}
