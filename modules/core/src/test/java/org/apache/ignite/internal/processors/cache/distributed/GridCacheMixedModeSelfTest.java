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

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests cache puts in mixed mode.
 *
 * TODO IGNITE-10345: Remove test in ignite 3.0.
 */
public class GridCacheMixedModeSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setForceServerMode(true);

        cfg.setCacheConfiguration(cacheConfiguration(igniteInstanceName));

        return cfg;
    }

    /**
     * @param igniteInstanceName Ignite instance name.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String igniteInstanceName) {
        CacheConfiguration cfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        cfg.setCacheMode(CacheMode.PARTITIONED);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(3);

        startClientGrid(3);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBasicOps() throws Exception {
        IgniteCache<Object, Object> cache = grid(3).cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 1000; i++)
            cache.put(i, i);

        for (int i = 0; i < 1000; i++)
            assertEquals(i, cache.get(i));

        for (int i = 0; i < 1000; i++)
            assertEquals(i, cache.getAndRemove(i));

        for (int i = 0; i < 1000; i++)
            assertNull(cache.get(i));
    }
}
