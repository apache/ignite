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

package org.apache.ignite.internal.managers.discovery;

import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import static org.apache.ignite.cache.GridCacheMode.*;
import static org.apache.ignite.cache.GridCacheDistributionMode.*;

/**
 *
 */
public class GridDiscoveryManagerSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("IfMayBeConditional")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disc = new TcpDiscoverySpi();

        disc.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disc);

        CacheConfiguration ccfg1 = defaultCacheConfiguration();

        ccfg1.setName(CACHE_NAME);

        CacheConfiguration ccfg2 = defaultCacheConfiguration();

        ccfg2.setName(null);

        GridCacheDistributionMode distrMode;

        if (gridName.equals(getTestGridName(1)))
            distrMode = NEAR_ONLY;
        else if (gridName.equals(getTestGridName(2)))
            distrMode = NEAR_PARTITIONED;
        else
            distrMode = PARTITIONED_ONLY;

        ccfg1.setCacheMode(PARTITIONED);
        ccfg2.setCacheMode(PARTITIONED);

        ccfg1.setDistributionMode(distrMode);
        ccfg2.setDistributionMode(distrMode);

        cfg.setCacheConfiguration(ccfg1, ccfg2);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testHasNearCache() throws Exception {
        GridKernal g0 = (GridKernal)startGrid(0); // PARTITIONED_ONLY cache.

        assertFalse(g0.context().discovery().hasNearCache(CACHE_NAME, 0));
        assertFalse(g0.context().discovery().hasNearCache(null, 0));

        assertFalse(g0.context().discovery().hasNearCache(CACHE_NAME, 1));
        assertFalse(g0.context().discovery().hasNearCache(null, 1));

        GridKernal g1 = (GridKernal)startGrid(1); // NEAR_ONLY cache.

        assertFalse(g0.context().discovery().hasNearCache(CACHE_NAME, 1));
        assertTrue(g0.context().discovery().hasNearCache(CACHE_NAME, 2));
        assertFalse(g0.context().discovery().hasNearCache(null, 1));
        assertTrue(g0.context().discovery().hasNearCache(null, 2));

        assertTrue(g1.context().discovery().hasNearCache(CACHE_NAME, 2));
        assertTrue(g1.context().discovery().hasNearCache(null, 2));

        GridKernal g2 = (GridKernal)startGrid(2); // NEAR_PARTITIONED cache.

        assertFalse(g0.context().discovery().hasNearCache(CACHE_NAME, 1));
        assertTrue(g0.context().discovery().hasNearCache(CACHE_NAME, 2));
        assertTrue(g0.context().discovery().hasNearCache(CACHE_NAME, 3));
        assertFalse(g0.context().discovery().hasNearCache(null, 1));
        assertTrue(g0.context().discovery().hasNearCache(null, 2));
        assertTrue(g0.context().discovery().hasNearCache(null, 3));

        assertTrue(g1.context().discovery().hasNearCache(CACHE_NAME, 2));
        assertTrue(g1.context().discovery().hasNearCache(CACHE_NAME, 3));
        assertTrue(g1.context().discovery().hasNearCache(null, 2));
        assertTrue(g1.context().discovery().hasNearCache(null, 3));

        assertTrue(g2.context().discovery().hasNearCache(CACHE_NAME, 3));
        assertTrue(g2.context().discovery().hasNearCache(null, 3));

        stopGrid(1);

        assertFalse(g0.context().discovery().hasNearCache(CACHE_NAME, 1));
        assertTrue(g0.context().discovery().hasNearCache(CACHE_NAME, 2));
        assertTrue(g0.context().discovery().hasNearCache(CACHE_NAME, 3));
        assertTrue(g0.context().discovery().hasNearCache(CACHE_NAME, 4));
        assertFalse(g0.context().discovery().hasNearCache(null, 1));
        assertTrue(g0.context().discovery().hasNearCache(null, 2));
        assertTrue(g0.context().discovery().hasNearCache(null, 3));
        assertTrue(g0.context().discovery().hasNearCache(null, 4));

        assertTrue(g2.context().discovery().hasNearCache(CACHE_NAME, 3));
        assertTrue(g2.context().discovery().hasNearCache(CACHE_NAME, 4));
        assertTrue(g2.context().discovery().hasNearCache(null, 3));
        assertTrue(g2.context().discovery().hasNearCache(null, 4));

        stopGrid(2);

        assertFalse(g0.context().discovery().hasNearCache(CACHE_NAME, 1));
        assertTrue(g0.context().discovery().hasNearCache(CACHE_NAME, 2));
        assertTrue(g0.context().discovery().hasNearCache(CACHE_NAME, 3));
        assertTrue(g0.context().discovery().hasNearCache(CACHE_NAME, 4));
        assertFalse(g0.context().discovery().hasNearCache(CACHE_NAME, 5));

        assertFalse(g0.context().discovery().hasNearCache(null, 1));
        assertTrue(g0.context().discovery().hasNearCache(null, 2));
        assertTrue(g0.context().discovery().hasNearCache(null, 3));
        assertTrue(g0.context().discovery().hasNearCache(null, 4));
        assertFalse(g0.context().discovery().hasNearCache(null, 5));
    }
}
