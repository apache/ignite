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

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import static org.apache.ignite.cache.CacheMode.*;

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

        if (gridName.equals(getTestGridName(1)))
            cfg.setClientMode(true);

        ccfg1.setNearConfiguration(null);
        ccfg2.setNearConfiguration(null);

        ccfg1.setCacheMode(PARTITIONED);
        ccfg2.setCacheMode(PARTITIONED);

        cfg.setCacheConfiguration(ccfg1, ccfg2);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testHasNearCache() throws Exception {
        IgniteKernal g0 = (IgniteKernal)startGrid(0); // PARTITIONED_ONLY cache.

        AffinityTopologyVersion zero = new AffinityTopologyVersion(0);
        AffinityTopologyVersion one = new AffinityTopologyVersion(1);
        AffinityTopologyVersion two = new AffinityTopologyVersion(2, 2);
        AffinityTopologyVersion three = new AffinityTopologyVersion(3);
        AffinityTopologyVersion four = new AffinityTopologyVersion(4);
        AffinityTopologyVersion five = new AffinityTopologyVersion(5);

        assertFalse(g0.context().discovery().hasNearCache(CACHE_NAME, zero));
        assertFalse(g0.context().discovery().hasNearCache(null, zero));

        assertFalse(g0.context().discovery().hasNearCache(CACHE_NAME, one));
        assertFalse(g0.context().discovery().hasNearCache(null, one));

        IgniteKernal g1 = (IgniteKernal)startGrid(1); // NEAR_ONLY cache.

        grid(1).createNearCache(null, new NearCacheConfiguration());

        grid(1).createNearCache(CACHE_NAME, new NearCacheConfiguration());

        assertFalse(g0.context().discovery().hasNearCache(CACHE_NAME, one));
        assertTrue(g0.context().discovery().hasNearCache(CACHE_NAME, two));
        assertFalse(g0.context().discovery().hasNearCache(null, one));
        assertTrue(g0.context().discovery().hasNearCache(null, two));

        assertTrue(g1.context().discovery().hasNearCache(CACHE_NAME, two));
        assertTrue(g1.context().discovery().hasNearCache(null, two));

        IgniteKernal g2 = (IgniteKernal)startGrid(2); // PARTITIONED_ONLY cache.

        assertFalse(g0.context().discovery().hasNearCache(CACHE_NAME, one));
        assertTrue(g0.context().discovery().hasNearCache(CACHE_NAME, two));
        assertTrue(g0.context().discovery().hasNearCache(CACHE_NAME, three));
        assertFalse(g0.context().discovery().hasNearCache(null, one));
        assertTrue(g0.context().discovery().hasNearCache(null, two));
        assertTrue(g0.context().discovery().hasNearCache(null, three));

        assertTrue(g1.context().discovery().hasNearCache(CACHE_NAME, two));
        assertTrue(g1.context().discovery().hasNearCache(CACHE_NAME, three));
        assertTrue(g1.context().discovery().hasNearCache(null, two));
        assertTrue(g1.context().discovery().hasNearCache(null, three));

        assertTrue(g2.context().discovery().hasNearCache(CACHE_NAME, three));
        assertTrue(g2.context().discovery().hasNearCache(null, three));

        stopGrid(2);

        // Wait all nodes are on version 4.
        for (;;) {
            if (F.forAll(
                Ignition.allGrids(),
                new IgnitePredicate<Ignite>() {
                    @Override public boolean apply(Ignite ignite) {
                        return ignite.cluster().topologyVersion() == 4;
                    }
                }))
                break;

            Thread.sleep(1000);
        }

        assertFalse(g0.context().discovery().hasNearCache(CACHE_NAME, one));
        assertTrue(g0.context().discovery().hasNearCache(CACHE_NAME, two));
        assertTrue(g0.context().discovery().hasNearCache(CACHE_NAME, three));
        assertTrue(g0.context().discovery().hasNearCache(CACHE_NAME, four));
        assertFalse(g0.context().discovery().hasNearCache(null, one));
        assertTrue(g0.context().discovery().hasNearCache(null, two));
        assertTrue(g0.context().discovery().hasNearCache(null, three));
        assertTrue(g0.context().discovery().hasNearCache(null, four));

        assertTrue(g1.context().discovery().hasNearCache(CACHE_NAME, three));
        assertTrue(g1.context().discovery().hasNearCache(CACHE_NAME, four));
        assertTrue(g1.context().discovery().hasNearCache(null, three));
        assertTrue(g1.context().discovery().hasNearCache(null, four));

        stopGrid(1);

        // Wait all nodes are on version 5.
        for (;;) {
            if (F.forAll(
                Ignition.allGrids(),
                new IgnitePredicate<Ignite>() {
                    @Override public boolean apply(Ignite ignite) {
                        return ignite.cluster().topologyVersion() == 5;
                    }
                }))
                break;

            Thread.sleep(1000);
        }

        assertFalse(g0.context().discovery().hasNearCache(CACHE_NAME, one));
        assertTrue(g0.context().discovery().hasNearCache(CACHE_NAME, two));
        assertTrue(g0.context().discovery().hasNearCache(CACHE_NAME, three));
        assertTrue(g0.context().discovery().hasNearCache(CACHE_NAME, four));
        assertFalse(g0.context().discovery().hasNearCache(CACHE_NAME, five));

        assertFalse(g0.context().discovery().hasNearCache(null, one));
        assertTrue(g0.context().discovery().hasNearCache(null, two));
        assertTrue(g0.context().discovery().hasNearCache(null, three));
        assertTrue(g0.context().discovery().hasNearCache(null, four));
        assertFalse(g0.context().discovery().hasNearCache(null, five));
    }
}
