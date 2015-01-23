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

package org.apache.ignite.internal;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.discovery.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.apache.ignite.cache.GridCacheMode.*;

/**
 * Tests for {@link org.apache.ignite.cluster.ClusterGroup#forCache(String, String...)} method.
 */
public class GridProjectionForCachesSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDiscoverySpi(discoverySpi());

        if (gridName.equals(getTestGridName(0)))
            cfg.setCacheConfiguration(cacheConfiguration(null));
        else if (gridName.equals(getTestGridName(1)))
            cfg.setCacheConfiguration(cacheConfiguration(CACHE_NAME));
        else if (gridName.equals(getTestGridName(2)) || gridName.equals(getTestGridName(3)))
            cfg.setCacheConfiguration(cacheConfiguration(null), cacheConfiguration(CACHE_NAME));
        else
            cfg.setCacheConfiguration();

        return cfg;
    }

    /**
     * @return Discovery SPI;
     */
    private DiscoverySpi discoverySpi() {
        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(IP_FINDER);

        return spi;
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(@Nullable String cacheName) {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setName(cacheName);
        cfg.setCacheMode(PARTITIONED);
        cfg.setBackups(1);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 0; i < 5; i++)
            startGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ignite = grid(0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testProjectionForDefaultCache() throws Exception {
        ClusterGroup prj = ignite.cluster().forCache(null);

        assert prj != null;
        assert prj.nodes().size() == 3;
        assert prj.nodes().contains(grid(0).localNode());
        assert !prj.nodes().contains(grid(1).localNode());
        assert prj.nodes().contains(grid(2).localNode());
        assert prj.nodes().contains(grid(3).localNode());
        assert !prj.nodes().contains(grid(4).localNode());
    }

    /**
     * @throws Exception If failed.
     */
    public void testProjectionForNamedCache() throws Exception {
        ClusterGroup prj = ignite.cluster().forCache(CACHE_NAME);

        assert prj != null;
        assert prj.nodes().size() == 3;
        assert !prj.nodes().contains(grid(0).localNode());
        assert prj.nodes().contains(grid(1).localNode());
        assert prj.nodes().contains(grid(2).localNode());
        assert prj.nodes().contains(grid(3).localNode());
        assert !prj.nodes().contains(grid(4).localNode());
    }

    /**
     * @throws Exception If failed.
     */
    public void testProjectionForBothCaches() throws Exception {
        ClusterGroup prj = ignite.cluster().forCache(null, CACHE_NAME);

        assert prj != null;
        assert prj.nodes().size() == 2;
        assert !prj.nodes().contains(grid(0).localNode());
        assert !prj.nodes().contains(grid(1).localNode());
        assert prj.nodes().contains(grid(2).localNode());
        assert prj.nodes().contains(grid(3).localNode());
        assert !prj.nodes().contains(grid(4).localNode());
    }

    /**
     * @throws Exception If failed.
     */
    public void testProjectionForWrongCacheName() throws Exception {
        ClusterGroup prj = ignite.cluster().forCache("wrong");

        assert prj != null;
        assert prj.nodes().isEmpty();
    }

    /**
     * @throws Exception If failed.
     */
    public void testProjections() throws Exception {
        ClusterNode locNode = ignite.cluster().localNode();
        UUID locId = locNode.id();

        assertNotNull(locId);

        assertEquals(5, ignite.cluster().nodes().size());

        ClusterGroup prj = ignite.cluster().forLocal();

        assertEquals(1, prj.nodes().size());
        assertEquals(locNode, F.first(prj.nodes()));

        prj = ignite.cluster().forHost(locNode);
        assertEquals(ignite.cluster().nodes().size(), prj.nodes().size());
        assertTrue(ignite.cluster().nodes().containsAll(prj.nodes()));
        try {
            ignite.cluster().forHost(null);
        }
        catch (NullPointerException ignored) {
            // No-op.
        }

        prj = ignite.cluster().forNode(locNode);
        assertEquals(1, prj.nodes().size());

        prj = ignite.cluster().forNode(locNode, locNode);
        assertEquals(1, prj.nodes().size());

        try {
            ignite.cluster().forNode(null);
        }
        catch (NullPointerException ignored) {
            // No-op.
        }

        prj = ignite.cluster().forNodes(F.asList(locNode));
        assertEquals(1, prj.nodes().size());

        prj = ignite.cluster().forNodes(F.asList(locNode, locNode));
        assertEquals(1, prj.nodes().size());

        try {
            ignite.cluster().forNodes(null);
        }
        catch (NullPointerException ignored) {
            // No-op.
        }

        prj = ignite.cluster().forNodeId(locId);
        assertEquals(1, prj.nodes().size());

        prj = ignite.cluster().forNodeId(locId, locId);
        assertEquals(1, prj.nodes().size());

        try {
            ignite.cluster().forNodeId(null);
        }
        catch (NullPointerException ignored) {
            // No-op.
        }

        prj = ignite.cluster().forNodeIds(F.asList(locId));
        assertEquals(1, prj.nodes().size());

        prj = ignite.cluster().forNodeIds(F.asList(locId, locId));
        assertEquals(1, prj.nodes().size());

        try {
            ignite.cluster().forNodeIds(null);
        }
        catch (NullPointerException ignored) {
            // No-op.
        }

        prj = ignite.cluster().forOthers(locNode);

        assertEquals(4, prj.nodes().size());
        assertFalse(prj.nodes().contains(locNode));

        assertEquals(4, ignite.cluster().forRemotes().nodes().size());
        assertTrue(prj.nodes().containsAll(ignite.cluster().forRemotes().nodes()));

        try {
            ignite.cluster().forOthers((ClusterNode)null);
        }
        catch (NullPointerException ignored) {
            // No-op.
        }
    }
}
