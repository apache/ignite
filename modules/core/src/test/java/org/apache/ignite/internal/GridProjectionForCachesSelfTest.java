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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Tests for {@link ClusterGroup#forCacheNodes(String)} method.
 */
public class GridProjectionForCachesSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        List<CacheConfiguration> ccfgs = new ArrayList<>();

        if (igniteInstanceName.equals(getTestIgniteInstanceName(0)))
            ccfgs.add(cacheConfiguration(DEFAULT_CACHE_NAME, new AttributeFilter(getTestIgniteInstanceName(0)), false));
        else if (igniteInstanceName.equals(getTestIgniteInstanceName(2)) ||
            igniteInstanceName.equals(getTestIgniteInstanceName(3)))
            ccfgs.add(cacheConfiguration(CACHE_NAME, new AttributeFilter(getTestIgniteInstanceName(2),
                getTestIgniteInstanceName(3)), true));

        cfg.setCacheConfiguration(ccfgs.toArray(new CacheConfiguration[ccfgs.size()]));

        return cfg;
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(
        @NotNull String cacheName,
        IgnitePredicate<ClusterNode> nodeFilter,
        boolean nearEnabled
    ) {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setName(cacheName);
        cfg.setCacheMode(PARTITIONED);

        if (nearEnabled)
            cfg.setNearConfiguration(new NearCacheConfiguration());

        cfg.setNodeFilter(nodeFilter);

        cfg.setBackups(1);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 0; i < 5; i++)
            startGrid(i);

        grid(1).createNearCache(CACHE_NAME, new NearCacheConfiguration());

        grid(2).cache(DEFAULT_CACHE_NAME);
        grid(3).cache(DEFAULT_CACHE_NAME);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ignite = grid(0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testProjectionForDefaultCache() throws Exception {
        final ClusterGroup prj = ignite.cluster().forCacheNodes(DEFAULT_CACHE_NAME);

        assertNotNull(prj);

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return prj.nodes().size() == 3;
            }
        }, 5000);

        assertEquals(3, prj.nodes().size());

        assertTrue(prj.nodes().contains(grid(0).localNode()));
        assertFalse(prj.nodes().contains(grid(1).localNode()));
        assertTrue(prj.nodes().contains(grid(2).localNode()));
        assertTrue(prj.nodes().contains(grid(3).localNode()));
        assertFalse(prj.nodes().contains(grid(4).localNode()));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testProjectionForNamedCache() throws Exception {
        final ClusterGroup prj = ignite.cluster().forCacheNodes(CACHE_NAME);

        assertNotNull(prj);

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return prj.nodes().size() == 3;
            }
        }, 5000);

        assertEquals("Invalid projection: " + prj.nodes(), 3, prj.nodes().size());
        assert !prj.nodes().contains(grid(0).localNode());
        assert prj.nodes().contains(grid(1).localNode());
        assert prj.nodes().contains(grid(2).localNode());
        assert prj.nodes().contains(grid(3).localNode());
        assert !prj.nodes().contains(grid(4).localNode());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testProjectionForDataCaches() throws Exception {
        ClusterGroup prj = ignite.cluster().forDataNodes(DEFAULT_CACHE_NAME);

        assert prj != null;
        assert prj.nodes().size() == 1;
        assert prj.nodes().contains(grid(0).localNode());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testProjectionForClientCaches() throws Exception {
        ClusterGroup prj = ignite.cluster().forClientNodes(CACHE_NAME);

        assert prj != null;
        assertEquals("Invalid projection: " + prj.nodes(), 1, prj.nodes().size());
        assert prj.nodes().contains(grid(1).localNode());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testProjectionForWrongCacheName() throws Exception {
        ClusterGroup prj = ignite.cluster().forCacheNodes("wrong");

        assert prj != null;
        assert prj.nodes().isEmpty();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
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

    /**
     *
     */
    private static class AttributeFilter implements IgnitePredicate<ClusterNode> {
        /** */
        private String[] attrs;

        /**
         * @param attrs Attribute values.
         */
        private AttributeFilter(String... attrs) {
            this.attrs = attrs;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            String igniteInstanceName = node.attribute(IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME);

            for (String attr : attrs) {
                if (F.eq(attr, igniteInstanceName))
                    return true;
            }

            return false;
        }
    }
}
