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

import java.util.Arrays;
import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TopologyValidator;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.CacheNameResource;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;

/**
 * Tests complex scenario with topology validator. Grid is split between to data centers, defined by attribute {@link
 * #DC_NODE_ATTR}. If only nodes from single DC are left in topology, grid is moved into inoperative state until special
 * activator node'll enter a topology, enabling grid operations.
 */
public class IgniteTopologyValidatorGridSplitCacheTest extends GridCommonAbstractTest {
    /** */
    private static final String DC_NODE_ATTR = "dc";

    /** */
    private static final String ACTIVATOR_NODE_ATTR = "split.resolved";

    /** */
    private static final int GRID_CNT = 8;

    /** */
    private static final int CACHES_CNT = 100;

    /** */
    private static final int RESOLVER_GRID_IDX = GRID_CNT;

    /** */
    private static final int CONFIGLESS_GRID_IDX = GRID_CNT + 1;

    /** */
    private boolean useCacheGrp = false;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        int idx = getTestIgniteInstanceIndex(gridName);

        cfg.setUserAttributes(F.asMap(DC_NODE_ATTR, idx % 2));

        if (idx != CONFIGLESS_GRID_IDX) {
            if (idx == RESOLVER_GRID_IDX) {
                cfg.setClientMode(true);

                cfg.setUserAttributes(F.asMap(ACTIVATOR_NODE_ATTR, "true"));
            }
            else
                cfg.setActiveOnStart(false);
        }

        return cfg;
    }

    /**  */
    protected Collection<CacheConfiguration> getCacheConfigurations() {
        CacheConfiguration[] ccfgs = new CacheConfiguration[CACHES_CNT];

        for (int cnt = 0; cnt < CACHES_CNT; cnt++) {
            CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

            ccfg.setName(testCacheName(cnt));
            ccfg.setCacheMode(PARTITIONED);
            ccfg.setWriteSynchronizationMode(PRIMARY_SYNC);
            ccfg.setBackups(0);
            ccfg.setTopologyValidator(new SplitAwareTopologyValidator());

            if (useCacheGrp)
                ccfg.setGroupName("testGroup");

            ccfgs[cnt] = ccfg;
        }

        return Arrays.asList(ccfgs);
    }

    /**
     * @param idx Index.
     * @return Cache name.
     */
    private String testCacheName(int idx) {
        return "test" + idx;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGridsMultiThreaded(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Tests topology split scenario.
     *
     * @throws Exception If failed.
     */
    public void testTopologyValidator() throws Exception {
        testTopologyValidator0(false);
    }

    /**
     * Tests topology split scenario.
     *
     * @throws Exception If failed.
     */
    public void testTopologyValidatorWithCacheGroup() throws Exception {
        testTopologyValidator0(true);
    }

    /**
     * Tests topology split scenario.
     * @param useCacheGrp Use cache group.
     *
     * @throws Exception If failed.
     */
    private void testTopologyValidator0(boolean useCacheGrp) throws Exception {
        this.useCacheGrp = useCacheGrp;

        IgniteEx grid = grid(0);

        grid.getOrCreateCaches(getCacheConfigurations());

        // Init grid index arrays
        int[] dc1 = new int[GRID_CNT / 2];

        for (int i = 0; i < dc1.length; ++i)
            dc1[i] = i * 2 + 1;

        int[] dc0 = new int[GRID_CNT - dc1.length];

        for (int i = 0; i < dc0.length; ++i)
            dc0[i] = i * 2;

        // Tests what each node is able to do puts.
        tryPut(dc0);

        tryPut(dc1);

        clearAll();

        // Force segmentation.
        for (int idx : dc1)
            stopGrid(idx);

        awaitPartitionMapExchange();

        try {
            tryPut(dc0);

            fail();
        }
        catch (Exception e) {
            // No-op.
        }

        // Repair split by adding activator node in topology.
        resolveSplit();

        tryPut(dc0);

        clearAll();

        // Fix split by adding node from second DC.
        startGrid(CONFIGLESS_GRID_IDX);

        awaitPartitionMapExchange();

        tryPut(CONFIGLESS_GRID_IDX);

        // Force split by removing last node from second DC.
        stopGrid(CONFIGLESS_GRID_IDX);

        awaitPartitionMapExchange();

        try {
            tryPut(dc0);

            fail();
        }
        catch (Exception e) {
            // No-op.
        }

        // Repair split by adding activator node in topology.
        resolveSplit();

        tryPut(dc0);

        clearAll();

        // Removing one node from segmented DC, shouldn't reset repair state.
        stopGrid(0);

        awaitPartitionMapExchange();

        for (int i = 0; i < dc0.length; i++) {
            int idx = dc0[i];

            if (idx == 0)
                continue;

            assertEquals("Expecting put count", CACHES_CNT, tryPut(idx));
        }

        clearAll(2);

        // Add node to segmented DC, shouldn't reset repair state.
        startGrid(0);

        awaitPartitionMapExchange();

        assertEquals("Expecting put count", CACHES_CNT * dc0.length, tryPut(dc0));
    }

    /**
     * @param g Node index.
     */
    private void clearAll(int g) {
        for (int i = 0; i < CACHES_CNT; i++)
            grid(g).cache(testCacheName(i)).clear();
    }

    /** */
    private void clearAll() {
        clearAll(0);
    }

    /**
     * Resolves split by client node join.
     *
     * @throws Exception If failed.
     */
    private void resolveSplit() throws Exception {
        startGrid(RESOLVER_GRID_IDX);

        stopGrid(RESOLVER_GRID_IDX);
    }

    /**
     * @param grids Grids to test.
     */
    private int tryPut(int... grids) {
        int putCnt = 0;

        for (int i = 0; i < grids.length; i++) {
            IgniteEx g = grid(grids[i]);
            for (int cnt = 0; cnt < CACHES_CNT; cnt++) {
                String cacheName = testCacheName(cnt);

                for (int k = 0; k < 100; k++) {
                    if (g.affinity(cacheName).isPrimary(g.localNode(), k)) {
                        IgniteCache<Object, Object> cache = g.cache(cacheName);

                        try {
                            cache.put(k, k);
                        }
                        catch (Throwable t) {
                            log.error("Failed to put entry: [cache=" + cacheName + ", key=" + k + ", nodeId=" +
                                g.name() + ']', t);

                            throw t;
                        }

                        assertEquals(1, cache.localSize());

                        putCnt++;

                        break;
                    }
                }
            }
        }

        return putCnt;
    }

    /**
     * Prevents cache from performing any operation if only nodes from single data center are left in topology.
     */
    private static class SplitAwareTopologyValidator implements TopologyValidator {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @CacheNameResource
        private String cacheName;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        @LoggerResource
        private IgniteLogger log;

        /** State. */
        private transient State state;

        /** {@inheritDoc} */
        @Override public boolean validate(Collection<ClusterNode> nodes) {
            initIfNeeded(nodes);

            if (!F.view(nodes, new IgnitePredicate<ClusterNode>() {
                @Override public boolean apply(ClusterNode node) {
                    return !node.isClient() && node.attribute(DC_NODE_ATTR) == null;
                }
            }).isEmpty()) {
                log.error("No valid server nodes are detected in topology: [cacheName=" + cacheName + ']');

                return false;
            }

            boolean segmented = segmented(nodes);

            if (!segmented)
                state = State.VALID; // Also clears possible REPAIRED state.
            else {
                if (state == State.REPAIRED) // Any topology change in segmented grid in repaired mode is valid.
                    return true;

                // Find discovery event node.
                ClusterNode evtNode = evtNode(nodes);

                if (activator(evtNode)) {
                    if (log.isInfoEnabled())
                        log.info("Grid segmentation is repaired: [cacheName=" + cacheName + ']');

                    state = State.REPAIRED;
                }
                else {
                    if (state == State.VALID) {
                        if (log.isInfoEnabled())
                            log.info("Grid segmentation is detected: [cacheName=" + cacheName + ']');
                    }

                    state = State.NOTVALID;
                }
            }

            return state != State.NOTVALID;
        }

        /** */
        private boolean segmented(Collection<ClusterNode> nodes) {
            ClusterNode prev = null;

            for (ClusterNode node : nodes) {
                if (node.isClient())
                    continue;

                if (prev != null &&
                    !prev.attribute(DC_NODE_ATTR).equals(node.attribute(DC_NODE_ATTR)))
                    return false;

                prev = node;
            }

            return true;
        }

        /**
         * @param node Node.
         * @return {@code True} if this is marker node.
         */
        private boolean activator(ClusterNode node) {
            return node.isClient() && node.attribute(ACTIVATOR_NODE_ATTR) != null;
        }

        /**
         * Sets initial validator state.
         *
         * @param nodes Topology nodes.
         */
        private void initIfNeeded(Collection<ClusterNode> nodes) {
            if (state != null)
                return;

            // Search for activator node in history on start.
            long topVer = evtNode(nodes).order();

            while(topVer > 0) {
                Collection<ClusterNode> top = ignite.cluster().topology(topVer--);

                // Stop on reaching history limit.
                if (top == null)
                    return;

                boolean segmented = segmented(top);

                // Stop on reaching valid topology.
                if (!segmented)
                    return;

                for (ClusterNode node : top) {
                    if (activator(node)) {
                        state = State.REPAIRED;

                        return;
                    }
                }
            }
        }

        /**
         * Returns node with biggest order (event topology version).
         *
         * @param nodes Topology nodes.
         * @return ClusterNode Node.
         */
        private ClusterNode evtNode(Collection<ClusterNode> nodes) {
            ClusterNode evtNode = null;

            for (ClusterNode node : nodes) {
                if (evtNode == null || node.order() > evtNode.order())
                    evtNode = node;
            }

            return evtNode;
        }

        /** States. */
        private enum State {
            /** Topology valid. */
            VALID,
            /** Topology not valid */
            NOTVALID,
            /** Topology repaired (valid) */
            REPAIRED;
        }
    }
}