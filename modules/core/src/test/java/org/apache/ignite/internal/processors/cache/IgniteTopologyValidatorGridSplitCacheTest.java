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
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TopologyValidator;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCacheTopologySplitAbstractTest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.CacheNameResource;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;

/**
 * Tests complex scenario with topology validator. Grid is split between to data centers, defined by attribute {@link
 * #DC_NODE_ATTR}. If only nodes from single DC are left in topology, grid is moved into inoperative state until special
 * activator node'll enter a topology, enabling grid operations.
 */
public class IgniteTopologyValidatorGridSplitCacheTest extends IgniteCacheTopologySplitAbstractTest {

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
    private boolean useCacheGrp;

    /**  */
    private int getDiscoPort(int gridIdx) {
        return TcpDiscoverySpi.DFLT_PORT + gridIdx;
    }

    /**  */
    private boolean isDiscoPort(int port) {
        return port >= TcpDiscoverySpi.DFLT_PORT &&
            port <= (TcpDiscoverySpi.DFLT_PORT + TcpDiscoverySpi.DFLT_PORT_RANGE);
    }

    /** {@inheritDoc} */
    @Override protected boolean isBlocked(int locPort, int rmtPort) {
        return isDiscoPort(rmtPort) && segment(locPort) != segment(rmtPort);
    }

    /**  */
    private int segment(int discoPort) {
        return (discoPort - TcpDiscoverySpi.DFLT_PORT) % 2;
    }

    /** {@inheritDoc} */
    @Override protected int segment(ClusterNode node) {
        return node.attribute(DC_NODE_ATTR);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        int idx = getTestIgniteInstanceIndex(gridName);

        Map<String, Object> userAttrs = new HashMap<>(4);

        userAttrs.put(DC_NODE_ATTR, idx % 2);

        TcpDiscoverySpi disco = (TcpDiscoverySpi)cfg.getDiscoverySpi();

        disco.setLocalPort(getDiscoPort(idx));

        if (idx != CONFIGLESS_GRID_IDX) {
            if (idx == RESOLVER_GRID_IDX) {
                cfg.setClientMode(true);

                userAttrs.put(ACTIVATOR_NODE_ATTR, "true");
            }
            else
                cfg.setActiveOnStart(false);
        }
        cfg.setUserAttributes(userAttrs);

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

    /**  */
    protected void stopGrids(int... grids) {
        for (int idx : grids)
            stopGrid(idx);
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
     *
     * @param useCacheGrp Use cache group.
     * @throws Exception If failed.
     */
    private void testTopologyValidator0(boolean useCacheGrp) throws Exception {
        this.useCacheGrp = useCacheGrp;

        IgniteEx grid = grid(0);

        grid.getOrCreateCaches(getCacheConfigurations());

        // Init grid index arrays
        int[] seg1 = new int[GRID_CNT / 2];

        for (int i = 0; i < seg1.length; ++i)
            seg1[i] = i * 2 + 1;

        int[] seg0 = new int[GRID_CNT - seg1.length];

        for (int i = 0; i < seg0.length; ++i)
            seg0[i] = i * 2;

        // Tests what each node is able to do puts.
        tryPut(seg0, seg1);

        clearAll();

        // Force segmentation.
        splitAndWait();

        try {
            tryPut(seg0, seg1);

            fail();
        }
        catch (Exception e) {
            // No-op.
        }

        // Repair split by adding activator node in topology.
        resolveSplit();

        tryPut(seg0);

        clearAll();

        try {
            tryPut(seg1);

            fail();
        }
        catch (Exception e) {
            // No-op.
        }

        stopGrids(seg1);

        // Fix split by adding node from second DC.
        unsplit();

        startGrid(CONFIGLESS_GRID_IDX);

        awaitPartitionMapExchange();

        tryPut(seg0);

        tryPut(CONFIGLESS_GRID_IDX);

        clearAll();

        // Force split by removing last node from second DC.
        stopGrid(CONFIGLESS_GRID_IDX);

        awaitPartitionMapExchange();

        try {
            tryPut(seg0);

            fail();
        }
        catch (Exception e) {
            // No-op.
        }

        // Repair split with concurrent server node join race.
        resolveSplitWithRace(CONFIGLESS_GRID_IDX);

        // Repair split by adding activator node in topology.
        resolveSplit();

        tryPut(seg0);

        clearAll();

        // Removing one node from segmented DC, shouldn't reset repair state.
        stopGrid(0);

        awaitPartitionMapExchange();

        for (int idx : seg0) {
            if (idx == 0)
                continue;

            assertEquals("Expecting put count", CACHES_CNT, tryPut(idx));
        }

        clearAll(2);

        // Add node to segmented DC, shouldn't reset repair state.
        startGrid(0);

        awaitPartitionMapExchange();

        assertEquals("Expecting put count", CACHES_CNT * seg0.length, tryPut(seg0));
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
     * Resolves split by client node join with server node join race simulation.
     *
     * @param srvNode server node index to simulate join race
     * @throws Exception If failed.
     */
    private void resolveSplitWithRace(int srvNode) throws Exception {
        startGrid(RESOLVER_GRID_IDX);

        startGrid(srvNode);

        awaitPartitionMapExchange();

        tryPut(srvNode);

        clearAll();

        stopGrid(srvNode);

        awaitPartitionMapExchange();

        try {
            tryPut(0);

            fail();
        }
        catch (Exception e) {
            // No-op.
        }

        stopGrid(RESOLVER_GRID_IDX);
    }

    /**
     * @param idx Grid to test.
     * @return number of successful puts to caches
     * @throws IgniteException If all tries to put was failed.
     * @throws AssertionError If some of tries to put was failed.
     */
    private int tryPut(int idx) {
        IgniteEx g = grid(idx);

        int putCnt = 0;

        IgniteException ex = null;

        for (int cnt = 0; cnt < CACHES_CNT; cnt++) {
            String cacheName = testCacheName(cnt);

            for (int k = 0; k < 100; k++) {
                if (g.affinity(cacheName).isPrimary(g.cluster().localNode(), k)) {
                    IgniteCache<Object, Object> cache = g.cache(cacheName);

                    try {
                        cache.put(k, k);

                        assertEquals(1, cache.localSize());

                        if (ex != null)
                            throw new AssertionError("Partial results (put count > 0)", ex);

                        putCnt++;
                    }
                    catch (Throwable t) {
                        IgniteException e = new IgniteException("Failed to put entry: [cache=" + cacheName + ", key=" + k + ']', t);

                        log.error(e.getMessage(), e.getCause());

                        if (ex == null)
                            ex = new IgniteException("Failed to put entry [node=" + g.name() + ']');

                        ex.addSuppressed(t);
                    }
                    break;
                }
            }
        }
        if (ex != null)
            throw ex;

        return putCnt;
    }

    /**
     * @param grids Grids to test.
     * @return number of successful puts to caches
     * @throws IgniteException If all tries to put was failed.
     * @throws AssertionError If some of tries to put was failed.
     */
    private int tryPut(int[]... grids) {
        int putCnt = 0;

        IgniteException ex = null;

        for (int[] idxs : grids) {
            for (int idx : idxs) {
                try {
                    putCnt += tryPut(idx);

                    if (ex != null)
                        throw new AssertionError("Partial result (put count > 0)", ex);
                }
                catch (Exception e) {
                    if (ex == null)
                        ex = new IgniteException("Failed to put entry");

                    ex.addSuppressed(e);
                }
            }
        }
        if (ex != null)
            throw ex;

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

        /** Activator node order on last repair. */
        private transient long lastActivatorOrder;

        /** {@inheritDoc} */
        @Override public boolean validate(Collection<ClusterNode> nodes) {
            initIfNeeded(nodes);

            for (ClusterNode node : F.view(nodes, new IgnitePredicate<ClusterNode>() {
                @Override public boolean apply(ClusterNode node) {
                    return !node.isClient() && node.attribute(DC_NODE_ATTR) == null;
                }
            })) {
                log.error("Not valid server nodes are detected in topology: [" +
                    "cacheName=" + cacheName + ", node=" + node + ']');

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

                if (activator(evtNode) && evtNode.order() > lastActivatorOrder) {
                    if (log.isInfoEnabled())
                        log.info("Grid segmentation is repaired: [cacheName=" + cacheName + ']');

                    repair(evtNode);
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

            while (topVer > 0) {
                Collection<ClusterNode> top = ignite.cluster().topology(topVer--);

                // Stop on reaching history limit.
                if (top == null)
                    return;

                boolean segmented = segmented(top);

                // Stop on reaching valid topology.
                if (!segmented)
                    return;

                ClusterNode activator = null;

                for (ClusterNode node : top) {
                    if (activator(node) && (activator == null || activator.order() < node.order())) {
                        activator = node;

                        break;
                    }
                }

                if (activator != null) {
                    repair(activator);

                    return;
                }
            }
        }

        /**
         * Set REPAIRED state and remember activator order.
         *
         * @param activator Activator node.
         */
        private void repair(ClusterNode activator) {
            state = State.REPAIRED;

            lastActivatorOrder = activator.order();
        }

        /**
         * Returns node with the biggest order (event topology version).
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
            /** Topology is valid. */
            VALID,
            /** Topology is not valid */
            NOTVALID,
            /** Topology is repaired (valid) */
            REPAIRED;
        }
    }
}