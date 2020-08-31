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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.TopologyValidator;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCacheTopologySplitAbstractTest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.CacheNameResource;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;
import static org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi.DFLT_PORT;

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
    private static final int GRID_CNT = 32;

    /** */
    private static final int CACHES_CNT = 50;

    /** */
    private static final int RESOLVER_GRID_IDX = GRID_CNT;

    /** */
    private static final int CONFIGLESS_GRID_IDX = GRID_CNT + 1;

    /** */
    private static final String STATIC_IP = "127.0.0.1";

    /** */
    private static final Collection<String> SEG_FINDER_0;

    /** */
    private static final Collection<String> SEG_FINDER_1;

    /** */
    private static final Collection<String> SEG_FINDER_ALL;

    static {
        Collection<String> seg0 = new ArrayList<>();

        Collection<String> seg1 = new ArrayList<>();

        for (int i = 0; i < GRID_CNT; i += 2) {
            seg0.add(STATIC_IP + ':' + (DFLT_PORT + i));

            seg1.add(STATIC_IP + ':' + (DFLT_PORT + i + 1));
        }
        SEG_FINDER_0 = Collections.unmodifiableCollection(seg0);

        SEG_FINDER_1 = Collections.unmodifiableCollection(seg1);

        SEG_FINDER_ALL = F.concat(false, SEG_FINDER_0, SEG_FINDER_1);
    }

    /** */
    private boolean useCacheGrp;

    /**  */
    private int getDiscoPort(int gridIdx) {
        return DFLT_PORT + gridIdx;
    }

    /**  */
    private boolean isDiscoPort(int port) {
        return port >= DFLT_PORT &&
            port <= (DFLT_PORT + TcpDiscoverySpi.DFLT_PORT_RANGE);
    }

    /** {@inheritDoc} */
    @Override protected boolean isBlocked(int locPort, int rmtPort) {
        return isDiscoPort(locPort) && isDiscoPort(rmtPort) && segment(locPort) != segment(rmtPort);
    }

    /**  */
    private int segment(int discoPort) {
        return (discoPort - DFLT_PORT) % 2;
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

        int segment = idx % 2;

        userAttrs.put(DC_NODE_ATTR, segment);

        TcpDiscoverySpi disco = (TcpDiscoverySpi)cfg.getDiscoverySpi();

        disco.setLocalPort(getDiscoPort(idx));

        disco.setIpFinder(new TcpDiscoveryVmIpFinder().setAddresses(segmented() ?
            (segment == 0 ? SEG_FINDER_0 : SEG_FINDER_1) : SEG_FINDER_ALL));

        if (idx != CONFIGLESS_GRID_IDX) {
            if (idx == RESOLVER_GRID_IDX) {
                userAttrs.put(ACTIVATOR_NODE_ATTR, "true");
            }
            else
                cfg.setActiveOnStart(false);
        }
        cfg.setUserAttributes(userAttrs);

        cfg.setMemoryConfiguration(new MemoryConfiguration().
            setDefaultMemoryPolicySize((50L << 20) + (100L << 20) * CACHES_CNT / GRID_CNT));

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
        Assume.assumeFalse("https://issues.apache.org/jira/browse/IGNITE-7952", MvccFeatureChecker.forcedMvcc());

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
    @Test
    public void testTopologyValidator() throws Exception {
        testTopologyValidator0(false);
    }

    /**
     * Tests topology split scenario.
     *
     * @throws Exception If failed.
     */
    @Test
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
        startClientGrid(RESOLVER_GRID_IDX);

        stopGrid(RESOLVER_GRID_IDX);
    }

    /**
     * Resolves split by client node join with server node join race simulation.
     *
     * @param srvNode server node index to simulate join race
     * @throws Exception If failed.
     */
    private void resolveSplitWithRace(int srvNode) throws Exception {
        startClientGrid(RESOLVER_GRID_IDX);

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

            int key = -1;

            Affinity<Object> aff = g.affinity(cacheName);

            for (int k = 0; k < aff.partitions(); k++) {
                if (aff.isPrimary(g.cluster().localNode(), k)) {
                    key = k;

                    break;
                }
            }

            assertTrue("Failed to find affinity key [gridIdx=" + idx + ", cache=" + cacheName + ']',
                key != -1);

            IgniteCache<Object, Object> cache = g.cache(cacheName);

            try {
                cache.put(key, key);

                assertEquals(1, cache.localSize());

                if (ex != null)
                    throw new AssertionError("Successful tryPut after failure [gridIdx=" + idx +
                        ", cacheName=" + cacheName + ']', ex);

                putCnt++;
            }
            catch (Throwable t) {
                IgniteException e = new IgniteException("Failed to put entry [gridIdx=" + idx + ", cache=" +
                    cacheName + ", key=" + key + ']', t);

                log.error(e.getMessage(), e.getCause());

                if (ex == null)
                    ex = new IgniteException("Failed to put entry [node=" + g.name() + ']');

                ex.addSuppressed(t);
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
                    int cnt = tryPut(idx);

                    if (ex != null)
                        throw new AssertionError("Successful tryPut after failure [gridIdx=" + idx +
                            ", sucessful puts = " + cnt + ']', ex);

                    putCnt += cnt;
                }
                catch (Exception e) {
                    if (ex == null)
                        ex = new IgniteException("Failed to put entry");

                    ex.addSuppressed(e);

                    if (putCnt != 0) {
                        throw new AssertionError("Failure after successful tryPut [gridIdx=" + idx +
                            ", successful puts = " + putCnt + ']', ex);
                    }
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
        private transient String cacheName;

        /** */
        @IgniteInstanceResource
        private transient Ignite ignite;

        /** */
        @LoggerResource
        private transient IgniteLogger log;

        /** State. */
        private transient State state;

        /** {@inheritDoc} */
        @Override public boolean validate(Collection<ClusterNode> nodes) {
            initIfNeeded(nodes);

            for (ClusterNode node : F.view(nodes, new IgnitePredicate<ClusterNode>() {
                @Override public boolean apply(ClusterNode node) {
                    return !node.isClient() && node.attribute(DC_NODE_ATTR) == null;
                }
            })) {
                log.error("Not valid server nodes are detected in topology: [cacheName=" + cacheName + ", node=" +
                    node + ']');

                return false;
            }

            boolean segmented = segmented(nodes);

            if (!segmented)
                state = State.VALID; // Also clears possible BEFORE_REPAIRED and REPAIRED states.
            else {
                if (state == State.REPAIRED) // Any topology change in segmented grid in repaired mode is valid.
                    return true;

                // Find discovery event node.
                ClusterNode evtNode = evtNode(nodes);

                if (activator(evtNode))
                    state = State.BEFORE_REPARED;
                else {
                    if (state == State.BEFORE_REPARED) {
                        boolean activatorLeft = true;

                        // Check if activator is no longer in topology.
                        for (ClusterNode node : nodes) {
                            if (node.isClient() && activator(node)) {
                                activatorLeft = false;

                                break;
                            }
                        }

                        if (activatorLeft) {
                            if (log.isInfoEnabled())
                                log.info("Grid segmentation is repaired: [cacheName=" + cacheName + ']');

                            state = State.REPAIRED; // Switch to REPAIRED state only when activator leaves.
                        } // Else stay in BEFORE_REPARED state.
                    }
                    else {
                        if (state == State.VALID) {
                            if (log.isInfoEnabled())
                                log.info("Grid segmentation is detected: [cacheName=" + cacheName + ']');
                        }

                        state = State.NOTVALID;
                    }
                }
            }

            return state == State.VALID || state == State.REPAIRED;
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
            /** Topology is valid. */
            VALID,
            /** Topology is not valid */
            NOTVALID,
            /** Before topology will be repaired (valid) */
            BEFORE_REPARED,
            /** Topology is repaired (valid) */
            REPAIRED;
        }
    }
}
