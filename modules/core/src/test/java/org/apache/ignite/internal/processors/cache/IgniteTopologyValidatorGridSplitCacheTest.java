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

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TopologyValidator;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.resources.CacheNameResource;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Tests complex scenario with topology validator.
 * Grid is split between to data centers, defined by attribute {@link #DC_NODE_ATTR}.
 * If only nodes from single DC are left in topology, grid is moved into inoperative state until special
 * activator node'll enter a topology, enabling grid operations.
 */
public class IgniteTopologyValidatorGridSplitCacheTest extends GridCommonAbstractTest {
    /** */
    private static final String DC_NODE_ATTR = "dc";

    /** */
    private static final String ACTIVATOR_NODE_ATTR = "split.resolved";

    /** */
    private static final int GRID_CNT = 4;

    /** */
    private static final int CACHES_CNT = 10;

    /** */
    private static final int RESOLVER_GRID_IDX = GRID_CNT;

    /** */
    private static final int CONFIGLESS_GRID_IDX = GRID_CNT + 1;

    /** */
    private static CountDownLatch initLatch = new CountDownLatch(GRID_CNT);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        int idx = getTestGridIndex(gridName);

        cfg.setUserAttributes(F.asMap(DC_NODE_ATTR, idx % 2));

        if (idx != CONFIGLESS_GRID_IDX) {
            if (idx == RESOLVER_GRID_IDX) {
                cfg.setClientMode(true);

                cfg.setUserAttributes(F.asMap(ACTIVATOR_NODE_ATTR, "true"));
            }
            else {
                CacheConfiguration[] ccfgs = new CacheConfiguration[CACHES_CNT];

                for (int cnt = 0; cnt < CACHES_CNT; cnt++) {
                    CacheConfiguration ccfg = new CacheConfiguration();

                    ccfg.setName(testCacheName(cnt));
                    ccfg.setCacheMode(PARTITIONED);
                    ccfg.setBackups(0);
                    ccfg.setTopologyValidator(new SplitAwareTopologyValidator());

                    ccfgs[cnt] = ccfg;
                }

                cfg.setCacheConfiguration(ccfgs);
            }
        }

        return cfg;
    }

    /**
     * @param idx Index.
     */
    private String testCacheName(int idx) {
        return "test" + idx;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * Tests topology split scenario.
     * @throws Exception
     */
    public void testTopologyValidator() throws Exception {
        assertTrue(initLatch.await(10, TimeUnit.SECONDS));

        // Tests what each node is able to do puts.
        tryPut(0, 1, 2, 3);

        clearAll();

        stopGrid(1);

        stopGrid(3);

        awaitPartitionMapExchange();

        try {
            tryPut(0, 2);

            fail();
        }
        catch (Exception e) {
            // No-op.
        }

        resolveSplit();

        tryPut(0, 2);

        clearAll();

        startGrid(CONFIGLESS_GRID_IDX);

        awaitPartitionMapExchange();

        tryPut(CONFIGLESS_GRID_IDX);

        stopGrid(CONFIGLESS_GRID_IDX);

        awaitPartitionMapExchange();

        try {
            tryPut(0, 2);

            fail();
        }
        catch (Exception e) {
            // No-op.
        }

        resolveSplit();

        tryPut(0, 2);

        clearAll();

        startGrid(1);

        awaitPartitionMapExchange();

        tryPut(0, 1, 2);
    }

    /** */
    private void clearAll() {
        for (int i = 0; i < CACHES_CNT; i++)
            grid(0).cache(testCacheName(i)).clear();
    }

    /**
     * Resolves split by client node join.
     */
    private void resolveSplit() throws Exception {
        startGrid(RESOLVER_GRID_IDX);

        stopGrid(RESOLVER_GRID_IDX);
    }

    /**
     * @param grids Grids to test.
     */
    private void tryPut(int... grids) {
        for (int i = 0; i < grids.length; i++) {
            IgniteEx g = grid(grids[i]);

            for (int cnt = 0; cnt < CACHES_CNT; cnt++) {
                String cacheName = testCacheName(cnt);

                for (int k = 0; k < 100; k++) {
                    if (g.affinity(cacheName).isPrimary(g.localNode(), k)) {
                        log().info("Put " + k + " to node " + g.localNode().id().toString());

                        IgniteCache<Object, Object> cache = g.cache(cacheName);

                        cache.put(k, k);

                        assertEquals(1, cache.localSize());

                        break;
                    }
                }
            }
        }
    }

    /**
     * Prevents cache from performing any operation if only nodes from single data center are left in topology.
     */
    private static class SplitAwareTopologyValidator implements TopologyValidator, LifecycleAware {
        /** */
        private static final long serialVersionUID = 0L;

        @CacheNameResource
        private String cacheName;

        @IgniteInstanceResource
        private Ignite ignite;

        @LoggerResource
        private IgniteLogger log;

        /** */
        private transient volatile long activatorTopVer;

        /** {@inheritDoc} */
        @Override public boolean validate(Collection<ClusterNode> nodes) {
            if (!F.view(nodes, new IgnitePredicate<ClusterNode>() {
                @Override public boolean apply(ClusterNode node) {
                    return !node.isClient() && node.attribute(DC_NODE_ATTR) == null;
                }
            }).isEmpty())
                return false;

            IgniteKernal kernal = (IgniteKernal)ignite.cache(cacheName).unwrap(Ignite.class);

            GridDhtCacheAdapter<Object, Object> dht = kernal.context().cache().internalCache(cacheName).context().dht();

            long cacheTopVer = dht.topology().topologyVersionFuture().topologyVersion().topologyVersion();

            if (hasSplit(nodes)) {
                boolean resolved = activatorTopVer != 0 && cacheTopVer >= activatorTopVer;

                if (!resolved)
                    log.info("Grid segmentation is detected, switching to inoperative state.");

                return resolved;
            }
            else
                activatorTopVer = 0;

            return true;
        }

        /** */
        private boolean hasSplit(Collection<ClusterNode> nodes) {
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

        @Override public void start() throws IgniteException {
            if (ignite.cluster().localNode().isClient())
                return;

            initLatch.countDown();

            ignite.events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    DiscoveryEvent discoEvt = (DiscoveryEvent)evt;

                    ClusterNode node = discoEvt.eventNode();

                    if (isMarkerNode(node))
                        activatorTopVer = discoEvt.topologyVersion();

                    return true;
                }
            }, EventType.EVT_NODE_LEFT);
        }

        /**
         * @param node Node.
         */
        private boolean isMarkerNode(ClusterNode node) {
            return node.isClient() && node.attribute(ACTIVATOR_NODE_ATTR) != null;
        }

        @Override public void stop() throws IgniteException {
        }
    }
}