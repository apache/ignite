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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemander;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.resources.CacheNameResource;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Tests complex scenario with topology validator.
 */
public class GridCacheSplitAwareTopologyValidatorSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String DC_NODE_ATTR = "dc";

    /** */
    private static final String RESOLVED_MARKER_NODE_ATTR = "split.resolved";

    /** */
    private static final int GRID_CNT = 4;

    /** */
    private static final int RESOLVER_GRID_IDX = GRID_CNT;

    /** */
    private static final int EMPTY_GRID_IDX = GRID_CNT + 1;

    /** */
    private SplitAwareTopologyValidator validator = new SplitAwareTopologyValidator();

    /** */
    private static CountDownLatch initLatch = new CountDownLatch(GRID_CNT);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        int idx = getTestGridIndex(gridName);

        cfg.setUserAttributes(F.asMap(DC_NODE_ATTR, idx % 2));

        if (idx != EMPTY_GRID_IDX) {
            CacheConfiguration ccfg = new CacheConfiguration();

            ccfg.setCacheMode(PARTITIONED);
            ccfg.setBackups(0);

            if (idx == RESOLVER_GRID_IDX) {
                cfg.setClientMode(true);

                cfg.setUserAttributes(F.asMap(RESOLVED_MARKER_NODE_ATTR, "true"));
            }
            else
                ccfg.setTopologyValidator(validator);

            cfg.setCacheConfiguration(ccfg);
        }

        return cfg;
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

        grid(0).cache(null).clear();

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

        grid(0).cache(null).clear();

        startGrid(EMPTY_GRID_IDX);

        awaitPartitionMapExchange();

        tryPut(EMPTY_GRID_IDX);

        stopGrid(EMPTY_GRID_IDX);

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
            int grid = grids[i];

            for (int k = 0; k < 100; k++) {
                boolean primary = grid(grid).affinity(null).isPrimary(grid(grid).localNode(), k);

                if (primary) {
                    log().info("Put " + k + " to node " + grid(grid).localNode().id().toString());

                    IgniteCache<Object, Object> jcache = jcache(grid);

                    jcache.put(k, k);

                    assertEquals(1, jcache(grid).localSize());

                    break;
                }
            }
        }
    }

    /**
     * Prevents grid from performing operations if only nodes from single data center are left in topology.
     */
    private static class SplitAwareTopologyValidator implements TopologyValidator, LifecycleAware {
        /** */
        private static final long serialVersionUID = 0L;

        @IgniteInstanceResource
        private Ignite ignite;

        @LoggerResource
        private IgniteLogger log;

        /** */
        private transient volatile long markerVersionId;

        /** {@inheritDoc} */
        @Override public boolean validate(Collection<ClusterNode> nodes) {
            if (!F.view(nodes, new IgnitePredicate<ClusterNode>() {
                @Override public boolean apply(ClusterNode node) {
                    return node.attribute(DC_NODE_ATTR) == null;
                }
            }).isEmpty())
                return false;

            IgniteKernal kernal = (IgniteKernal)ignite;

            GridCacheSharedContext sharedCtx = U.field(kernal.context().cache(), "sharedCtx");

            AffinityTopologyVersion topVer = sharedCtx.exchange().topologyVersion();

            if (hasSplit(nodes)) {
                boolean resolved = markerVersionId != 0 && topVer.topologyVersion() >= markerVersionId;

                if (!resolved)
                    log.info("Grid segmentation is detected, switching to inoperative state.");

                return resolved;
            }
            else
                markerVersionId = 0;

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
                    DiscoveryEvent evt1 = (DiscoveryEvent)evt;

                    ClusterNode node = evt1.eventNode();

                    if (isMarkerNode(node))
                        markerVersionId = evt1.topologyVersion();

                    return true;
                }
            }, EventType.EVT_NODE_JOINED);
        }

        /**
         * @param node Node.
         */
        private boolean isMarkerNode(ClusterNode node) {
            return node.isClient() && node.attribute(RESOLVED_MARKER_NODE_ATTR) != null;
        }

        @Override public void stop() throws IgniteException {
        }
    }
}