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
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheRebalanceMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 *
 */
public class GridDiscoveryManagerAliveCacheSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int PERM_NODES_CNT = 5;

    /** */
    private static final int TMP_NODES_CNT = 3;

    /** */
    private static final int ITERATIONS = 20;

    /** */
    private int gridCntr;

    /** */
    private List<Ignite> alive = new ArrayList<>(PERM_NODES_CNT + TMP_NODES_CNT);

    /** */
    private volatile CountDownLatch latch;

    /** */
    private final IgnitePredicate<Event> lsnr = new IgnitePredicate<Event>() {
        @Override public boolean apply(Event evt) {
            assertNotNull("Topology lost nodes before stopTempNodes() was called.", latch);

            latch.countDown();

            return true;
        }
    };

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration cCfg = defaultCacheConfiguration();

        cCfg.setCacheMode(PARTITIONED);
        cCfg.setBackups(1);
        cCfg.setDistributionMode(NEAR_PARTITIONED);
        cCfg.setRebalanceMode(SYNC);
        cCfg.setQueryIndexEnabled(false);
        cCfg.setWriteSynchronizationMode(FULL_SYNC);

        TcpDiscoverySpi disc = new TcpDiscoverySpi();

        disc.setIpFinder(IP_FINDER);

        cfg.setCacheConfiguration(cCfg);
        cfg.setDiscoverySpi(disc);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        for (int i = 0; i < PERM_NODES_CNT; i++) {
            Ignite g = startGrid(gridCntr++);

            g.events().localListen(lsnr, EventType.EVT_NODE_LEFT);

            alive.add(g);
        }

        for (int i = 0; i < PERM_NODES_CNT + TMP_NODES_CNT; i++)
            F.rand(alive).jcache(null).put(i, String.valueOf(i));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAlives() throws Exception {
        for (int i = 0; i < ITERATIONS; i++) {
            info("Performing iteration: " + i);

            // Clear latch reference, so any unexpected EVT_NODE_LEFT would fail the test.
            latch = null;

            startTempNodes();

            awaitDiscovery(PERM_NODES_CNT + TMP_NODES_CNT);

            // When temporary nodes stop every permanent node should receive TMP_NODES_CNT events.
            latch = new CountDownLatch(PERM_NODES_CNT * TMP_NODES_CNT);

            stopTempNodes();

            latch.await();

            validateAlives();
        }
    }

    /**
     * Waits while topology on all nodes became equals to the expected size.
     *
     * @param nodesCnt Expected nodes count.
     * @throws InterruptedException If interrupted.
     */
    @SuppressWarnings("BusyWait")
    private void awaitDiscovery(long nodesCnt) throws InterruptedException {
        for (Ignite g : alive) {
            while (g.cluster().nodes().size() != nodesCnt)
                Thread.sleep(10);
        }
    }

    /**
     * Validates that all node collections contain actual information.
     */
    @SuppressWarnings("SuspiciousMethodCalls")
    private void validateAlives() {
        for (Ignite g : alive)
            assertEquals(PERM_NODES_CNT, g.cluster().nodes().size());

        for (final Ignite g : alive) {
            IgniteKernal k = (IgniteKernal)g;

            GridDiscoveryManager discoMgr = k.context().discovery();

            final Collection<ClusterNode> currTop = g.cluster().nodes();

            long currVer = discoMgr.topologyVersion();

            for (long v = currVer; v > currVer - GridDiscoveryManager.DISCOVERY_HISTORY_SIZE && v > 0; v--) {
                F.forAll(discoMgr.aliveCacheNodes(null, v),
                    new IgnitePredicate<ClusterNode>() {
                        @Override public boolean apply(ClusterNode e) {
                            return currTop.contains(e);
                        }
                    });

                F.forAll(discoMgr.aliveRemoteCacheNodes(null, v),
                    new IgnitePredicate<ClusterNode>() {
                        @Override public boolean apply(ClusterNode e) {
                            return currTop.contains(e) || g.cluster().localNode().equals(e);
                        }
                    });

                assertTrue(
                    currTop.contains(GridCacheUtils.oldest(k.internalCache().context(), currVer)));
            }
        }
    }

    /**
     * Starts temporary nodes.
     *
     * @throws Exception If failed.
     */
    private void startTempNodes() throws Exception {
        for (int j = 0; j < TMP_NODES_CNT; j++) {
            Ignite newNode = startGrid(gridCntr++);

            info("New node started: " + newNode.name());

            alive.add(newNode);

            newNode.events().localListen(lsnr, EventType.EVT_NODE_LEFT);
        }
    }

    /**
     * Stops temporary nodes.
     */
    private void stopTempNodes() {
        int rmv = 0;

        Collection<Ignite> toRmv = new ArrayList<>(TMP_NODES_CNT);

        for (Iterator<Ignite> iter = alive.iterator(); iter.hasNext() && rmv < TMP_NODES_CNT;) {
            toRmv.add(iter.next());

            iter.remove();

            rmv++;
        }

        // Remove listeners to avoid receiving events from stopping nodes.
        for (Ignite g : toRmv)
            g.events().stopLocalListen(lsnr, EventType.EVT_NODE_LEFT);

        for (Ignite g : toRmv)
            G.stop(g.name(), false);
    }
}
