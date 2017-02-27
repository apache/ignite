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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

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
    private static final int ITERATIONS = 10;

    /** */
    private int gridCntr;

    /** */
    private List<Ignite> alive = new ArrayList<>(PERM_NODES_CNT + TMP_NODES_CNT);

    /** */
    private volatile CountDownLatch latch;

    /** */
    private boolean clientMode;

    /** */
    private final IgnitePredicate<Event> lsnr = new IgnitePredicate<Event>() {
        @Override public boolean apply(Event evt) {
            assertNotNull("Topology lost nodes before stopTempNodes() was called.", latch);

            latch.countDown();

            return true;
        }
    };

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 10 * 60 * 1000; //10 minutes.
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration cCfg = defaultCacheConfiguration();

        cCfg.setCacheMode(PARTITIONED);
        cCfg.setBackups(1);
        cCfg.setNearConfiguration(new NearCacheConfiguration());
        cCfg.setRebalanceMode(SYNC);
        cCfg.setWriteSynchronizationMode(FULL_SYNC);

        TcpDiscoverySpi disc = new TcpDiscoverySpi();

        if (clientMode && ((gridName.charAt(gridName.length() - 1) - '0') & 1) != 0)
            cfg.setClientMode(true);
        else
            disc.setMaxMissedClientHeartbeats(50);

        disc.setHeartbeatFrequency(500);
        disc.setIpFinder(IP_FINDER);
        disc.setAckTimeout(1000);
        disc.setSocketTimeout(1000);

        cfg.setCacheConfiguration(cCfg);
        cfg.setDiscoverySpi(disc);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        for (int i = 0; i < PERM_NODES_CNT; i++) {
            Ignite g = startGrid(gridCntr++);

            g.events().localListen(lsnr, EventType.EVT_NODE_LEFT, EventType.EVT_NODE_FAILED);

            alive.add(g);
        }

        for (int i = 0; i < PERM_NODES_CNT + TMP_NODES_CNT; i++)
            F.rand(alive).cache(null).put(i, String.valueOf(i));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestAlive() throws Exception {
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
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAlives() throws Exception {
        clientMode = false;

        doTestAlive();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAlivesClient() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-1583");

        clientMode = true;

        doTestAlive();
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
            ((TcpDiscoverySpi)g.configuration().getDiscoverySpi()).waitForClientMessagePrecessed();

            while (g.cluster().nodes().size() != nodesCnt)
                Thread.sleep(10);
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

            newNode.events().localListen(lsnr, EventType.EVT_NODE_LEFT, EventType.EVT_NODE_FAILED);
        }
    }

    /**
     * Stops temporary nodes.
     */
    private void stopTempNodes() {
        Collection<Ignite> toRmv = new ArrayList<>(alive.subList(0, TMP_NODES_CNT));

        alive.removeAll(toRmv);

        // Remove listeners to avoid receiving events from stopping nodes.
        for (Ignite g : toRmv)
            g.events().stopLocalListen(lsnr, EventType.EVT_NODE_LEFT, EventType.EVT_NODE_FAILED);

        for (Iterator<Ignite> itr = toRmv.iterator(); itr.hasNext(); ) {
            Ignite g = itr.next();

            if (g.cluster().localNode().isClient()) {
                G.stop(g.name(), false);

                itr.remove();
            }
        }

        for (Ignite g : toRmv) {
            assert !g.cluster().localNode().isClient();

            G.stop(g.name(), false);
        }
    }
}
