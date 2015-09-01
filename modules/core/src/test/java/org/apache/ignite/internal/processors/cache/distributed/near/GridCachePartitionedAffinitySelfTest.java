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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_READ;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_REMOVED;

/**
 * Partitioned affinity test.
 */
@SuppressWarnings({"PointlessArithmeticExpression"})
public class GridCachePartitionedAffinitySelfTest extends GridCommonAbstractTest {
    /** Backup count. */
    private static final int BACKUPS = 1;

    /** Grid count. */
    private static final int GRIDS = 3;

    /** Fail flag. */
    private static AtomicBoolean failFlag = new AtomicBoolean(false);

    /** */
    private TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setBackups(BACKUPS);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setRebalanceMode(SYNC);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration(cacheCfg);
        cfg.setDiscoverySpi(spi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(GRIDS);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @param ignite Grid.
     * @return Affinity.
     */
    static Affinity<Object> affinity(Ignite ignite) {
        return ignite.affinity(null);
    }

    /**
     * @param aff Affinity.
     * @param key Key.
     * @return Nodes.
     */
    private static Collection<? extends ClusterNode> nodes(Affinity<Object> aff, Object key) {
        return aff.mapKeyToPrimaryAndBackups(key);
    }

    /** @throws Exception If failed. */
    public void testAffinity() throws Exception {
        waitTopologyUpdate();

        Object key = 12345;

        Collection<? extends ClusterNode> nodes = null;

        for (int i = 0; i < GRIDS; i++) {
            Collection<? extends ClusterNode> affNodes = nodes(affinity(grid(i)), key);

            info("Affinity picture for grid [i=" + i + ", aff=" + U.toShortString(affNodes));

            if (nodes == null)
                nodes = affNodes;
            else
                assert F.eqOrdered(nodes, affNodes);
        }
    }

    /**
     * @param g Grid.
     * @param keyCnt Key count.
     */
    private static synchronized void printAffinity(Ignite g, int keyCnt) {
        X.println(">>>");
        X.println(">>> Printing affinity for node: " + g.name());

        Affinity<Object> aff = affinity(g);

        for (int i = 0; i < keyCnt; i++) {
            Collection<? extends ClusterNode> affNodes = nodes(aff, i);

            X.println(">>> Affinity nodes [key=" + i + ", partition=" + aff.partition(i) +
                ", nodes=" + U.nodes2names(affNodes) + ", ids=" + U.nodeIds(affNodes) + ']');
        }

        partitionMap(g);
    }

    /** @param g Grid. */
    private static void partitionMap(Ignite g) {
        X.println(">>> Full partition map for grid: " + g.name());
        X.println(">>> " + dht(g.cache(null)).topology().partitionMap(false).toFullString());
    }

    /** @throws Exception If failed. */
    @SuppressWarnings("BusyWait")
    private void waitTopologyUpdate() throws Exception {
        GridTestUtils.waitTopologyUpdate(null, BACKUPS, log());
    }

    /** @throws Exception If failed. */
    public void testAffinityWithPut() throws Exception {
        waitTopologyUpdate();

        Ignite mg = grid(0);

        IgniteCache<Integer, String> mc = mg.cache(null);

        int keyCnt = 10;

        printAffinity(mg, keyCnt);

        info("Registering event listener...");

        // Register event listener on remote nodes.
        compute(mg.cluster().forRemotes()).run(new ListenerJob(keyCnt, mg.name()));

        for (int i = 0; i < keyCnt; i++) {
            if (failFlag.get())
                fail("testAffinityWithPut failed.");

            info("Before putting key [key=" + i + ", grid=" + mg.name() + ']');

            mc.put(i, Integer.toString(i));

            if (failFlag.get())
                fail("testAffinityWithPut failed.");
        }

        Thread.sleep(1000);

        if (failFlag.get())
            fail("testAffinityWithPut failed.");
    }

    /**
     *
     */
    private static class ListenerJob implements IgniteRunnable {
        /** Grid. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** Logger. */
        @LoggerResource
        private IgniteLogger log;

        /** */
        private int keyCnt;

        /** Master grid name. */
        private String master;

        /** */
        private AtomicInteger evtCnt = new AtomicInteger();

        /**
         *
         */
        private ListenerJob() {
            // No-op.
        }

        /**
         * @param keyCnt Key count.
         * @param master Master.
         */
        private ListenerJob(int keyCnt, String master) {
            this.keyCnt = keyCnt;
            this.master = master;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            printAffinity(ignite, keyCnt);

            IgnitePredicate<Event> lsnr = new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    CacheEvent e = (CacheEvent)evt;

                    switch (e.type()) {
                        case EVT_CACHE_OBJECT_PUT:
//                            new Exception("Dumping stack on grid [" + grid.name() + ", evtHash=" +
//                                System.identityHashCode(evt) + ']').printStackTrace(System.out);

                            log.info(">>> Grid cache event [grid=" + ignite.name() + ", name=" + e.name() +
                                ", key=" + e.key() + ", oldVal=" + e.oldValue() + ", newVal=" + e.newValue() +
                                ']');

                            evtCnt.incrementAndGet();

                            if (!ignite.name().equals(master) && evtCnt.get() > keyCnt * (BACKUPS + 1)) {
                                failFlag.set(true);

                                fail("Invalid put event count on grid [cnt=" + evtCnt.get() + ", grid=" +
                                    ignite.name() + ']');
                            }

                            Collection<? extends ClusterNode> affNodes = nodes(affinity(ignite), e.key());

                            if (!affNodes.contains(ignite.cluster().localNode())) {
                                failFlag.set(true);

                                fail("Key should not be mapped to node [key=" + e.key() + ", node=" + ignite.name() + ']');
                            }

                            break;

                        default:
                            failFlag.set(true);

                            fail("Invalid cache event [grid=" + ignite + ", evt=" + evt + ']');
                    }

                    return true;
                }
            };

            ignite.events().localListen(lsnr,
                EVT_CACHE_OBJECT_PUT,
                EVT_CACHE_OBJECT_READ,
                EVT_CACHE_OBJECT_REMOVED);
        }
    }
}