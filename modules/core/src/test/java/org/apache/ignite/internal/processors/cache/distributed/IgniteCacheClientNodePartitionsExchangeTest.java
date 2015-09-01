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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheServerNotFoundException;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.fair.FairAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class IgniteCacheClientNodePartitionsExchangeTest extends GridCommonAbstractTest {
    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean client;

    /** */
    private boolean fairAffinity;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder).setForceServerMode(true);

        cfg.setClientMode(client);

        CacheConfiguration ccfg = new CacheConfiguration();

        if (fairAffinity)
            ccfg.setAffinity(new FairAffinityFunction());

        cfg.setCacheConfiguration(ccfg);

        cfg.setCommunicationSpi(new TestCommunicationSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testServerNodeLeave() throws Exception {
        Ignite ignite0 = startGrid(0);

        client = true;

        final Ignite ignite1 = startGrid(1);

        waitForTopologyUpdate(2, 2);

        final Ignite ignite2 = startGrid(2);

        waitForTopologyUpdate(3, 3);

        ignite0.close();

        waitForTopologyUpdate(2, 4);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                ignite1.cache(null).get(1);

                return null;
            }
        }, CacheServerNotFoundException.class, null);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                ignite2.cache(null).get(1);

                return null;
            }
        }, CacheServerNotFoundException.class, null);

        ignite1.close();

        waitForTopologyUpdate(1, 5);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                ignite2.cache(null).get(1);

                return null;
            }
        }, CacheServerNotFoundException.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSkipPreload() throws Exception {
        Ignite ignite0 = startGrid(0);

        final CountDownLatch evtLatch0 = new CountDownLatch(1);

        ignite0.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                log.info("Rebalance event: " + evt);

                evtLatch0.countDown();

                return true;
            }
        }, EventType.EVT_CACHE_REBALANCE_STARTED, EventType.EVT_CACHE_REBALANCE_STOPPED);

        client = true;

        Ignite ignite1 = startGrid(1);

        assertTrue(evtLatch0.await(1000, TimeUnit.MILLISECONDS));

        ignite1.close();

        assertTrue(evtLatch0.await(1000, TimeUnit.MILLISECONDS));

        ignite1 = startGrid(1);

        final CountDownLatch evtLatch1 = new CountDownLatch(1);

        ignite1.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                log.info("Rebalance event: " + evt);

                evtLatch1.countDown();

                return true;
            }
        }, EventType.EVT_CACHE_REBALANCE_STARTED, EventType.EVT_CACHE_REBALANCE_STOPPED);

        assertTrue(evtLatch0.await(1000, TimeUnit.MILLISECONDS));

        client = false;

        startGrid(2);

        assertTrue(evtLatch0.await(1000, TimeUnit.MILLISECONDS));
        assertFalse(evtLatch1.await(1000, TimeUnit.MILLISECONDS));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionsExchange() throws Exception {
        partitionsExchange();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionsExchangeFairAffinity() throws Exception {
        fairAffinity = true;

        partitionsExchange();
    }

    /**
     * @throws Exception If failed.
     */
    private void partitionsExchange() throws Exception {
        Ignite ignite0 = startGrid(0);

        TestCommunicationSpi spi0 = (TestCommunicationSpi)ignite0.configuration().getCommunicationSpi();

        Ignite ignite1 = startGrid(1);

        waitForTopologyUpdate(2, 2);

        TestCommunicationSpi spi1 = (TestCommunicationSpi)ignite1.configuration().getCommunicationSpi();

        assertEquals(0, spi0.partitionsSingleMessages());
        assertEquals(1, spi0.partitionsFullMessages());

        assertEquals(1, spi1.partitionsSingleMessages());
        assertEquals(0, spi1.partitionsFullMessages());

        spi0.reset();
        spi1.reset();

        client = true;

        log.info("Start client node1.");

        Ignite ignite2 = startGrid(2);

        waitForTopologyUpdate(3, 3);

        TestCommunicationSpi spi2 = (TestCommunicationSpi)ignite2.configuration().getCommunicationSpi();

        assertEquals(0, spi0.partitionsSingleMessages());
        assertEquals(1, spi0.partitionsFullMessages());

        assertEquals(0, spi1.partitionsSingleMessages());
        assertEquals(0, spi1.partitionsFullMessages());

        assertEquals(1, spi2.partitionsSingleMessages());
        assertEquals(0, spi2.partitionsFullMessages());

        spi0.reset();
        spi1.reset();
        spi2.reset();

        log.info("Start client node2.");

        Ignite ignite3 = startGrid(3);

        waitForTopologyUpdate(4, 4);

        TestCommunicationSpi spi3 = (TestCommunicationSpi)ignite3.configuration().getCommunicationSpi();

        assertEquals(0, spi0.partitionsSingleMessages());
        assertEquals(1, spi0.partitionsFullMessages());

        assertEquals(0, spi1.partitionsSingleMessages());
        assertEquals(0, spi1.partitionsFullMessages());

        assertEquals(0, spi2.partitionsSingleMessages());
        assertEquals(0, spi2.partitionsFullMessages());

        assertEquals(1, spi3.partitionsSingleMessages());
        assertEquals(0, spi3.partitionsFullMessages());

        spi0.reset();
        spi1.reset();
        spi2.reset();
        spi3.reset();

        log.info("Start one more server node.");

        client = false;

        Ignite ignite4 = startGrid(4);

        waitForTopologyUpdate(5, 5);

        TestCommunicationSpi spi4 = (TestCommunicationSpi)ignite4.configuration().getCommunicationSpi();

        assertEquals(0, spi0.partitionsSingleMessages());
        assertEquals(4, spi0.partitionsFullMessages());

        assertEquals(1, spi1.partitionsSingleMessages());
        assertEquals(0, spi1.partitionsFullMessages());

        assertEquals(1, spi2.partitionsSingleMessages());
        assertEquals(0, spi2.partitionsFullMessages());

        assertEquals(1, spi3.partitionsSingleMessages());
        assertEquals(0, spi3.partitionsFullMessages());

        assertEquals(1, spi4.partitionsSingleMessages());
        assertEquals(0, spi4.partitionsFullMessages());

        spi0.reset();
        spi1.reset();
        spi2.reset();
        spi3.reset();

        log.info("Stop server node.");

        ignite4.close();

        waitForTopologyUpdate(4, 6);

        assertEquals(0, spi0.partitionsSingleMessages());
        assertEquals(3, spi0.partitionsFullMessages());

        assertEquals(1, spi1.partitionsSingleMessages());
        assertEquals(0, spi1.partitionsFullMessages());

        assertEquals(1, spi2.partitionsSingleMessages());
        assertEquals(0, spi2.partitionsFullMessages());

        assertEquals(1, spi3.partitionsSingleMessages());
        assertEquals(0, spi3.partitionsFullMessages());

        spi0.reset();
        spi1.reset();
        spi2.reset();

        log.info("Stop client node2.");

        ignite3.close();

        waitForTopologyUpdate(3, 7);

        assertEquals(0, spi0.partitionsSingleMessages());
        assertEquals(0, spi0.partitionsFullMessages());

        assertEquals(0, spi1.partitionsSingleMessages());
        assertEquals(0, spi1.partitionsFullMessages());

        assertEquals(0, spi2.partitionsSingleMessages());
        assertEquals(0, spi2.partitionsFullMessages());

        spi0.reset();
        spi1.reset();

        log.info("Stop client node1.");

        ignite2.close();

        waitForTopologyUpdate(2, 8);

        assertEquals(0, spi0.partitionsSingleMessages());
        assertEquals(0, spi0.partitionsFullMessages());

        assertEquals(0, spi1.partitionsSingleMessages());
        assertEquals(0, spi1.partitionsFullMessages());

        log.info("Stop server node.");

        ignite1.close();

        waitForTopologyUpdate(1, 9);

        assertEquals(0, spi0.partitionsSingleMessages());
        assertEquals(0, spi0.partitionsFullMessages());
    }

    /**
     * @param expNodes Expected number of nodes.
     * @param topVer Expected topology version.
     * @throws Exception If failed.
     */
    private void waitForTopologyUpdate(int expNodes, int topVer) throws Exception {
        final AffinityTopologyVersion ver = new AffinityTopologyVersion(topVer, 0);

        waitForTopologyUpdate(expNodes, ver);
    }

    /**
     * @param expNodes Expected number of nodes.
     * @param topVer Expected topology version.
     * @throws Exception If failed.
     */
    private void waitForTopologyUpdate(int expNodes, final AffinityTopologyVersion topVer) throws Exception {
        List<Ignite> nodes = G.allGrids();

        assertEquals(expNodes, nodes.size());

        for (Ignite ignite : nodes) {
            final IgniteKernal kernal = (IgniteKernal)ignite;

            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return topVer.equals(kernal.context().cache().context().exchange().readyAffinityVersion());
                }
            }, 10_000);

            assertEquals("Unexpected affinity version for " + ignite.name(),
                topVer,
                kernal.context().cache().context().exchange().readyAffinityVersion());
        }

        Iterator<Ignite> it = nodes.iterator();

        Ignite ignite0 = it.next();

        Affinity<Integer> aff0 = ignite0.affinity(null);

        while (it.hasNext()) {
            Ignite ignite = it.next();

            Affinity<Integer> aff = ignite.affinity(null);

            assertEquals(aff0.partitions(), aff.partitions());

            for (int part = 0; part < aff.partitions(); part++)
                assertEquals(aff0.mapPartitionToPrimaryAndBackups(part), aff.mapPartitionToPrimaryAndBackups(part));
        }

        for (Ignite ignite : nodes) {
            final IgniteKernal kernal = (IgniteKernal)ignite;

            for (IgniteInternalCache cache : kernal.context().cache().caches()) {
                GridDhtPartitionTopology top = cache.context().topology();

                assertEquals("Unexpected topology version [node=" + ignite.name() + ", cache=" + cache.name() + ']',
                    topVer,
                    top.topologyVersion());
            }
        }

        awaitPartitionMapExchange();
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientOnlyCacheStart() throws Exception {
        clientOnlyCacheStart(false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearOnlyCacheStart() throws Exception {
        clientOnlyCacheStart(true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientOnlyCacheStartFromServerNode() throws Exception {
        clientOnlyCacheStart(false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearOnlyCacheStartFromServerNode() throws Exception {
        clientOnlyCacheStart(true, true);
    }

    /**
     * @param nearCache If {@code true} creates near cache on client.
     * @param srvNode If {@code true} creates client cache on server node.
     * @throws Exception If failed.
     */
    private void clientOnlyCacheStart(boolean nearCache, boolean srvNode) throws Exception {
        Ignite ignite0 = startGrid(0);
        Ignite ignite1 = startGrid(1);

        waitForTopologyUpdate(2, 2);

        final String CACHE_NAME1 = "cache1";

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(CACHE_NAME1);

        if (srvNode)
            ccfg.setNodeFilter(new TestFilter(getTestGridName(2)));

        ignite0.createCache(ccfg);

        client = !srvNode;

        Ignite ignite2 = startGrid(2);

        waitForTopologyUpdate(3, 3);

        TestCommunicationSpi spi0 = (TestCommunicationSpi)ignite0.configuration().getCommunicationSpi();
        TestCommunicationSpi spi1 = (TestCommunicationSpi)ignite1.configuration().getCommunicationSpi();
        TestCommunicationSpi spi2 = (TestCommunicationSpi)ignite2.configuration().getCommunicationSpi();

        spi0.reset();
        spi1.reset();
        spi2.reset();

        assertNull(((IgniteKernal)ignite2).context().cache().context().cache().internalCache(CACHE_NAME1));

        if (nearCache)
            ignite2.getOrCreateNearCache(CACHE_NAME1, new NearCacheConfiguration<>());
        else
            ignite2.cache(CACHE_NAME1);

        waitForTopologyUpdate(3, new AffinityTopologyVersion(3, 1));

        GridCacheAdapter cache = ((IgniteKernal)ignite2).context().cache().context().cache().internalCache(CACHE_NAME1);

        assertNotNull(cache);
        assertEquals(nearCache, cache.context().isNear());

        assertEquals(0, spi0.partitionsSingleMessages());
        assertEquals(1, spi0.partitionsFullMessages());
        assertEquals(0, spi1.partitionsSingleMessages());
        assertEquals(0, spi1.partitionsFullMessages());
        assertEquals(1, spi2.partitionsSingleMessages());
        assertEquals(0, spi2.partitionsFullMessages());

        ClusterNode clientNode = ((IgniteKernal)ignite2).localNode();

        for (Ignite ignite : Ignition.allGrids()) {
            GridDiscoveryManager disco = ((IgniteKernal)ignite).context().discovery();

            assertTrue(disco.cacheNode(clientNode, CACHE_NAME1));
            assertFalse(disco.cacheAffinityNode(clientNode, CACHE_NAME1));
            assertEquals(nearCache, disco.cacheNearNode(clientNode, CACHE_NAME1));
        }

        spi0.reset();
        spi1.reset();
        spi2.reset();

        AffinityTopologyVersion topVer;

        if (!srvNode) {
            log.info("Close client cache: " + CACHE_NAME1);

            ignite2.cache(CACHE_NAME1).close();

            assertNull(((IgniteKernal)ignite2).context().cache().context().cache().internalCache(CACHE_NAME1));

            waitForTopologyUpdate(3, new AffinityTopologyVersion(3, 2));

            assertEquals(0, spi0.partitionsSingleMessages());
            assertEquals(0, spi0.partitionsFullMessages());
            assertEquals(0, spi1.partitionsSingleMessages());
            assertEquals(0, spi1.partitionsFullMessages());
            assertEquals(0, spi2.partitionsSingleMessages());
            assertEquals(0, spi2.partitionsFullMessages());

            topVer = new AffinityTopologyVersion(3, 3);
        }
        else
            topVer = new AffinityTopologyVersion(3, 2);

        final String CACHE_NAME2 = "cache2";

        ccfg = new CacheConfiguration();

        ccfg.setName(CACHE_NAME2);

        ignite2.createCache(ccfg);

        waitForTopologyUpdate(3, topVer);

        assertEquals(0, spi0.partitionsSingleMessages());
        assertEquals(2, spi0.partitionsFullMessages());
        assertEquals(1, spi1.partitionsSingleMessages());
        assertEquals(0, spi1.partitionsFullMessages());
        assertEquals(1, spi2.partitionsSingleMessages());
        assertEquals(0, spi2.partitionsFullMessages());
    }

    /**
     *
     */
    private static class TestFilter implements IgnitePredicate<ClusterNode> {
        /** */
        private String exclNodeName;

        /**
         * @param exclNodeName Node name to exclude.
         */
        public TestFilter(String exclNodeName) {
            this.exclNodeName = exclNodeName;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode clusterNode) {
            return !exclNodeName.equals(clusterNode.attribute(IgniteNodeAttributes.ATTR_GRID_NAME));
        }
    }

    /**
     * Test communication SPI.
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** */
        private AtomicInteger partSingleMsgs = new AtomicInteger();

        /** */
        private AtomicInteger partFullMsgs = new AtomicInteger();

        /** */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackClosure) {
            super.sendMessage(node, msg, ackClosure);

            Object msg0 = ((GridIoMessage)msg).message();

            if (msg0 instanceof GridDhtPartitionsSingleMessage) {
                if (((GridDhtPartitionsSingleMessage)msg0).exchangeId() != null) {
                    log.info("Partitions message: " + msg0.getClass().getSimpleName());

                    partSingleMsgs.incrementAndGet();
                }
            }
            else if (msg0 instanceof GridDhtPartitionsFullMessage) {
                if (((GridDhtPartitionsFullMessage)msg0).exchangeId() != null) {
                    log.info("Partitions message: " + msg0.getClass().getSimpleName());

                    partFullMsgs.incrementAndGet();
                }
            }
        }

        /**
         *
         */
        void reset() {
            partSingleMsgs.set(0);
            partFullMsgs.set(0);
        }

        /**
         * @return Sent partitions single messages.
         */
        int partitionsSingleMessages() {
            return partSingleMsgs.get();
        }

        /**
         * @return Sent partitions full messages.
         */
        int partitionsFullMessages() {
            return partFullMsgs.get();
        }
    }

}