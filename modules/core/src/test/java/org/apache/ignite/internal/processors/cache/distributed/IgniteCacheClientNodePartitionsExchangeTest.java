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
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.ExchangeContext.IGNITE_EXCHANGE_COMPATIBILITY_VER_1;

/**
 *
 */
public class IgniteCacheClientNodePartitionsExchangeTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setForceServerMode(true);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        cfg.setCacheConfiguration(ccfg);

        cfg.setCommunicationSpi(new TestCommunicationSpi());

        cfg.setIncludeEventTypes(EventType.EVT_CACHE_REBALANCE_STARTED, EventType.EVT_CACHE_REBALANCE_STOPPED);

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
    @Test
    public void testServerNodeLeave() throws Exception {
        Ignite ignite0 = startGrid(0);

        final Ignite ignite1 = startClientGrid(1);

        waitForTopologyUpdate(2, 2);

        final Ignite ignite2 = startClientGrid(2);

        waitForTopologyUpdate(3, 3);

        ignite0.close();

        waitForTopologyUpdate(2, 4);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                ignite1.cache(DEFAULT_CACHE_NAME).get(1);

                return null;
            }
        }, CacheServerNotFoundException.class, null);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                ignite2.cache(DEFAULT_CACHE_NAME).get(1);

                return null;
            }
        }, CacheServerNotFoundException.class, null);

        ignite1.close();

        waitForTopologyUpdate(1, 5);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                ignite2.cache(DEFAULT_CACHE_NAME).get(1);

                return null;
            }
        }, CacheServerNotFoundException.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
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

        Ignite ignite1 = startClientGrid(1);

        assertFalse(evtLatch0.await(1000, TimeUnit.MILLISECONDS));

        ignite1.close();

        assertFalse(evtLatch0.await(1000, TimeUnit.MILLISECONDS));

        ignite1 = startClientGrid(1);

        final CountDownLatch evtLatch1 = new CountDownLatch(1);

        ignite1.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                log.info("Rebalance event: " + evt);

                evtLatch1.countDown();

                return true;
            }
        }, EventType.EVT_CACHE_REBALANCE_STARTED, EventType.EVT_CACHE_REBALANCE_STOPPED);

        assertFalse(evtLatch0.await(1000, TimeUnit.MILLISECONDS));

        startGrid(2);

        awaitPartitionMapExchange();

        assertFalse(evtLatch0.await(1000, TimeUnit.MILLISECONDS));
        assertFalse(evtLatch1.await(1000, TimeUnit.MILLISECONDS));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionsExchange() throws Exception {
        partitionsExchange(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionsExchangeCompatibilityMode() throws Exception {
        System.setProperty(IGNITE_EXCHANGE_COMPATIBILITY_VER_1, "true");

        try {
            partitionsExchange(true);
        }
        finally {
            System.clearProperty(IGNITE_EXCHANGE_COMPATIBILITY_VER_1);
        }
    }

    /**
     * @param compatibilityMode {@code True} to test old exchange protocol.
     * @throws Exception If failed.
     */
    private void partitionsExchange(boolean compatibilityMode) throws Exception {
        Ignite ignite0 = startGrid(0);

        TestCommunicationSpi spi0 = (TestCommunicationSpi)ignite0.configuration().getCommunicationSpi();

        Ignite ignite1 = startGrid(1);

        waitForTopologyUpdate(2, new AffinityTopologyVersion(2, 1));

        TestCommunicationSpi spi1 = (TestCommunicationSpi)ignite1.configuration().getCommunicationSpi();

        assertEquals(0, spi0.partitionsSingleMessages());
        assertEquals(2, spi0.partitionsFullMessages());

        assertEquals(2, spi1.partitionsSingleMessages());
        assertEquals(0, spi1.partitionsFullMessages());

        spi0.reset();
        spi1.reset();

        log.info("Start client node1.");

        Ignite ignite2 = startClientGrid(2);

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

        Ignite ignite3 = startClientGrid(3);

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

        Ignite ignite4 = startGrid(4);

        waitForTopologyUpdate(5, new AffinityTopologyVersion(5, 1));

        TestCommunicationSpi spi4 = (TestCommunicationSpi)ignite4.configuration().getCommunicationSpi();

        assertEquals(0, spi0.partitionsSingleMessages());
        assertEquals(8, spi0.partitionsFullMessages());

        assertEquals(2, spi1.partitionsSingleMessages());
        assertEquals(0, spi1.partitionsFullMessages());

        assertEquals(2, spi2.partitionsSingleMessages());
        assertEquals(0, spi2.partitionsFullMessages());

        assertEquals(2, spi3.partitionsSingleMessages());
        assertEquals(0, spi3.partitionsFullMessages());

        assertEquals(2, spi4.partitionsSingleMessages());
        assertEquals(0, spi4.partitionsFullMessages());

        spi0.reset();
        spi1.reset();
        spi2.reset();
        spi3.reset();

        log.info("Stop server node.");

        ignite4.close();

        if (compatibilityMode) {
            // With late affinity old protocol exchange on server leave is completed by discovery message.
            // With FairAffinityFunction affinity calculation is different, this causes one more topology change.
            boolean exchangeAfterRebalance = false;

            waitForTopologyUpdate(4,
                exchangeAfterRebalance ? new AffinityTopologyVersion(6, 1) : new AffinityTopologyVersion(6, 0));

            assertEquals(0, spi0.partitionsSingleMessages());
            assertEquals(exchangeAfterRebalance ? 3 : 0, spi0.partitionsFullMessages());

            assertEquals(exchangeAfterRebalance ? 2 : 1, spi1.partitionsSingleMessages());
            assertEquals(0, spi1.partitionsFullMessages());

            assertEquals(exchangeAfterRebalance ? 1 : 0, spi2.partitionsSingleMessages());
            assertEquals(0, spi2.partitionsFullMessages());

            assertEquals(exchangeAfterRebalance ? 1 : 0, spi3.partitionsSingleMessages());
            assertEquals(0, spi3.partitionsFullMessages());
        }
        else {
            waitForTopologyUpdate(4, 6);

            assertEquals(0, spi0.partitionsSingleMessages());
            assertEquals(3, spi0.partitionsFullMessages());

            assertEquals(1, spi1.partitionsSingleMessages());
            assertEquals(0, spi1.partitionsFullMessages());

            assertEquals(1, spi2.partitionsSingleMessages());
            assertEquals(0, spi2.partitionsFullMessages());

            assertEquals(1, spi3.partitionsSingleMessages());
            assertEquals(0, spi3.partitionsFullMessages());
        }

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

        Affinity<Integer> aff0 = ignite0.affinity(DEFAULT_CACHE_NAME);

        while (it.hasNext()) {
            Ignite ignite = it.next();

            Affinity<Integer> aff = ignite.affinity(DEFAULT_CACHE_NAME);

            assertEquals(aff0.partitions(), aff.partitions());

            for (int part = 0; part < aff.partitions(); part++)
                assertEquals(aff0.mapPartitionToPrimaryAndBackups(part), aff.mapPartitionToPrimaryAndBackups(part));
        }

        for (Ignite ignite : nodes) {
            final IgniteKernal kernal = (IgniteKernal)ignite;

            for (IgniteInternalCache cache : kernal.context().cache().caches()) {
                GridDhtPartitionTopology top = cache.context().topology();

                waitForReadyTopology(top, topVer);

                assertEquals("Unexpected topology version [node=" + ignite.name() + ", cache=" + cache.name() + ']',
                    topVer,
                    top.readyTopologyVersion());
            }
        }

        awaitPartitionMapExchange();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientOnlyCacheStart() throws Exception {
        clientOnlyCacheStart(false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNearOnlyCacheStart() throws Exception {
        clientOnlyCacheStart(true, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientOnlyCacheStartFromServerNode() throws Exception {
        clientOnlyCacheStart(false, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
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

        boolean lateAff = ignite1.configuration().isLateAffinityAssignment();

        waitForTopologyUpdate(2, new AffinityTopologyVersion(2, lateAff ? 1 : 0));

        final String CACHE_NAME1 = "cache1";

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setName(CACHE_NAME1);

        if (srvNode)
            ccfg.setNodeFilter(new TestFilter(getTestIgniteInstanceName(2)));

        ignite0.createCache(ccfg);

        Ignite ignite2;

        ignite2 = !srvNode ? startClientGrid(2) : startGrid(2);

        int minorVer = srvNode && lateAff ? 1 : 0;

        waitForTopologyUpdate(3, new AffinityTopologyVersion(3, minorVer));

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

        GridCacheAdapter cache = ((IgniteKernal)ignite2).context().cache().context().cache().internalCache(CACHE_NAME1);

        assertNotNull(cache);
        assertEquals(nearCache, cache.context().isNear());

        assertEquals(0, spi0.partitionsSingleMessages());
        assertEquals(0, spi0.partitionsFullMessages());
        assertEquals(0, spi1.partitionsSingleMessages());
        assertEquals(0, spi1.partitionsFullMessages());
        assertEquals(0, spi2.partitionsSingleMessages());
        assertEquals(0, spi2.partitionsFullMessages());

        final ClusterNode clientNode = ((IgniteKernal)ignite2).localNode();

        for (Ignite ignite : Ignition.allGrids()) {
            final GridDiscoveryManager disco = ((IgniteKernal)ignite).context().discovery();

            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return disco.cacheNode(clientNode, CACHE_NAME1);
                }
            }, 5000);

            assertTrue(disco.cacheNode(clientNode, CACHE_NAME1));

            assertFalse(disco.cacheAffinityNode(clientNode, CACHE_NAME1));
            assertEquals(nearCache, disco.cacheNearNode(clientNode, CACHE_NAME1));
        }

        spi0.reset();
        spi1.reset();
        spi2.reset();

        if (!srvNode) {
            log.info("Close client cache: " + CACHE_NAME1);

            ignite2.cache(CACHE_NAME1).close();

            assertNull(((IgniteKernal)ignite2).context().cache().context().cache().internalCache(CACHE_NAME1));

            assertEquals(0, spi0.partitionsSingleMessages());
            assertEquals(0, spi0.partitionsFullMessages());
            assertEquals(0, spi1.partitionsSingleMessages());
            assertEquals(0, spi1.partitionsFullMessages());
            assertEquals(0, spi2.partitionsSingleMessages());
            assertEquals(0, spi2.partitionsFullMessages());
        }

        final String CACHE_NAME2 = "cache2";

        ccfg = new CacheConfiguration(CACHE_NAME2);

        log.info("Create new cache: " + CACHE_NAME2);

        ignite2.createCache(ccfg);

        waitForTopologyUpdate(3, new AffinityTopologyVersion(3, ++minorVer));

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
            return !exclNodeName.equals(clusterNode.attribute(IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME));
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
