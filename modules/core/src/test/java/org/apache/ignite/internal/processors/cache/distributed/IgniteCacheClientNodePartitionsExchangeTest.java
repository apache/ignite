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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cache.affinity.fair.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.managers.communication.*;
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

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

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

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

        assertFalse(evtLatch0.await(1000, TimeUnit.MILLISECONDS));

        ignite1.close();

        assertFalse(evtLatch0.await(1000, TimeUnit.MILLISECONDS));

        ignite1 = startGrid(1);

        final CountDownLatch evtLatch1 = new CountDownLatch(1);

        ignite1.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                log.info("Rebalance event: " + evt);

                evtLatch1.countDown();

                return true;
            }
        }, EventType.EVT_CACHE_REBALANCE_STARTED, EventType.EVT_CACHE_REBALANCE_STOPPED);

        assertFalse(evtLatch0.await(1000, TimeUnit.MILLISECONDS));

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
        List<Ignite> nodes = G.allGrids();

        assertEquals(expNodes, nodes.size());

        final AffinityTopologyVersion ver = new AffinityTopologyVersion(topVer, 0);

        for (Ignite ignite : nodes) {
            final IgniteKernal kernal = (IgniteKernal)ignite;

            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return ver.equals(kernal.context().cache().context().exchange().readyAffinityVersion());
                }
            }, 10_000);

            assertEquals("Unexpected affinity version for " + ignite.name(),
                ver,
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
                    ver,
                    top.topologyVersion());
            }
        }

        awaitPartitionMapExchange();
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
        @Override public void sendMessage(ClusterNode node, Message msg) {
            super.sendMessage(node, msg);

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
