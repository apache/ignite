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

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 *
 */
public class IgniteRebalanceScheduleResendPartitionsTest extends GridCommonAbstractTest {
    /** */
    public static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        cfg.setConsistentId(name);

        cfg.setAutoActivationEnabled(false);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                )
        );

        cfg.setCacheConfiguration(
            new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setAffinity(
                    new RendezvousAffinityFunction(false, 32))
                .setBackups(1)
        );

        cfg.setCommunicationSpi(new BlockTcpCommunicationSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * https://issues.apache.org/jira/browse/IGNITE-8684
     *
     * @throws Exception If failed.
     */
    public void test() throws Exception {
        Ignite ig0 = startGrids(3);

        ig0.cluster().active(true);

        int entries = 100_000;

        try (IgniteDataStreamer<Integer, Integer> st = ig0.dataStreamer(DEFAULT_CACHE_NAME)) {
            st.allowOverwrite(true);

            for (int i = 0; i < entries; i++)
                st.addData(i, -i);
        }

        IgniteEx ig3 = startGrid(3);

        AtomicInteger cnt = new AtomicInteger();

        CountDownLatch latch = new CountDownLatch(1);

        runAsync(() -> {
            try {
                latch.await();

                // Sleep should be less that full awaitPartitionMapExchange().
                Thread.sleep(super.getPartitionMapExchangeTimeout());

                log.info("Await completed, continue rebalance.");

                unwrapSPI(ig3).resume();
            }
            catch (InterruptedException ignored) {
                // No-op.
            }
        });

        // Compare previous single message with current.
        MessageComparator prevMessageComparator = new MessageComparator();

        // Pause rebalance and count of single map messages.
        unwrapSPI(ig3).pause(GridDhtPartitionDemandMessage.class, (msg) -> {
            System.out.println("Send partition single message:" + msg);

            // Continue after baseline changed exchange occurred.
            if (msg.exchangeId() != null)
                latch.countDown();

            if (prevMessageComparator.prevEquals(msg))
                cnt.incrementAndGet();
        });

        ig3.cluster().setBaselineTopology(ig3.context().discovery().topologyVersion());

        awaitPartitionMapExchange();

        // We should not send equals single map on schedule partition state.
        Assert.assertEquals(0, cnt.get());

        IgniteCache<Integer, Integer> cache = ig3.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < entries; i++) {
            Integer val = cache.get(i);

            Assert.assertEquals(-i, (int)val);
        }
    }

    /** {@inheritDoc} */
    @Override protected long getPartitionMapExchangeTimeout() {
        return super.getPartitionMapExchangeTimeout() * 2;
    }

    /**
     * @param ig Ignite.
     * @return BlockTcpCommunicationSpi.
     */
    private BlockTcpCommunicationSpi unwrapSPI(IgniteEx ig) {
        return ((BlockTcpCommunicationSpi)ig.configuration().getCommunicationSpi());
    }

    /**
     * Compare previous single message with current.
     */
    private static class MessageComparator {
        /** */
        private GridDhtPartitionsSingleMessage prev;

        /**
         * @param msg Partition single message.
         */
        private synchronized boolean prevEquals(GridDhtPartitionsSingleMessage msg) {
            if (msg.exchangeId() != null)
                return false;

            if (prev == null) {
                prev = msg;

                return false;
            }

            AtomicBoolean prevEquals = new AtomicBoolean(true);

            prev.partitions().forEach((k0, v0) -> {
                GridDhtPartitionMap val1 = msg.partitions().get(k0);

                if (val1 == null)
                    prevEquals.set(false);

                boolean equals = v0.map().equals(val1.map());

                prevEquals.set(prevEquals.get() && equals);
            });

            prev = msg;

            return prevEquals.get();
        }
    }

    /**
     *
     */
    protected static class BlockTcpCommunicationSpi extends TcpCommunicationSpi {
        private volatile IgniteInClosure<GridDhtPartitionsSingleMessage> cls;

        /** */
        private volatile Class msgCls;

        /** */
        private final Queue<T3<ClusterNode, Message, IgniteInClosure>> queue = new ConcurrentLinkedQueue<>();

        /** */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
            throws IgniteSpiException {
            Class msgCls0 = msgCls;

            if (msgCls0 != null && ((GridIoMessage)msg).message().getClass().equals(msgCls0)) {
                queue.add(new T3<>(node, msg, ackC));

                log.info("Block message: " + msg);

                return;
            }

            if (((GridIoMessage)msg).message().getClass().equals(GridDhtPartitionsSingleMessage.class)) {
                if (cls != null)
                    cls.apply((GridDhtPartitionsSingleMessage)((GridIoMessage)msg).message());
            }

            super.sendMessage(node, msg, ackC);
        }

        /**
         * @param msgCls Message class.
         */
        private synchronized void pause(Class msgCls, IgniteInClosure<GridDhtPartitionsSingleMessage> cls) {
            this.msgCls = msgCls;
            this.cls = cls;
        }

        /**
         *
         */
        private synchronized void resume() {
            msgCls = null;

            for (T3<ClusterNode, Message, IgniteInClosure> msg : queue)
                super.sendMessage(msg.get1(), msg.get2(), msg.get3());

            queue.clear();
        }
    }
}
