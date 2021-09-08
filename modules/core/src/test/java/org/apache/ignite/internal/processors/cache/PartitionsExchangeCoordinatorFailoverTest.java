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

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Advanced coordinator failure scenarios during PME.
 */
public class PartitionsExchangeCoordinatorFailoverTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "cache";

    /** Coordinator node name. */
    private static final String CRD_NONE = "crd";

    /** */
    private volatile Supplier<TcpCommunicationSpi> spiFactory = TcpCommunicationSpi::new;

    /** */
    private boolean newCaches = true;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setCommunicationSpi(spiFactory.get().setName("tcp"));

        cfg.setCacheConfiguration(
                new CacheConfiguration(CACHE_NAME)
                    .setBackups(2)
                    .setAffinity(new RendezvousAffinityFunction(false, 32))
        );

        // Add cache that exists only on coordinator node.
        if (newCaches && igniteInstanceName.equals(CRD_NONE)) {
            IgnitePredicate<ClusterNode> nodeFilter = node -> node.consistentId().equals(igniteInstanceName);

            cfg.setCacheConfiguration(
                    new CacheConfiguration(CACHE_NAME + 0)
                            .setBackups(2)
                            .setNodeFilter(nodeFilter)
                            .setAffinity(new RendezvousAffinityFunction(false, 32))
            );
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 60 * 1000L;
    }

    /**
     * Tests that new coordinator is able to finish old exchanges in case of in-complete coordinator initialization.
     */
    @Test
    public void testNewCoordinatorCompletedExchange() throws Exception {
        spiFactory = TestRecordingCommunicationSpi::new;

        IgniteEx crd = startGrid(CRD_NONE);

        IgniteEx newCrd = startGrid(1);

        crd.cluster().active(true);

        // 3 node join topology version.
        AffinityTopologyVersion joinThirdNodeVer = new AffinityTopologyVersion(3, 0);

        // 4 node join topology version.
        AffinityTopologyVersion joinFourNodeVer = new AffinityTopologyVersion(4, 0);

        // Block FullMessage for newly joined nodes.
        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(crd);

        final CountDownLatch sndFullMsgLatch = new CountDownLatch(1);

        // Delay sending full message to newly joined nodes.
        spi.blockMessages((node, msg) -> {
            if (msg instanceof GridDhtPartitionsFullMessage && node.order() > 2) {
                try {
                    sndFullMsgLatch.await();
                }
                catch (Throwable ignored) { }

                return true;
            }

            return false;
        });

        IgniteInternalFuture joinTwoNodesFut = GridTestUtils.runAsync(() -> startGridsMultiThreaded(2, 2));

        GridCachePartitionExchangeManager exchangeMgr = newCrd.context().cache().context().exchange();

        // Wait till new coordinator finishes third node join exchange.
        GridTestUtils.waitForCondition(
            () -> exchangeMgr.readyAffinityVersion().compareTo(joinThirdNodeVer) >= 0,
            getTestTimeout()
        );

        IgniteInternalFuture startLastNodeFut = GridTestUtils.runAsync(() -> startGrid(5));

        // Wait till new coordinator starts third node join exchange.
        GridTestUtils.waitForCondition(
            () -> exchangeMgr.lastTopologyFuture().initialVersion().compareTo(joinFourNodeVer) >= 0,
            getTestTimeout()
        );

        IgniteInternalFuture stopCrdFut = GridTestUtils.runAsync(() -> stopGrid(CRD_NONE, true, false));

        // Magic sleep to make sure that coordinator stop process has started.
        U.sleep(1000);

        // Resume full messages sending to unblock coordinator stopping process.
        sndFullMsgLatch.countDown();

        // Coordinator stop should succeed.
        stopCrdFut.get();

        // Nodes join should succeed.
        joinTwoNodesFut.get();

        startLastNodeFut.get();

        awaitPartitionMapExchange();

        // Check that all caches are operable.
        for (Ignite grid : G.allGrids()) {
            IgniteCache cache = grid.cache(CACHE_NAME);

            Assert.assertNotNull(cache);

            cache.put(0, 0);
        }
    }

    /**
     * Test checks that delayed full messages are processed correctly in case of changed coordinator.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDelayedFullMessageReplacedIfCoordinatorChanged() throws Exception {
        spiFactory = TestRecordingCommunicationSpi::new;

        IgniteEx crd = startGrid(CRD_NONE);

        IgniteEx newCrd = startGrid(1);

        IgniteEx problemNode = startGrid(2);

        crd.cluster().active(true);

        awaitPartitionMapExchange();

        blockSendingFullMessage(crd, node -> node.equals(problemNode.localNode()));

        IgniteInternalFuture joinNextNodeFut = GridTestUtils.runAsync(() -> startGrid(3));

        joinNextNodeFut.get();

        U.sleep(5000);

        blockSendingFullMessage(newCrd, node -> node.equals(problemNode.localNode()));

        IgniteInternalFuture stopCrdFut = GridTestUtils.runAsync(() -> stopGrid(CRD_NONE));

        stopCrdFut.get();

        U.sleep(5000);

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(newCrd);

        spi.stopBlock(true);

        awaitPartitionMapExchange();
    }

    /**
     * Test that exchange coordinator initialized correctly in case of exchanges merge and caches without affinity nodes.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCoordinatorChangeAfterExchangesMerge() throws Exception {
        // Delay demand messages sending to suspend late affinity assignment.
        spiFactory = () -> new DynamicDelayingCommunicationSpi(msg -> {
            final int delay = 5_000;

            if (msg instanceof GridDhtPartitionDemandMessage) {
                GridDhtPartitionDemandMessage demandMsg = (GridDhtPartitionDemandMessage) msg;

                if (demandMsg.groupId() == GridCacheUtils.cacheId(GridCacheUtils.UTILITY_CACHE_NAME))
                    return 0;

                return delay;
            }

            return 0;
        });

        final IgniteEx crd = startGrid(CRD_NONE);

        startGrid(1);

        for (int k = 0; k < 1024; k++)
            crd.cache(CACHE_NAME).put(k, k);

        // Delay sending single messages to ensure exchanges are merged.
        spiFactory = () -> new DynamicDelayingCommunicationSpi(msg -> {
            final int delay = 1_000;

            if (msg instanceof GridDhtPartitionsSingleMessage) {
                GridDhtPartitionsSingleMessage singleMsg = (GridDhtPartitionsSingleMessage) msg;

                if (singleMsg.exchangeId() != null)
                    return delay;
            }

            return 0;
        });

        // This should trigger exchanges merge.
        startGridsMultiThreaded(2, 2);

        // Delay sending single message from new node to have time to shutdown coordinator.
        spiFactory = () -> new DynamicDelayingCommunicationSpi(msg -> {
            final int delay = 5_000;

            if (msg instanceof GridDhtPartitionsSingleMessage) {
                GridDhtPartitionsSingleMessage singleMsg = (GridDhtPartitionsSingleMessage) msg;

                if (singleMsg.exchangeId() != null)
                    return delay;
            }

            return 0;
        });

        // Trigger next exchange.
        IgniteInternalFuture startNodeFut = GridTestUtils.runAsync(() -> startGrid(4));

        // Wait till other nodes will send their messages to coordinator.
        U.sleep(2_500);

        // And then stop coordinator node.
        stopGrid(CRD_NONE, true);

        startNodeFut.get();

        awaitPartitionMapExchange();

        // Check that all caches are operable.
        for (Ignite grid : G.allGrids()) {
            IgniteCache cache = grid.cache(CACHE_NAME);

            Assert.assertNotNull(cache);

            for (int k = 0; k < 1024; k++)
                Assert.assertEquals(k, cache.get(k));

            for (int k = 0; k < 1024; k++)
                cache.put(k, k);
        }
    }

    /**
     * Test checks that changing coordinator to a node that joining to cluster at the moment works correctly
     * in case of exchanges merge and completed exchange on other joining nodes.
     */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK, value = "true")
    public void testChangeCoordinatorToLocallyJoiningNode() throws Exception {
        newCaches = false;

        spiFactory = TestRecordingCommunicationSpi::new;

        IgniteEx crd = startGrid(CRD_NONE);

        final int newCrdNodeIdx = 1;

        // A full message shouldn't be send to new coordinator.
        blockSendingFullMessage(crd, node -> node.consistentId().equals(getTestIgniteInstanceName(newCrdNodeIdx)));

        CountDownLatch joiningNodeSentSingleMsg = new CountDownLatch(1);

        // For next joining node delay sending single message to emulate exchanges merge.
        spiFactory = () -> new DynamicDelayingCommunicationSpi(msg -> {
            final int delay = 5_000;

            if (msg instanceof GridDhtPartitionsSingleMessage) {
                GridDhtPartitionsSingleMessage singleMsg = (GridDhtPartitionsSingleMessage) msg;

                if (singleMsg.exchangeId() != null) {
                    joiningNodeSentSingleMsg.countDown();

                    return delay;
                }
            }

            return 0;
        });

        IgniteInternalFuture<?> newCrdJoinFut = GridTestUtils.runAsync(() -> startGrid(newCrdNodeIdx));

        // Wait till new coordinator node sent single message.
        joiningNodeSentSingleMsg.await();

        spiFactory = TcpCommunicationSpi::new;

        // Additionally start 2 new nodes. Their exchange should be merged with exchange on join new coordinator node.
        startGridsMultiThreaded(2, 2);

        Assert.assertFalse("New coordinator join shouldn't be happened before stopping old coordinator.",
            newCrdJoinFut.isDone());

        // Stop coordinator.
        stopGrid(CRD_NONE);

        // New coordinator join process should succeed after that.
        newCrdJoinFut.get();

        awaitPartitionMapExchange();

        // Check that affinity are equal on all nodes.
        AffinityTopologyVersion affVer = ((IgniteEx) ignite(1)).cachex(CACHE_NAME)
            .context().shared().exchange().readyAffinityVersion();

        List<List<ClusterNode>> expAssignment = null;
        IgniteEx expAssignmentNode = null;

        for (Ignite node : G.allGrids()) {
            IgniteEx nodeEx = (IgniteEx) node;

            List<List<ClusterNode>> assignment = nodeEx.cachex(CACHE_NAME).context().affinity().assignments(affVer);

            if (expAssignment == null) {
                expAssignment = assignment;
                expAssignmentNode = nodeEx;
            }
            else
                Assert.assertEquals("Affinity assignments are different " +
                    "[expectedNode=" + expAssignmentNode + ", actualNode=" + nodeEx + "]", expAssignment, assignment);
        }
    }

    /**
     * Test checks that changing coordinator to a node that joining to cluster at the moment works correctly
     * in case of completed exchange on client nodes.
     */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK, value = "true")
    public void testChangeCoordinatorToLocallyJoiningNode2() throws Exception {
        newCaches = false;

        spiFactory = TestRecordingCommunicationSpi::new;

        IgniteEx crd = startGrid(CRD_NONE);

        // Start several clients.
        IgniteEx clientNode = (IgniteEx)startClientGridsMultiThreaded(2, 2);

        awaitPartitionMapExchange();

        final int newCrdNodeIdx = 1;

        // A full message shouldn't be send to new coordinator.
        blockSendingFullMessage(crd, node -> node.consistentId().equals(getTestIgniteInstanceName(newCrdNodeIdx)));

        IgniteInternalFuture<?> newCrdJoinFut = GridTestUtils.runAsync(() -> startGrid(newCrdNodeIdx));

        // Wait till client node will receive full message and finish exchange on node join.
        GridTestUtils.waitForCondition(() -> {
                GridDhtPartitionsExchangeFuture fut = clientNode.cachex(CACHE_NAME)
                    .context().shared().exchange().lastFinishedFuture();

                return fut != null && fut.topologyVersion().equals(new AffinityTopologyVersion(4, 0));
            }, 60_000
        );

        Assert.assertFalse("New coordinator join shouldn't be happened before stopping old coordinator.",
            newCrdJoinFut.isDone());

        // Stop coordinator.
        stopGrid(CRD_NONE);

        // New coordinator join process should succeed after that.
        newCrdJoinFut.get();

        awaitPartitionMapExchange();
    }

    /**
     * Blocks sending full message from coordinator to non-coordinator node.
     *
     * @param from Coordinator node.
     * @param pred Non-coordinator node predicate.
     *                  If predicate returns {@code true} a full message will not be send to that node.
     */
    private void blockSendingFullMessage(IgniteEx from, Predicate<ClusterNode> pred) {
        // Block FullMessage for newly joined nodes.
        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(from);

        // Delay sending full messages (without exchange id).
        spi.blockMessages((node, msg) -> {
            if (msg instanceof GridDhtPartitionsFullMessage) {
                GridDhtPartitionsFullMessage fullMsg = (GridDhtPartitionsFullMessage) msg;

                if (fullMsg.exchangeId() != null && pred.test(node)) {
                    log.warning("Blocked sending " + msg + " to " + node);

                    return true;
                }
            }

            return false;
        });
    }

    /**
     * Communication SPI that allows to delay sending message by predicate.
     */
    static class DynamicDelayingCommunicationSpi extends TcpCommunicationSpi {
        /** Function that returns delay in milliseconds for given message. */
        private final Function<Message, Integer> delayMsgFunc;

        /** */
        DynamicDelayingCommunicationSpi() {
            this(msg -> 0);
        }

        /**
         * @param delayMsgFunc Function to calculate delay for message.
         */
        DynamicDelayingCommunicationSpi(final Function<Message, Integer> delayMsgFunc) {
            this.delayMsgFunc = delayMsgFunc;
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
                throws IgniteSpiException {
            try {
                GridIoMessage ioMsg = (GridIoMessage)msg;

                int delay = delayMsgFunc.apply(ioMsg.message());

                if (delay > 0) {
                    log.warning(String.format("Delay sending %s to %s", msg, node));

                    U.sleep(delay);
                }
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new IgniteSpiException(e);
            }

            super.sendMessage(node, msg, ackC);
        }
    }
}
