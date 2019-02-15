/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
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
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Advanced coordinator failure scenarios during PME.
 */
public class PartitionsExchangeCoordinatorFailoverTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    private Supplier<CommunicationSpi> spiFactory = TcpCommunicationSpi::new;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setCommunicationSpi(spiFactory.get());

        cfg.setCacheConfiguration(
                new CacheConfiguration(CACHE_NAME)
                    .setBackups(2)
                    .setAffinity(new RendezvousAffinityFunction(false, 32))
        );

        // Add cache that exists only on coordinator node.
        if (igniteInstanceName.equals("crd")) {
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

        IgniteEx crd = (IgniteEx) startGrid("crd");

        IgniteEx newCrd = startGrid(1);

        crd.cluster().active(true);

        // 3 node join topology version.
        AffinityTopologyVersion joinThirdNodeVer = new AffinityTopologyVersion(3, 0);

        // 4 node join topology version.
        AffinityTopologyVersion joinFourNodeVer = new AffinityTopologyVersion(4, 0);

        // Block FullMessage for newly joined nodes.
        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(crd);

        final CountDownLatch sendFullMsgLatch = new CountDownLatch(1);

        // Delay sending full message to newly joined nodes.
        spi.blockMessages((node, msg) -> {
            if (msg instanceof GridDhtPartitionsFullMessage && node.order() > 2) {
                try {
                    sendFullMsgLatch.await();
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

        IgniteInternalFuture stopCrdFut = GridTestUtils.runAsync(() -> stopGrid("crd", true, false));

        // Magic sleep to make sure that coordinator stop process has started.
        U.sleep(1000);

        // Resume full messages sending to unblock coordinator stopping process.
        sendFullMsgLatch.countDown();

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

        IgniteEx crd = startGrid("crd");

        IgniteEx newCrd = startGrid(1);

        IgniteEx problemNode = startGrid(2);

        crd.cluster().active(true);

        awaitPartitionMapExchange();

        blockSendingFullMessage(crd, problemNode);

        IgniteInternalFuture joinNextNodeFut = GridTestUtils.runAsync(() -> startGrid(3));

        joinNextNodeFut.get();

        U.sleep(5000);

        blockSendingFullMessage(newCrd, problemNode);

        IgniteInternalFuture stopCoordinatorFut = GridTestUtils.runAsync(() -> stopGrid("crd"));

        stopCoordinatorFut.get();

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
                GridDhtPartitionDemandMessage demandMessage = (GridDhtPartitionDemandMessage) msg;

                if (demandMessage.groupId() == GridCacheUtils.cacheId(GridCacheUtils.UTILITY_CACHE_NAME))
                    return 0;

                return delay;
            }

            return 0;
        });

        final IgniteEx crd = startGrid("crd");

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
        stopGrid("crd", true);

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
     * Blocks sending full message from coordinator to non-coordinator node.
     * @param from Coordinator node.
     * @param to Non-coordinator node.
     */
    private void blockSendingFullMessage(IgniteEx from, IgniteEx to) {
        // Block FullMessage for newly joined nodes.
        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(from);

        // Delay sending full messages (without exchange id).
        spi.blockMessages((node, msg) -> {
            if (msg instanceof GridDhtPartitionsFullMessage) {
                GridDhtPartitionsFullMessage fullMsg = (GridDhtPartitionsFullMessage) msg;

                if (fullMsg.exchangeId() != null && node.order() == to.localNode().order()) {
                    log.warning("Blocked sending " + msg + " to " + to.localNode());

                    return true;
                }
            }

            return false;
        });
    }

    /**
     * Communication SPI that allows to delay sending message by predicate.
     */
    class DynamicDelayingCommunicationSpi extends TcpCommunicationSpi {
        /** Function that returns delay in milliseconds for given message. */
        private final Function<Message, Integer> delayMessageFunc;

        /** */
        DynamicDelayingCommunicationSpi() {
            this(msg -> 0);
        }

        /**
         * @param delayMessageFunc Function to calculate delay for message.
         */
        DynamicDelayingCommunicationSpi(final Function<Message, Integer> delayMessageFunc) {
            this.delayMessageFunc = delayMessageFunc;
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
                throws IgniteSpiException {
            try {
                GridIoMessage ioMsg = (GridIoMessage)msg;

                int delay = delayMessageFunc.apply(ioMsg.message());

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
