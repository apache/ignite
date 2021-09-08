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

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
@SuppressWarnings("deprecation")
public class ExchangeMergeStaleServerNodesTest extends GridCommonAbstractTest {
    /** */
    private Map<String, DelayableCommunicationSpi> commSpis;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CommunicationSpi commSpi = commSpis == null ? null : commSpis.get(igniteInstanceName);

        if (commSpi != null)
            cfg.setCommunicationSpi(commSpi);

        return cfg;
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testServersFailAfterMerge() throws Exception {
        DelayableCommunicationSpi delaySpi1 = new DelayableCommunicationSpi((msg) -> {
            if (msg instanceof GridDhtPartitionsSingleMessage) {
                GridDhtPartitionsSingleMessage singleMsg = (GridDhtPartitionsSingleMessage)msg;

                return singleMsg.exchangeId() != null && singleMsg.exchangeId().topologyVersion().equals(new AffinityTopologyVersion(2, 0));
            }

            return false;
        });

        commSpis = F.asMap(
            getTestIgniteInstanceName(0), new DelayableCommunicationSpi((msg) -> false),
            getTestIgniteInstanceName(1), delaySpi1,
            getTestIgniteInstanceName(2), new DelayableCommunicationSpi((msg) -> msg instanceof GridDhtPartitionsSingleMessage),
            getTestIgniteInstanceName(3), new DelayableCommunicationSpi((msg) -> false)
        );

        try {
            IgniteEx crd = startGrid(0);

            GridCachePartitionExchangeManager<Object, Object> exchMgr = crd.context().cache().context().exchange();

            exchMgr.mergeExchangesTestWaitVersion(new AffinityTopologyVersion(3, 0), null);

            // Single message for this node is blocked until further notice.
            IgniteInternalFuture<IgniteEx> fut = GridTestUtils.runAsync(() -> startGrid(1), "starter1");

            GridTestUtils.waitForCondition(() -> exchMgr.lastTopologyFuture().exchangeId().topologyVersion()
                .equals(new AffinityTopologyVersion(2, 0)), getTestTimeout());

            IgniteInternalFuture<IgniteEx> futFail = GridTestUtils.runAsync(() -> startGrid(2), "starter2");

            GridTestUtils.waitForCondition(exchMgr::hasPendingExchange, getTestTimeout());

            // Unblock message to proceed merging.
            delaySpi1.replay(crd.cluster().localNode().id());

            // Wait for merged exchange.
            GridTestUtils.waitForCondition(
                () -> exchMgr.mergeExchangesTestWaitVersion() == null, getTestTimeout());

            futFail.cancel();
            stopGrid(getTestIgniteInstanceName(2), true);

            fut.get();

            try {
                futFail.get();
            }
            catch (IgniteCheckedException ignore) {
                // No-op.
            }

            // Check that next nodes can successfully join topology.
            startGrid(3);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testServersFailAfterDoubleMerge() throws Exception {
        commSpis = F.asMap(
            getTestIgniteInstanceName(0), new DelayableCommunicationSpi((msg) -> false),
            getTestIgniteInstanceName(1), new DelayableCommunicationSpi((msg) -> false),
            getTestIgniteInstanceName(2), new DelayableCommunicationSpi((msg) -> false),
            getTestIgniteInstanceName(3), new DelayableCommunicationSpi((msg) -> msg instanceof GridDhtPartitionsSingleMessage),
            getTestIgniteInstanceName(4), new DelayableCommunicationSpi((msg) -> false)
        );

        try {
            IgniteEx crd = startGrid(0);

            GridCachePartitionExchangeManager<Object, Object> exchMgr = crd.context().cache().context().exchange();

            exchMgr.mergeExchangesTestWaitVersion(new AffinityTopologyVersion(4, 0), null);

            // This start will trigger an exchange.
            IgniteInternalFuture<IgniteEx> fut1 = GridTestUtils.runAsync(() -> startGrid(1), "starter1");
            // This exchange will be merged.
            IgniteInternalFuture<IgniteEx> fut2 = GridTestUtils.runAsync(() -> startGrid(2), "starter2");

            GridTestUtils.waitForCondition(() -> exchMgr.lastTopologyFuture().exchangeId().topologyVersion()
                .equals(new AffinityTopologyVersion(2, 0)), getTestTimeout());

            // This exchange will be merged as well, but the node will be failed.
            IgniteInternalFuture<IgniteEx> futFail = GridTestUtils.runAsync(() -> startGrid(3), "starter3");

            GridTestUtils.waitForCondition(exchMgr::hasPendingExchange, getTestTimeout());

            // Wait for merged exchange.
            GridTestUtils.waitForCondition(
                () -> exchMgr.mergeExchangesTestWaitVersion() == null, getTestTimeout());

            futFail.cancel();
            stopGrid(getTestIgniteInstanceName(2), true);

            fut1.get();
            fut2.get();

            try {
                futFail.get();
            }
            catch (IgniteCheckedException ignore) {
                // No-op.
            }

            // Check that next nodes can successfully join topology.
            startGrid(4);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    private static class DelayableCommunicationSpi extends TcpCommunicationSpi {
        /** */
        private ConcurrentMap<UUID, Collection<Runnable>> delayed = new ConcurrentHashMap<>();

        /** */
        private IgnitePredicate<Message> delayPred;

        /**
         * @param delayPred Delay predicate.
         */
        private DelayableCommunicationSpi(IgnitePredicate<Message> delayPred) {
            this.delayPred = delayPred;
        }

        /**
         * @param nodeId Node ID to replay.
         */
        private void replay(UUID nodeId) {
            Collection<Runnable> old = delayed.replace(nodeId, new ConcurrentLinkedDeque<>());

            if (old != null) {
                for (Runnable task : old)
                    task.run();
            }
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
            final Message msg0 = ((GridIoMessage)msg).message();

            if (delayPred.apply(msg0)) {
                delayed.computeIfAbsent(
                    node.id(),
                    (nodeId) -> new ConcurrentLinkedDeque<>()
                ).add(new Runnable() {
                    @Override public void run() {
                        DelayableCommunicationSpi.super.sendMessage(node, msg, ackC);
                    }
                });

                log.info("Delayed message: " + msg0);
            }
            else {
                try {
                    super.sendMessage(node, msg, ackC);
                }
                catch (Exception e) {
                    U.log(null, e);
                }
            }
        }
    }
}
