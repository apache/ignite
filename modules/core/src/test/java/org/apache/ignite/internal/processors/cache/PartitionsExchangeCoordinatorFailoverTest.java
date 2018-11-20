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

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;

/**
 * Advanced coordinator failure scenarios during PME.
 */
public class PartitionsExchangeCoordinatorFailoverTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        IgnitePredicate<ClusterNode> nodeFilter = node -> node.consistentId().equals(igniteInstanceName);

        cfg.setCacheConfiguration(
            new CacheConfiguration("cache-" + igniteInstanceName)
                .setBackups(1)
                .setNodeFilter(nodeFilter)
                .setAffinity(new RendezvousAffinityFunction(false, 32))
        );

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
    public void testNewCoordinatorCompletedExchange() throws Exception {
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
            IgniteCache cache = grid.cache("cache-" + grid.cluster().localNode().consistentId());

            Assert.assertNotNull(cache);

            cache.put(0, 0);
        }
    }
}
