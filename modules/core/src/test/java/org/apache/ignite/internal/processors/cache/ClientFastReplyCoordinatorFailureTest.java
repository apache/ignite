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
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeOrHaltFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Test;

/**
 * Covers race with client join and instant successive coordinator change.
 */
public class ClientFastReplyCoordinatorFailureTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Least significant bits of old coordinator's node ID. */
    public static final int OLD_CRD_BITS = 0xFFFF;

    /** Latch that will be triggered after blocking message from client to old coordinator. */
    private final CountDownLatch clientSingleMesssageLatch = new CountDownLatch(1);

    /** Latch that will be triggered after blocking message from new server to old coordinator. */
    private final CountDownLatch newSrvSingleMesssageLatch = new CountDownLatch(1);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setFailureHandler(new StopNodeOrHaltFailureHandler());

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();

        // Block messages to old coordinator right before killing it.
        if (igniteInstanceName.contains("client")) {
            commSpi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode node, Message msg) {
                    if (msg instanceof GridDhtPartitionsSingleMessage &&
                        (node.id().getLeastSignificantBits() & OLD_CRD_BITS) == 0) {
                        info("Going to block message [node=" + node + ", msg=" + msg + ']');

                        clientSingleMesssageLatch.countDown();

                        return true;
                    }

                    return false;
                }
            });
        }
        else if (getTestIgniteInstanceName(3).equals(igniteInstanceName)) {
            commSpi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode node, Message msg) {
                    if (msg instanceof GridDhtPartitionsSingleMessage &&
                        (node.id().getLeastSignificantBits() & OLD_CRD_BITS) == 0L) {
                        info("Going to block message [node=" + node + ", msg=" + msg + ']');

                        newSrvSingleMesssageLatch.countDown();

                        return true;
                    }

                    return false;
                }
            });
        }

        cfg.setCommunicationSpi(commSpi);

        return cfg;
    }

    /**
     * Cleanup after test.
     */
    @After
    public void cleanUp() {
        stopAllGrids();
    }

    /**
     * Checks that new coordinator will respond to client single partitions message.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testClientFastReply() throws Exception {
        startGrids(3);

        awaitPartitionMapExchange();

        // Client join will be hanging on local join exchange.
        IgniteInternalFuture<Ignite> startFut = GridTestUtils.runAsync(() -> startClientGrid("client-1"));

        clientSingleMesssageLatch.await();

        // Server start will be blocked.
        IgniteInternalFuture<IgniteEx> srvStartFut = GridTestUtils.runAsync(() -> startGrid(3));

        newSrvSingleMesssageLatch.await();

        stopGrid(0);

        srvStartFut.get();

        startFut.get();
    }
}
