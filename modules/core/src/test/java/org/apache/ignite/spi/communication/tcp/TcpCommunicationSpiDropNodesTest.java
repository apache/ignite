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

package org.apache.ignite.spi.communication.tcp;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;

/**
 *
 */
public class TcpCommunicationSpiDropNodesTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Nodes count. */
    private static final int NODES_CNT = 4;

    /** Block. */
    private static volatile boolean block;

    /** Predicate. */
    private static IgniteBiPredicate<ClusterNode, ClusterNode> pred;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setClockSyncFrequency(300000);
        cfg.setFailureDetectionTimeout(1000);

        TestCommunicationSpi spi = new TestCommunicationSpi();

        spi.setIdleConnectionTimeout(100);
        spi.setSharedMemoryPort(-1);

        TcpDiscoverySpi discoSpi = (TcpDiscoverySpi) cfg.getDiscoverySpi();
        discoSpi.setIpFinder(IP_FINDER);

        cfg.setCommunicationSpi(spi);
        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        block = false;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testOneNode() throws Exception {
        pred = new IgniteBiPredicate<ClusterNode, ClusterNode>() {
            @Override public boolean apply(ClusterNode locNode, ClusterNode rmtNode) {
                return block && rmtNode.order() == 3;
            }
        };

        startGrids(NODES_CNT);

        final CountDownLatch latch = new CountDownLatch(1);

        grid(0).events().localListen(new IgnitePredicate<Event>() {
            @Override
            public boolean apply(Event event) {
                latch.countDown();

                return true;
            }
        }, EVT_NODE_FAILED);

        U.sleep(1000); // Wait for write timeout and closing idle connections.

        block = true;

        grid(0).compute().broadcast(new IgniteRunnable() {
            @Override public void run() {
                // No-op.
            }
        });

        assertTrue(latch.await(15, TimeUnit.SECONDS));

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return grid(3).cluster().topologyVersion() == NODES_CNT + 1;
            }
        }, 5000));

        for (int i = 0; i < 10; i++) {
            U.sleep(1000);

            assertEquals(NODES_CNT - 1, grid(0).cluster().nodes().size());

            int liveNodesCnt = 0;

            for (int j = 0; j < NODES_CNT; j++) {
                IgniteEx ignite;

                try {
                    ignite = grid(j);

                    log.info("Checking topology for grid(" + j + "): " + ignite.cluster().nodes());

                    ClusterNode locNode = ignite.localNode();

                    if (locNode.order() != 3) {
                        assertEquals(NODES_CNT - 1, ignite.cluster().nodes().size());

                        for (ClusterNode node : ignite.cluster().nodes())
                            assertTrue(node.order() != 3);

                        liveNodesCnt++;
                    }
                }
                catch (Exception e) {
                    log.info("Checking topology for grid(" + j + "): no grid in topology.");
                }
            }

            assertEquals(NODES_CNT - 1, liveNodesCnt);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTwoNodesEachOther() throws Exception {
        pred = new IgniteBiPredicate<ClusterNode, ClusterNode>() {
            @Override public boolean apply(ClusterNode locNode, ClusterNode rmtNode) {
                return block && (locNode.order() == 2 || locNode.order() == 4) &&
                    (rmtNode.order() == 2 || rmtNode.order() == 4);
            }
        };

        startGrids(NODES_CNT);

        final CountDownLatch latch = new CountDownLatch(1);

        grid(0).events().localListen(new IgnitePredicate<Event>() {
            @Override
            public boolean apply(Event event) {
                latch.countDown();

                return true;
            }
        }, EVT_NODE_FAILED);

        U.sleep(1000); // Wait for write timeout and closing idle connections.

        block = true;

        final CyclicBarrier barrier = new CyclicBarrier(2);

        IgniteInternalFuture<Void> fut1 = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                barrier.await();

                grid(1).compute().withNoFailover().broadcast(new IgniteRunnable() {
                    @Override public void run() {
                        // No-op.
                    }
                });

                return null;
            }
        });

        IgniteInternalFuture<Void> fut2 = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                barrier.await();

                grid(3).compute().withNoFailover().broadcast(new IgniteRunnable() {
                    @Override public void run() {
                        // No-op.
                    }
                });

                return null;
            }
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return grid(2).cluster().nodes().size() == NODES_CNT - 1;
            }
        }, 5000);

        try {
            fut1.get();
        }
        catch (IgniteCheckedException e) {
            // No-op.
        }

        try {
            fut2.get();
        }
        catch (IgniteCheckedException e) {
            // No-op.
        }

        long failedNodeOrder = 1 + 2 + 3 + 4;

        for (ClusterNode node : grid(0).cluster().nodes())
            failedNodeOrder -= node.order();

        for (int i = 0; i < 10; i++) {
            U.sleep(1000);

            assertEquals(NODES_CNT - 1, grid(0).cluster().nodes().size());

            int liveNodesCnt = 0;

            for (int j = 0; j < NODES_CNT; j++) {
                IgniteEx ignite;

                try {
                    ignite = grid(j);

                    log.info("Checking topology for grid(" + j + "): " + ignite.cluster().nodes());

                    ClusterNode locNode = ignite.localNode();

                    if (locNode.order() != failedNodeOrder) {
                        assertEquals(NODES_CNT - 1, ignite.cluster().nodes().size());

                        for (ClusterNode node : ignite.cluster().nodes())
                            assertTrue(node.order() != failedNodeOrder);

                        liveNodesCnt++;
                    }
                }
                catch (Exception e) {
                    log.info("Checking topology for grid(" + j + "): no grid in topology.");
                }
            }

            assertEquals(NODES_CNT - 1, liveNodesCnt);
        }
    }

    /**
     *
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override protected GridCommunicationClient createTcpClient(ClusterNode node, int connIdx) throws IgniteCheckedException {
            if (pred.apply(getLocalNode(), node)) {
                Map<String, Object> attrs = new HashMap<>(node.attributes());

                attrs.put(createAttributeName(ATTR_ADDRS), Collections.singleton("127.0.0.1"));
                attrs.put(createAttributeName(ATTR_PORT), 47200);
                attrs.put(createAttributeName(ATTR_EXT_ADDRS), Collections.emptyList());
                attrs.put(createAttributeName(ATTR_HOST_NAMES), Collections.emptyList());

                ((TcpDiscoveryNode)node).setAttributes(attrs);
            }

            return super.createTcpClient(node, connIdx);
        }

        /**
         * @param name Name.
         */
        private String createAttributeName(String name) {
            return getClass().getSimpleName() + '.' + name;
        }
    }
}
