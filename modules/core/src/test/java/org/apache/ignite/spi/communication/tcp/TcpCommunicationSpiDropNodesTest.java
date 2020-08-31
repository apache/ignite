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
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;

/**
 * Tests grid node kicking on communication failure.
 */
public class TcpCommunicationSpiDropNodesTest extends GridCommonAbstractTest {
    /** Nodes count. */
    private static final int NODES_CNT = 4;

    /** Block. */
    private static volatile boolean block;

    /** Predicate. */
    private static IgniteBiPredicate<ClusterNode, ClusterNode> pred;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setFailureDetectionTimeout(1000);

        TestCommunicationSpi spi = new TestCommunicationSpi();

        spi.setIdleConnectionTimeout(100);
        spi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(spi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        System.setProperty(IgniteSystemProperties.IGNITE_ENABLE_FORCIBLE_NODE_KILL,"true");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        System.clearProperty(IgniteSystemProperties.IGNITE_ENABLE_FORCIBLE_NODE_KILL);
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
     * Server node shouldn't be failed by other server node if IGNITE_ENABLE_FORCIBLE_NODE_KILL=true.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testOneNode() throws Exception {
        pred = new IgniteBiPredicate<ClusterNode, ClusterNode>() {
            @Override public boolean apply(ClusterNode locNode, ClusterNode rmtNode) {
                return block && rmtNode.order() == 3;
            }
        };

        startGrids(NODES_CNT);

        AtomicInteger evts = new AtomicInteger();

        grid(0).events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                evts.incrementAndGet();

                return true;
            }
        }, EVT_NODE_FAILED);

        U.sleep(1000); // Wait for write timeout and closing idle connections.

        block = true;

        try {
            grid(0).compute().broadcast(new IgniteRunnable() {
                @Override public void run() {
                    // No-op.
                }
            });

            fail("Should have exception here.");
        } catch (IgniteException e) {
            assertTrue(e.getCause() instanceof IgniteSpiException);
        }

        block = false;

        assertEquals(NODES_CNT, grid(0).cluster().nodes().size());
        assertEquals(0, evts.get());
    }

    /**
     * Servers shouldn't fail each other if IGNITE_ENABLE_FORCIBLE_NODE_KILL=true.
     * @throws Exception If failed.
     */
    @Test
    public void testTwoNodesEachOther() throws Exception {
        pred = new IgniteBiPredicate<ClusterNode, ClusterNode>() {
            @Override public boolean apply(ClusterNode locNode, ClusterNode rmtNode) {
                return block && (locNode.order() == 2 || locNode.order() == 4) &&
                    (rmtNode.order() == 2 || rmtNode.order() == 4);
            }
        };

        startGrids(NODES_CNT);

        AtomicInteger evts = new AtomicInteger();

        grid(0).events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                evts.incrementAndGet();

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

        try {
            fut1.get();

            fail("Should fail with SpiException");
        }
        catch (IgniteCheckedException e) {
            assertTrue(e.getCause().getCause() instanceof IgniteSpiException);
        }

        try {
            fut2.get();

            fail("Should fail with SpiException");
        }
        catch (IgniteCheckedException e) {
            assertTrue(e.getCause().getCause() instanceof IgniteSpiException);
        }

        assertEquals(NODES_CNT, grid(0).cluster().nodes().size());
        assertEquals(0, evts.get());

        for (int j = 0; j < NODES_CNT; j++) {
            IgniteEx ignite;

            try {
                ignite = grid(j);

                log.info("Checking topology for grid(" + j + "): " + ignite.cluster().nodes());

                assertEquals(NODES_CNT, ignite.cluster().nodes().size());
            }
            catch (Exception e) {
                log.info("Checking topology for grid(" + j + "): no grid in topology.");
            }
        }
    }

    /**
     *
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override protected GridCommunicationClient createTcpClient(ClusterNode node, int connIdx)
            throws IgniteCheckedException {
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
