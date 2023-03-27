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

package org.apache.ignite.spi.discovery.zk.internal;

import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.zookeeper.ZkTestClientCnxnSocketNIO;
import org.junit.Test;

import static org.apache.ignite.spi.discovery.zk.internal.ZookeeperDiscoveryImpl.IGNITE_ZOOKEEPER_DISCOVERY_SPI_MAX_EVTS;

/**
 * Tests for Zookeeper SPI discovery.
 */
public class ZookeeperDiscoveryConcurrentStartAndStartStopTest extends ZookeeperDiscoverySpiTestBase {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentStartWithClient() throws Exception {
        final int NODES = 20;

        for (int i = 0; i < 3; i++) {
            info("Iteration: " + i);

            final int srvIdx = ThreadLocalRandom.current().nextInt(NODES);

            final AtomicInteger idx = new AtomicInteger();

            GridTestUtils.runMultiThreaded(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    int threadIdx = idx.getAndIncrement();

                    if (threadIdx == srvIdx || ThreadLocalRandom.current().nextBoolean())
                        startClientGrid(threadIdx);
                    else
                        startGrid(threadIdx);

                    return null;
                }
            }, NODES, "start-node");

            waitForTopology(NODES);

            stopAllGrids();

            checkEventsConsistency();

            evts.clear();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentStart() throws Exception {
        final int NODES = 20;

        for (int i = 0; i < 3; i++) {
            info("Iteration: " + i);

            final AtomicInteger idx = new AtomicInteger();

            final CyclicBarrier b = new CyclicBarrier(NODES);

            GridTestUtils.runMultiThreaded(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    b.await();

                    int threadIdx = idx.getAndIncrement();

                    startGrid(threadIdx);

                    return null;
                }
            }, NODES, "start-node");

            waitForTopology(NODES);

            stopAllGrids();

            checkEventsConsistency();

            evts.clear();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentStartStop1() throws Exception {
        concurrentStartStop(1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentStartStop2() throws Exception {
        concurrentStartStop(5);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_ZOOKEEPER_DISCOVERY_SPI_MAX_EVTS, value = "1")
    public void testConcurrentStartStop2_EventsThrottle() throws Exception {
        concurrentStartStop(5);
    }

    /**
     * @param initNodes Number of initially started nnodes.
     * @throws Exception If failed.
     */
    private void concurrentStartStop(final int initNodes) throws Exception {
        startGrids(initNodes);

        final int NODES = 5;

        long topVer = initNodes;

        for (int i = 0; i < GridTestUtils.SF.applyLB(10, 2); i++) {
            info("Iteration: " + i);

            DiscoveryEvent[] expEvts = new DiscoveryEvent[NODES];

            startGridsMultiThreaded(initNodes, NODES);

            for (int j = 0; j < NODES; j++)
                expEvts[j] = ZookeeperDiscoverySpiTestHelper.joinEvent(++topVer);

            helper.checkEvents(ignite(0), evts, expEvts);

            checkEventsConsistency();

            final CyclicBarrier b = new CyclicBarrier(NODES);

            GridTestUtils.runMultiThreaded(new IgniteInClosure<Integer>() {
                @Override public void apply(Integer idx) {
                    try {
                        b.await();

                        stopGrid(initNodes + idx);
                    }
                    catch (Exception e) {
                        e.printStackTrace();

                        fail();
                    }
                }
            }, NODES, "stop-node");

            for (int j = 0; j < NODES; j++)
                expEvts[j] = ZookeeperDiscoverySpiTestHelper.leftEvent(++topVer, false);

            helper.checkEvents(ignite(0), evts, expEvts);

            checkEventsConsistency();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClusterRestart() throws Exception {
        startGridsMultiThreaded(3, false);

        stopAllGrids();

        evts.clear();

        startGridsMultiThreaded(3, false);

        checkZkNodesCleanup();

        waitForTopology(3);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConnectionRestore4() throws Exception {
        testSockNio = true;

        Ignite node0 = startGrid(0);

        ZkTestClientCnxnSocketNIO c0 = ZkTestClientCnxnSocketNIO.forNode(node0);

        c0.closeSocket(false);

        startGrid(1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStartStop_1_Node() throws Exception {
        startGrid(0);

        waitForTopology(1);

        stopGrid(0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRestarts_2_Nodes() throws Exception {
        startGrid(0);

        for (int i = 0; i < 10; i++) {
            info("Iteration: " + i);

            startGrid(1);

            waitForTopology(2);

            stopGrid(1);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStartStop_2_Nodes_WithCache() throws Exception {
        startGrids(2);

        for (Ignite node : G.allGrids()) {
            IgniteCache<Object, Object> cache = node.cache(DEFAULT_CACHE_NAME);

            assertNotNull(cache);

            for (int i = 0; i < 100; i++) {
                cache.put(i, node.name());

                assertEquals(node.name(), cache.get(i));
            }
        }

        awaitPartitionMapExchange();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStartStop_2_Nodes() throws Exception {
        ZookeeperDiscoverySpiTestHelper.ackEveryEventSystemProperty();

        startGrid(0);

        waitForTopology(1);

        startGrid(1);

        waitForTopology(2);

        for (Ignite node : G.allGrids())
            node.compute().broadcast(new ZookeeperDiscoverySpiTestHelper.DummyCallable(null));

        awaitPartitionMapExchange();

        helper.waitForEventsAcks(ignite(0));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleClusters() throws Exception {
        Ignite c0 = startGrid(0);

        zkRootPath = "/cluster2";

        Ignite c1 = startGridsMultiThreaded(1, 5);

        zkRootPath = "/cluster3";

        Ignite c2 = startGridsMultiThreaded(6, 3);

        checkNodesNumber(c0, 1);
        checkNodesNumber(c1, 5);
        checkNodesNumber(c2, 3);

        stopGrid(2);

        checkNodesNumber(c0, 1);
        checkNodesNumber(c1, 4);
        checkNodesNumber(c2, 3);

        for (int i = 0; i < 3; i++)
            stopGrid(i + 6);

        checkNodesNumber(c0, 1);
        checkNodesNumber(c1, 4);

        c2 = startGridsMultiThreaded(6, 2);

        checkNodesNumber(c0, 1);
        checkNodesNumber(c1, 4);
        checkNodesNumber(c2, 2);

        evts.clear();
    }

    /**
     * @param node Node.
     * @param expNodes Expected node in cluster.
     * @throws Exception If failed.
     */
    private void checkNodesNumber(final Ignite node, final int expNodes) throws Exception {
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return node.cluster().nodes().size() == expNodes;
            }
        }, 5000);

        assertEquals(expNodes, node.cluster().nodes().size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStartStop1() throws Exception {
        ZookeeperDiscoverySpiTestHelper.ackEveryEventSystemProperty();

        startGridsMultiThreaded(5, false);

        waitForTopology(5);

        awaitPartitionMapExchange();

        helper.waitForEventsAcks(ignite(0));

        stopGrid(0);

        waitForTopology(4);

        for (Ignite node : G.allGrids())
            node.compute().broadcast(new ZookeeperDiscoverySpiTestHelper.DummyCallable(null));

        startGrid(0);

        waitForTopology(5);

        awaitPartitionMapExchange();

        helper.waitForEventsAcks(grid(CU.oldest(ignite(1).cluster().nodes())));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStartStop3() throws Exception {
        startGrids(4);

        awaitPartitionMapExchange();

        stopGrid(0);

        startGrid(5);

        awaitPartitionMapExchange();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStartStop4() throws Exception {
        startGrids(6);

        awaitPartitionMapExchange();

        stopGrid(2);

        if (ThreadLocalRandom.current().nextBoolean())
            awaitPartitionMapExchange();

        stopGrid(1);

        if (ThreadLocalRandom.current().nextBoolean())
            awaitPartitionMapExchange();

        stopGrid(0);

        if (ThreadLocalRandom.current().nextBoolean())
            awaitPartitionMapExchange();

        startGrid(7);

        awaitPartitionMapExchange();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStartStop2() throws Exception {
        startGridsMultiThreaded(10, false);

        GridTestUtils.runMultiThreaded((IgniteInClosure<Integer>)this::stopGrid, 3, "stop-node-thread");

        waitForTopology(7);

        startGridsMultiThreaded(0, 3);

        waitForTopology(10);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStartStopWithClients() throws Exception {
        final int SRVS = 3;

        startGrids(SRVS);

        final int THREADS = 30;

        for (int i = 0; i < GridTestUtils.SF.applyLB(5, 2); i++) {
            info("Iteration: " + i);

            startClientGridsMultiThreaded(SRVS, THREADS);

            waitForTopology(SRVS + THREADS);

            GridTestUtils.runMultiThreaded(new IgniteInClosure<Integer>() {
                @Override public void apply(Integer idx) {
                    stopGrid(idx + SRVS);
                }
            }, THREADS, "stop-node");

            waitForTopology(SRVS);

            checkEventsConsistency();
        }
    }
}
