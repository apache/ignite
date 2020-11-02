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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.curator.test.TestingZooKeeperServer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteState;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpi;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpiTestUtil;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.transactions.Transaction;
import org.apache.zookeeper.ZkTestClientCnxnSocketNIO;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_WAL_LOG_TX_RECORDS;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/**
 * Tests for Zookeeper SPI discovery.
 */
public class ZookeeperDiscoverySegmentationAndConnectionRestoreTest extends ZookeeperDiscoverySpiTestBase {
    /**
     * Verifies correct handling of SEGMENTATION event with STOP segmentation policy: node is stopped successfully,
     * all its threads are shut down.
     *
     * @throws Exception If failed.
     *
     * @see <a href="https://issues.apache.org/jira/browse/IGNITE-9040">IGNITE-9040</a> ticket for more context of the test.
     */
    @Test
    @WithSystemProperty(key = IGNITE_WAL_LOG_TX_RECORDS, value = "true")
    public void testStopNodeOnSegmentaion() throws Exception {
        sesTimeout = 2000;
        testSockNio = true;
        persistence = true;
        atomicityMode = CacheAtomicityMode.TRANSACTIONAL;
        backups = 2;

        final Ignite node0 = startGrid(0);

        sesTimeout = 10_000;
        testSockNio = false;

        startGrid(1);

        node0.cluster().active(true);

        final IgniteEx client = startClientGrid(2);

        //first transaction
        client.transactions().txStart(PESSIMISTIC, READ_COMMITTED, 0, 0);
        client.cache(DEFAULT_CACHE_NAME).put(0, 0);

        //second transaction to create a deadlock with the first one
        // and guarantee transaction futures will be presented on segmented node
        // (erroneous write to WAL on segmented node stop happens
        // on completing transaction with NodeStoppingException)
        GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                Transaction tx2 = client.transactions().txStart(OPTIMISTIC, READ_COMMITTED, 0, 0);
                client.cache(DEFAULT_CACHE_NAME).put(0, 0);
                tx2.commit();
            }
        });

        //next block simulates Ignite node segmentation by closing socket of ZooKeeper client
        {
            final CountDownLatch l = new CountDownLatch(1);

            node0.events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    l.countDown();

                    return false;
                }
            }, EventType.EVT_NODE_SEGMENTED);

            ZkTestClientCnxnSocketNIO c0 = ZkTestClientCnxnSocketNIO.forNode(node0);

            c0.closeSocket(true);

            for (int i = 0; i < 10; i++) {
                //noinspection BusyWait
                Thread.sleep(1_000);

                if (l.getCount() == 0)
                    break;
            }

            info("Allow connect");

            c0.allowConnect();

            assertTrue(l.await(10, TimeUnit.SECONDS));
        }

        waitForNodeStop(node0.name());

        checkStoppedNodeThreads(node0.name());
    }

    /** */
    private void checkStoppedNodeThreads(String nodeName) {
        Set<Thread> threads = Thread.getAllStackTraces().keySet();

        for (Thread t : threads) {
            if (t.getName().contains(nodeName))
                throw new AssertionError("Thread from stopped node has been found: " + t.getName());
        }
    }

    /** */
    private void waitForNodeStop(String name) throws Exception {
        while (true) {
            if (IgnitionEx.state(name) == IgniteState.STARTED)
                //noinspection BusyWait
                Thread.sleep(2000);
            else
                break;
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSegmentation1() throws Exception {
        sesTimeout = 2000;
        testSockNio = true;

        Ignite node0 = startGrid(0);

        final CountDownLatch l = new CountDownLatch(1);

        node0.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                l.countDown();

                return false;
            }
        }, EventType.EVT_NODE_SEGMENTED);

        ZkTestClientCnxnSocketNIO c0 = ZkTestClientCnxnSocketNIO.forNode(node0);

        c0.closeSocket(true);

        for (int i = 0; i < 10; i++) {
            //noinspection BusyWait
            Thread.sleep(1_000);

            if (l.getCount() == 0)
                break;
        }

        info("Allow connect");

        c0.allowConnect();

        assertTrue(l.await(10, TimeUnit.SECONDS));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSegmentation2() throws Exception {
        sesTimeout = 2000;

        Ignite node0 = startGrid(0);

        final CountDownLatch l = new CountDownLatch(1);

        node0.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                l.countDown();

                return false;
            }
        }, EventType.EVT_NODE_SEGMENTED);

        try {
            zkCluster.close();

            assertTrue(l.await(10, TimeUnit.SECONDS));
        }
        finally {
            zkCluster = ZookeeperDiscoverySpiTestUtil.createTestingCluster(ZK_SRVS);

            zkCluster.start();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSegmentation3() throws Exception {
        sesTimeout = 5000;

        Ignite node0 = startGrid(0);

        final CountDownLatch l = new CountDownLatch(1);

        node0.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                l.countDown();

                return false;
            }
        }, EventType.EVT_NODE_SEGMENTED);

        List<TestingZooKeeperServer> srvs = zkCluster.getServers();

        assertEquals(3, srvs.size());

        try {
            srvs.get(0).stop();
            srvs.get(1).stop();

            QuorumPeer qp = srvs.get(2).getQuorumPeer();

            // Zookeeper's socket timeout [tickTime * initLimit] + 5 additional seconds for other logic
            assertTrue(l.await(qp.getTickTime() * qp.getInitLimit() + 5000, TimeUnit.MILLISECONDS));
        }
        finally {
            zkCluster.close();

            zkCluster = ZookeeperDiscoverySpiTestUtil.createTestingCluster(ZK_SRVS);

            zkCluster.start();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-8178")
    @Test
    public void testQuorumRestore() throws Exception {
        sesTimeout = 60_000;

        startGrids(3);

        waitForTopology(3);

        List<TestingZooKeeperServer> srvs = zkCluster.getServers();

        assertEquals(3, srvs.size());

        try {
            srvs.get(0).stop();
            srvs.get(1).stop();

            U.sleep(2000);

            srvs.get(1).restart();

            U.sleep(4000);

            startGrid(4);

            waitForTopology(4);
        }
        finally {
            zkCluster.close();

            zkCluster = ZookeeperDiscoverySpiTestUtil.createTestingCluster(ZK_SRVS);

            zkCluster.start();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConnectionRestore1() throws Exception {
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
    public void testConnectionRestore2() throws Exception {
        testSockNio = true;

        Ignite node0 = startGrid(0);

        ZkTestClientCnxnSocketNIO c0 = ZkTestClientCnxnSocketNIO.forNode(node0);

        c0.closeSocket(false);

        startGridsMultiThreaded(1, 5);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConnectionRestore_NonCoordinator1() throws Exception {
        connectionRestore_NonCoordinator(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConnectionRestore_NonCoordinator2() throws Exception {
        connectionRestore_NonCoordinator(true);
    }

    /**
     * @param failWhenDisconnected {@code True} if fail node while another node is disconnected.
     * @throws Exception If failed.
     */
    private void connectionRestore_NonCoordinator(boolean failWhenDisconnected) throws Exception {
        testSockNio = true;

        Ignite node0 = startGrid(0);
        Ignite node1 = startGrid(1);

        ZkTestClientCnxnSocketNIO c1 = ZkTestClientCnxnSocketNIO.forNode(node1);

        c1.closeSocket(true);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() {
                try {
                    startGrid(2);
                }
                catch (Exception e) {
                    info("Start error: " + e);
                }

                return null;
            }
        }, "start-node");

        helper.checkEvents(node0, evts, ZookeeperDiscoverySpiTestHelper.joinEvent(3));

        if (failWhenDisconnected) {
            ZookeeperDiscoverySpi spi = spis.get(getTestIgniteInstanceName(2));

            closeZkClient(spi);

            helper.checkEvents(node0, evts, ZookeeperDiscoverySpiTestHelper.leftEvent(4, true));
        }

        c1.allowConnect();

        helper.checkEvents(ignite(1), evts, ZookeeperDiscoverySpiTestHelper.joinEvent(3));

        if (failWhenDisconnected) {
            helper.checkEvents(ignite(1), evts, ZookeeperDiscoverySpiTestHelper.leftEvent(4, true));

            IgnitionEx.stop(getTestIgniteInstanceName(2), true, true);
        }

        fut.get();

        waitForTopology(failWhenDisconnected ? 2 : 3);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConnectionRestore_Coordinator1() throws Exception {
        connectionRestore_Coordinator(1, 1, 0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConnectionRestore_Coordinator1_1() throws Exception {
        connectionRestore_Coordinator(1, 1, 1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConnectionRestore_Coordinator2() throws Exception {
        connectionRestore_Coordinator(1, 3, 0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConnectionRestore_Coordinator3() throws Exception {
        connectionRestore_Coordinator(3, 3, 0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConnectionRestore_Coordinator4() throws Exception {
        connectionRestore_Coordinator(3, 3, 1);
    }

    /**
     * @param initNodes Number of initially started nodes.
     * @param startNodes Number of nodes to start after coordinator loose connection.
     * @param failCnt Number of nodes to stop after coordinator loose connection.
     * @throws Exception If failed.
     */
    private void connectionRestore_Coordinator(final int initNodes, int startNodes, int failCnt) throws Exception {
        sesTimeout = 30_000;
        testSockNio = true;

        Ignite node0 = startGrids(initNodes);

        ZkTestClientCnxnSocketNIO c0 = ZkTestClientCnxnSocketNIO.forNode(node0);

        c0.closeSocket(true);

        final AtomicInteger nodeIdx = new AtomicInteger(initNodes);

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() {
                try {
                    startGrid(nodeIdx.getAndIncrement());
                }
                catch (Exception e) {
                    error("Start failed: " + e);
                }

                return null;
            }
        }, startNodes, "start-node");

        int cnt = 0;

        DiscoveryEvent[] expEvts = new DiscoveryEvent[startNodes - failCnt];

        int expEvtCnt = 0;

        sesTimeout = 1000;

        List<ZkTestClientCnxnSocketNIO> blockedC = new ArrayList<>();

        final List<String> failedZkNodes = new ArrayList<>(failCnt);

        for (int i = initNodes; i < initNodes + startNodes; i++) {
            final ZookeeperDiscoverySpi spi = helper.waitSpi(getTestIgniteInstanceName(i), spis);

            assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    Object spiImpl = GridTestUtils.getFieldValue(spi, "impl");

                    if (spiImpl == null)
                        return false;

                    long internalOrder = GridTestUtils.getFieldValue(spiImpl, "rtState", "internalOrder");

                    return internalOrder > 0;
                }
            }, 10_000));

            if (cnt++ < failCnt) {
                ZkTestClientCnxnSocketNIO c = ZkTestClientCnxnSocketNIO.forNode(getTestIgniteInstanceName(i));

                c.closeSocket(true);

                blockedC.add(c);

                failedZkNodes.add(ZookeeperDiscoverySpiTestHelper.aliveZkNodePath(spi));
            }
            else {
                expEvts[expEvtCnt] = ZookeeperDiscoverySpiTestHelper.joinEvent(initNodes + expEvtCnt + 1);

                expEvtCnt++;
            }
        }

        ZookeeperDiscoverySpiTestHelper.waitNoAliveZkNodes(log, zkCluster.getConnectString(), failedZkNodes, 30_000);

        c0.allowConnect();

        for (ZkTestClientCnxnSocketNIO c : blockedC)
            c.allowConnect();

        if (expEvts.length > 0) {
            for (int i = 0; i < initNodes; i++)
                helper.checkEvents(ignite(i), evts, expEvts);
        }

        fut.get();

        waitForTopology(initNodes + startNodes - failCnt);
    }

    /**
     * @param spi Spi instance.
     */
    private static void closeZkClient(ZookeeperDiscoverySpi spi) {
        ZooKeeper zk = ZookeeperDiscoverySpiTestHelper.zkClient(spi);

        try {
            zk.close();
        }
        catch (Exception e) {
            fail("Unexpected error: " + e);
        }
    }
}
