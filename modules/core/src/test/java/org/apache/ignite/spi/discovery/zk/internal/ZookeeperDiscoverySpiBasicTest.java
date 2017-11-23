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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.curator.test.TestingCluster;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.managers.discovery.DiscoveryLocalJoinData;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.zookeeper.ZkTestClientCnxnSocketNIO;
import org.apache.zookeeper.ZooKeeper;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.spi.discovery.zk.internal.ZookeeperDiscoveryImpl.IGNITE_ZOOKEEPER_DISCOVERY_SPI_ACK_THRESHOLD;
import static org.apache.zookeeper.ZooKeeper.ZOOKEEPER_CLIENT_CNXN_SOCKET;

/**
 *
 */
public class ZookeeperDiscoverySpiBasicTest extends GridCommonAbstractTest {
    /** */
    private static final int ZK_SRVS = 3;

    /** */
    private static TestingCluster zkCluster;

    /** */
    private static final boolean USE_TEST_CLUSTER = true;

    /** */
    private boolean client;

    /** */
    private static ConcurrentHashMap<UUID, Map<Long, DiscoveryEvent>> evts = new ConcurrentHashMap<>();

    /** */
    private static volatile boolean err;

    /** */
    private boolean testSockNio;

    /** */
    private int sesTimeout;

    /** */
    private ConcurrentHashMap<String, ZookeeperDiscoverySpi> spis = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        if (testSockNio)
            System.setProperty(ZOOKEEPER_CLIENT_CNXN_SOCKET, ZkTestClientCnxnSocketNIO.class.getName());

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        ZookeeperDiscoverySpi zkSpi = new ZookeeperDiscoverySpi();

        zkSpi.setSessionTimeout(sesTimeout > 0 ? sesTimeout : 10_000);

        spis.put(igniteInstanceName, zkSpi);

        if (USE_TEST_CLUSTER) {
            assert zkCluster != null;

            zkSpi.setZkConnectionString(zkCluster.getConnectString());
        }
        else
            zkSpi.setZkConnectionString("localhost:2181");

        cfg.setDiscoverySpi(zkSpi);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

        // cfg.setMarshaller(new JdkMarshaller());

        cfg.setClientMode(client);

        Map<IgnitePredicate<? extends Event>, int[]> lsnrs = new HashMap<>();

        lsnrs.put(new IgnitePredicate<Event>() {
            /** */
            @IgniteInstanceResource
            private Ignite ignite;

            @Override public boolean apply(Event evt) {
                try {
                    DiscoveryEvent discoveryEvt = (DiscoveryEvent)evt;

                    UUID locId = ignite.cluster().localNode().id();

                    Map<Long, DiscoveryEvent> nodeEvts = evts.get(locId);

                    if (nodeEvts == null) {
                        Object old = evts.put(locId, nodeEvts = new TreeMap<>());

                        assertNull(old);

                        synchronized (nodeEvts) {
                            DiscoveryLocalJoinData locJoin = ((IgniteKernal)ignite).context().discovery().localJoin();

                            nodeEvts.put(locJoin.event().topologyVersion(), locJoin.event());
                        }
                    }

                    synchronized (nodeEvts) {
                        DiscoveryEvent old = nodeEvts.put(discoveryEvt.topologyVersion(), discoveryEvt);

                        assertNull(old);
                    }
                }
                catch (Throwable e) {
                    err = true;

                    info("Unexpected error: " + e);
                }

                return true;
            }
        }, new int[]{EVT_NODE_JOINED, EVT_NODE_FAILED, EVT_NODE_LEFT});

        cfg.setLocalEventListeners(lsnrs);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        IgnitionEx.TEST_ZK = false;

        if (USE_TEST_CLUSTER) {
            zkCluster = new TestingCluster(ZK_SRVS);

            zkCluster.start();
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        if (zkCluster != null) {
            try {
                zkCluster.close();
            }
            catch (Exception e) {
                U.error(log, "Failed to stop Zookeeper client: " + e, e);
            }

            zkCluster = null;
        }

        super.afterTestsStopped();
    }

    /**
     *
     */
    private static void ackEveryEventSystemProperty() {
        System.setProperty(IGNITE_ZOOKEEPER_DISCOVERY_SPI_ACK_THRESHOLD, "1");
    }

    /**
     *
     */
    private void clearAckEveryEventSystemProperty() {
        System.setProperty(IGNITE_ZOOKEEPER_DISCOVERY_SPI_ACK_THRESHOLD, "1");
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        reset();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        try {
            assertFalse("Unexpected error, see log for details", err);

            checkEventsConsistency();
        }
        finally {
            reset();

            stopAllGrids();
        }

        clearAckEveryEventSystemProperty();
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientNodesStatus() throws Exception {
        startGrid(0);

        for (Ignite node : G.allGrids()) {
            assertEquals(0, node.cluster().forClients().nodes().size());
            assertEquals(1, node.cluster().forServers().nodes().size());
        }

        client = true;

        startGrid(1);

        for (Ignite node : G.allGrids()) {
            assertEquals(1, node.cluster().forClients().nodes().size());
            assertEquals(1, node.cluster().forServers().nodes().size());
        }

        client = false;

        startGrid(2);

        client = true;

        startGrid(3);

        for (Ignite node : G.allGrids()) {
            assertEquals(2, node.cluster().forClients().nodes().size());
            assertEquals(2, node.cluster().forServers().nodes().size());
        }

        stopGrid(1);

        waitForTopology(3);

        for (Ignite node : G.allGrids()) {
            assertEquals(1, node.cluster().forClients().nodes().size());
            assertEquals(2, node.cluster().forServers().nodes().size());
        }

        stopGrid(2);

        waitForTopology(2);

        for (Ignite node : G.allGrids()) {
            assertEquals(1, node.cluster().forClients().nodes().size());
            assertEquals(1, node.cluster().forServers().nodes().size());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testStopNode_1() throws Exception {
        startGrids(5);

        waitForTopology(5);

        stopGrid(3);

        waitForTopology(4);

        startGrid(3);

        waitForTopology(5);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCustomEventsSimple1_SingleNode() throws Exception {
        ackEveryEventSystemProperty();

        Ignite srv0 = startGrid(0);

        srv0.createCache(new CacheConfiguration<>("c1"));

        waitForEventsAcks(srv0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCustomEventsSimple1_5_Nodes() throws Exception {
        ackEveryEventSystemProperty();

        Ignite srv0 = startGrids(5);

        srv0.createCache(new CacheConfiguration<>("c1"));

        awaitPartitionMapExchange();

        waitForEventsAcks(srv0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSegmentation1() throws Exception {
        sesTimeout = 1000;
        testSockNio = true;

        Ignite node0 = startGrid(0);

        final CountDownLatch l = new CountDownLatch(1);

        node0.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event event) {
                l.countDown();

                return false;
            }
        }, EventType.EVT_NODE_SEGMENTED);

        ZkTestClientCnxnSocketNIO c0 = ZkTestClientCnxnSocketNIO.forNode(node0);

        c0.closeSocket(true);

        for (int i = 0; i < 10; i++) {
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
    public void testConnectionRestore_NonCoordinator1() throws Exception {
        connectionRestore_NonCoordinator(false);
    }

    /**
     * @throws Exception If failed.
     */
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
            @Override public Void call() throws Exception {
                try {
                    startGrid(2);
                }
                catch (Exception e) {
                    info("Start error: " + e);
                }

                return null;
            }
        }, "start-node");

        checkEvents(node0, joinEvent(3));

        if (failWhenDisconnected) {
            ZookeeperDiscoverySpi spi = spis.get(getTestIgniteInstanceName(2));

            closeZkClient(spi);

            checkEvents(node0, failEvent(4));
        }

        c1.allowConnect();

        checkEvents(ignite(1), joinEvent(3));

        if (failWhenDisconnected) {
            checkEvents(ignite(1), failEvent(4));

            IgnitionEx.stop(getTestIgniteInstanceName(2), true, true);
        }

        fut.get();

        waitForTopology(failWhenDisconnected ? 2 : 3);
    }

    /**
     * @throws Exception If failed.
     */
    public void testConnectionRestore_Coordinator1() throws Exception {
        connectionRestore_Coordinator(1, 1, 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testConnectionRestore_Coordinator1_1() throws Exception {
        connectionRestore_Coordinator(1, 1, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testConnectionRestore_Coordinator2() throws Exception {
        connectionRestore_Coordinator(1, 3, 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testConnectionRestore_Coordinator3() throws Exception {
        connectionRestore_Coordinator(3, 3, 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testConnectionRestore_Coordinator4() throws Exception {
        connectionRestore_Coordinator(3, 3, 1);
    }

    /**
     * @param initNodes Number of initially started nodes.
     * @param startNodes Number of nodes to start after coordinator loose connection.
     * @param failCnt Number of nodes to stop after coordinator loose connection.
     * @throws Exception If failed.
     */
    private void connectionRestore_Coordinator(int initNodes, int startNodes, int failCnt) throws Exception {
        sesTimeout = 30_000;
        testSockNio = true;

        Ignite node0 = startGrids(initNodes);

        ZkTestClientCnxnSocketNIO c0 = ZkTestClientCnxnSocketNIO.forNode(node0);

        c0.closeSocket(true);

        final AtomicInteger nodeIdx = new AtomicInteger(initNodes);

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
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
            ZookeeperDiscoverySpi spi = waitSpi(getTestIgniteInstanceName(i));

            ZookeeperDiscoveryImpl impl = GridTestUtils.getFieldValue(spi, "impl");

            impl.waitConnectStart();

            if (cnt++ < failCnt) {
                ZkTestClientCnxnSocketNIO c = ZkTestClientCnxnSocketNIO.forNode(getTestIgniteInstanceName(i));

                c.closeSocket(true);

                blockedC.add(c);

                failedZkNodes.add((String)GridTestUtils.getFieldValue(impl, "locNodeZkPath"));
            }
            else {
                expEvts[expEvtCnt] = joinEvent(initNodes + expEvtCnt + 1);

                expEvtCnt++;
            }
        }

        final ZookeeperClient zkClient = new ZookeeperClient(log, zkCluster.getConnectString(), 10_000, null);

        try {
            assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    try {
                        List<String> c = zkClient.getChildren("/apacheIgnite/default/alive");

                        for (String failedZkNode : failedZkNodes) {
                            if (c.contains(failedZkNode))
                                return false;
                        }

                        return true;
                    }
                    catch (Exception e) {
                        fail();

                        return true;
                    }
                }
            }, 10_000));
        }
        finally {
            zkClient.close();
        }

        c0.allowConnect();

        for (ZkTestClientCnxnSocketNIO c : blockedC)
            c.allowConnect();

        if (expEvts.length > 0) {
            for (int i = 0; i < initNodes; i++)
                checkEvents(ignite(i), expEvts);
        }

        fut.get();

        waitForTopology(initNodes + startNodes - failCnt);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClusterRestart() throws Exception {
        startGridsMultiThreaded(3, false);

        stopAllGrids();

        evts.clear();

        startGridsMultiThreaded(3, false);

        waitForTopology(3);
    }

    /**
     * @throws Exception If failed.
     */
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
    public void testStartStop_1_Node() throws Exception {
        startGrid(0);

        waitForTopology(1);

        stopGrid(0);
    }

    /**
     * @throws Exception If failed.
     */
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
    public void testStartStop_2_Nodes() throws Exception {
        ackEveryEventSystemProperty();

        startGrid(0);

        waitForTopology(1);

        startGrid(1);

        waitForTopology(2);

        for (Ignite node : G.allGrids())
            node.compute().broadcast(new DummyCallable(null));

        awaitPartitionMapExchange();

        waitForEventsAcks(ignite(0));
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartStop1() throws Exception {
        ackEveryEventSystemProperty();

        startGridsMultiThreaded(5, false);

        waitForTopology(5);

        awaitPartitionMapExchange();

        waitForEventsAcks(ignite(0));

        stopGrid(0);

        waitForTopology(4);

        for (Ignite node : G.allGrids())
            node.compute().broadcast(new DummyCallable(null));

        startGrid(0);

        waitForTopology(5);

        awaitPartitionMapExchange();

        waitForEventsAcks(grid(CU.oldest(ignite(1).cluster().nodes())));
    }

    /**
     * @throws Exception If failed.
     */
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
    public void testStartStop2() throws Exception {
        startGridsMultiThreaded(10, false);

        GridTestUtils.runMultiThreaded(new IgniteInClosure<Integer>() {
            @Override public void apply(Integer idx) {
                stopGrid(idx);
            }
        }, 3, "stop-node-thread");

        waitForTopology(7);

        startGridsMultiThreaded(0, 3);

        waitForTopology(10);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartStopWithClients() throws Exception {
        final int SRVS = 3;

        startGrids(SRVS);

        client = true;

        final int THREADS = 30;

        for (int i = 0; i < 5; i++) {
            info("Iteration: " + i);

            startGridsMultiThreaded(SRVS, THREADS);

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

    /**
     * @throws Exception If failed.
     */
    public void testTopologyChangeAndZkRestart() throws Exception {

    }

    /**
     * @param restartZk
     * @throws Exception If failed.
     */
    private void topologyChangeWithRestarts(boolean restartZk) throws Exception {
        startGrid(0);

        long stopTime = System.currentTimeMillis();
    }

    /**
     * @throws Exception If failed.
     */
    public void testRandomTopologyChanges() throws Exception {
        randomTopologyChanges(false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRandomTopologyChangesRestartZk() throws Exception {
        randomTopologyChanges(true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRandomTopologyChangesCloseClients() throws Exception {
        randomTopologyChanges(false, true);
    }

    /**
     * @param restartZk If {@code true} in background restarts on of ZK servers.
     * @param closeClientSock If {@code true} in background closes zk clients' sockets.
     * @throws Exception If failed.
     */
    private void randomTopologyChanges(boolean restartZk, boolean closeClientSock) throws Exception {
        if (closeClientSock)
            testSockNio = true;

        List<Integer> startedNodes = new ArrayList<>();
        List<String> startedCaches = new ArrayList<>();

        int nextNodeIdx = 0;
        int nextCacheIdx = 0;

        long stopTime = System.currentTimeMillis() + 60_000;

        int MAX_NODES = 20;
        int MAX_CACHES = 10;

        AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<?> fut1 = restartZk ? startRestartZkServers(stopTime, stop) : null;
        IgniteInternalFuture<?> fut2 = closeClientSock ? startCloseZkClientSocket(stopTime, stop) : null;

        try {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            while (System.currentTimeMillis() < stopTime) {
                if (startedNodes.size() > 0 && rnd.nextInt(10) == 0) {
                    boolean startCache = startedCaches.size() < 2 ||
                        (startedCaches.size() < MAX_CACHES && rnd.nextInt(5) != 0);

                    int nodeIdx = startedNodes.get(rnd.nextInt(startedNodes.size()));

                    if (startCache) {
                        String cacheName = "cache-" + nextCacheIdx++;

                        log.info("Next, start new cache [cacheName=" + cacheName +
                            ", node=" + nodeIdx +
                            ", crd=" + (startedNodes.isEmpty() ? null : Collections.min(startedNodes)) +
                            ", curCaches=" + startedCaches.size() + ']');

                        ignite(nodeIdx).createCache(new CacheConfiguration<>(cacheName));

                        startedCaches.add(cacheName);
                    }
                    else {
                        if (startedCaches.size() > 1) {
                            String cacheName = startedCaches.get(rnd.nextInt(startedCaches.size()));

                            log.info("Next, stop cache [nodeIdx=" + nodeIdx +
                                ", node=" + nodeIdx +
                                ", crd=" + (startedNodes.isEmpty() ? null : Collections.min(startedNodes)) +
                                ", cacheName=" + startedCaches.size() + ']');

                            ignite(nodeIdx).destroyCache(cacheName);

                            assertTrue(startedCaches.remove(cacheName));
                        }
                    }
                }
                else {
                    boolean startNode = startedNodes.size() < 2 ||
                        (startedNodes.size() < MAX_NODES && rnd.nextInt(5) != 0);

                    if (startNode) {
                        int nodeIdx = nextNodeIdx++;

                        log.info("Next, start new node [nodeIdx=" + nodeIdx +
                            ", crd=" + (startedNodes.isEmpty() ? null : Collections.min(startedNodes)) +
                            ", curNodes=" + startedNodes.size() + ']');

                        startGrid(nodeIdx);

                        assertTrue(startedNodes.add(nodeIdx));
                    }
                    else {
                        if (startedNodes.size() > 1) {
                            int nodeIdx = startedNodes.get(rnd.nextInt(startedNodes.size()));

                            log.info("Next, stop [nodeIdx=" + nodeIdx +
                                ", crd=" + (startedNodes.isEmpty() ? null : Collections.min(startedNodes)) +
                                ", curNodes=" + startedNodes.size() + ']');

                            stopGrid(nodeIdx);

                            assertTrue(startedNodes.remove((Integer)nodeIdx));
                        }
                    }
                }

                U.sleep(rnd.nextInt(100) + 1);
            }
        }
        finally {
            stop.set(true);
        }

        if (fut1 != null)
            fut1.get();

        if (fut2 != null)
            fut2.get();
    }

    /**
     *
     */
    private void reset() {
        System.clearProperty(ZOOKEEPER_CLIENT_CNXN_SOCKET);

        ZkTestClientCnxnSocketNIO.reset();

        System.clearProperty(ZOOKEEPER_CLIENT_CNXN_SOCKET);

        err = false;

        evts.clear();
    }

    /**
     * @param stopTime Stop time.
     * @param stop Stop flag.
     * @return Future.
     */
    private IgniteInternalFuture<?> startRestartZkServers(final long stopTime, final AtomicBoolean stop) {
        return GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                while (!stop.get() && System.currentTimeMillis() < stopTime) {
                    U.sleep(rnd.nextLong(500) + 500);

                    int idx = rnd.nextInt(ZK_SRVS);

                    log.info("Restart ZK server: " + idx);

                    zkCluster.getServers().get(idx).restart();

                }

                return null;
            }
        }, "zk-restart-thread");
    }

    /**
     * @param stopTime Stop time.
     * @param stop Stop flag.
     * @return Future.
     */
    private IgniteInternalFuture<?> startCloseZkClientSocket(final long stopTime, final AtomicBoolean stop) {
        assert testSockNio;

        return GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                while (!stop.get() && System.currentTimeMillis() < stopTime) {
                    U.sleep(rnd.nextLong(100) + 50);

                    List<Ignite> nodes = G.allGrids();

                    if (nodes.size() > 0) {
                        Ignite node = nodes.get(rnd.nextInt(nodes.size()));

                        ZkTestClientCnxnSocketNIO nio = ZkTestClientCnxnSocketNIO.forNode(node);

                        if (nio != null) {
                            info("Close zk client socket for node: " + node.name());

                            try {
                                nio.closeSocket(false);
                            }
                            catch (Exception e) {
                                info("Failed to close zk client socket for node: " + node.name());
                            }
                        }
                    }
                }

                return null;
            }
        }, "zk-restart-thread");
    }

    /**
     * @param node Node.
     * @throws Exception If failed.
     */
    private void waitForEventsAcks(final Ignite node) throws Exception {
        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                Map<Object, Object> evts = GridTestUtils.getFieldValue(node.configuration().getDiscoverySpi(),
                    "impl", "evtsData", "evts");

                if (!evts.isEmpty()) {
                    info("Unacked events: " + evts);

                    return false;
                }

                return true;
            }
        }, 10_000));
    }

    /**
     * @throws Exception If failed.
     */
    private void checkEventsConsistency() throws Exception {
        for (Map.Entry<UUID, Map<Long, DiscoveryEvent>> nodeEvtEntry : evts.entrySet()) {
            UUID nodeId = nodeEvtEntry.getKey();
            Map<Long, DiscoveryEvent> nodeEvts = nodeEvtEntry.getValue();

            for (Map.Entry<UUID, Map<Long, DiscoveryEvent>> nodeEvtEntry0 : evts.entrySet()) {
                if (!nodeId.equals(nodeEvtEntry0.getKey())) {
                    Map<Long, DiscoveryEvent> nodeEvts0 = nodeEvtEntry0.getValue();

                    synchronized (nodeEvts) {
                        synchronized (nodeEvts0) {
                            checkEventsConsistency(nodeEvts, nodeEvts0);
                        }
                    }
                }
            }
        }
    }

    /**
     * @param evts1 Received events.
     * @param evts2 Received events.
     */
    private void checkEventsConsistency(Map<Long, DiscoveryEvent> evts1, Map<Long, DiscoveryEvent> evts2) {
        for (Map.Entry<Long, DiscoveryEvent> e1 : evts1.entrySet()) {
            DiscoveryEvent evt1 = e1.getValue();
            DiscoveryEvent evt2 = evts2.get(e1.getKey());

            if (evt2 != null) {
                assertEquals(evt1.topologyVersion(), evt2.topologyVersion());
                assertEquals(evt1.eventNode(), evt2.eventNode());
                assertEquals(evt1.topologyNodes(), evt2.topologyNodes());
            }
        }
    }

    /**
     * @param nodeName Node name.
     * @return Node's discovery SPI.
     * @throws Exception If failed.
     */
    private ZookeeperDiscoverySpi waitSpi(final String nodeName) throws Exception {
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return spis.contains(nodeName);
            }
        }, 5000);

        ZookeeperDiscoverySpi spi = spis.get(nodeName);

        assertNotNull("Failed to get SPI for node: " + nodeName, spi);

        return spi;
    }

    private static DiscoveryEvent joinEvent(long topVer) {
        DiscoveryEvent expEvt = new DiscoveryEvent(null, null, EventType.EVT_NODE_JOINED, null);

        expEvt.topologySnapshot(topVer, null);

        return expEvt;
    }

    private static DiscoveryEvent failEvent(long topVer) {
        DiscoveryEvent expEvt = new DiscoveryEvent(null, null, EventType.EVT_NODE_FAILED, null);

        expEvt.topologySnapshot(topVer, null);

        return expEvt;
    }

    /**
     * @param node Node.
     * @param expEvts Expected events.
     * @throws Exception If fialed.
     */
    private void checkEvents(final Ignite node, final DiscoveryEvent...expEvts) throws Exception {
        checkEvents(node.cluster().localNode().id(), expEvts);
    }

    /**
     * @param nodeId Node ID.
     * @param expEvts Expected events.
     * @throws Exception If failed.
     */
    private void checkEvents(final UUID nodeId, final DiscoveryEvent...expEvts) throws Exception {
        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                Map<Long, DiscoveryEvent> nodeEvts = evts.get(nodeId);

                if (nodeEvts == null) {
                    info("No events for node: " + nodeId);

                    return false;
                }

                synchronized (nodeEvts) {
                    for (DiscoveryEvent expEvt : expEvts) {
                        DiscoveryEvent evt0 = nodeEvts.get(expEvt.topologyVersion());

                        if (evt0 == null) {
                            info("No event for version: " + expEvt.topologyVersion());

                            return false;
                        }

                        assertEquals(expEvt.type(), evt0.type());
                    }
                }

                return true;
            }
        }, 10000));
    }

    /**
     * @param spi Spi instance.
     */
    private void closeZkClient(ZookeeperDiscoverySpi spi) {
        ZooKeeper zk = GridTestUtils.getFieldValue(spi, "impl", "zkClient", "zk");

        try {
            zk.close();
        }
        catch (Exception e) {
            fail("Unexpected error: " + e);
        }
    }

    /**
     * @param expSize Expected nodes number.
     * @throws Exception If failed.
     */
    private void waitForTopology(final int expSize) throws Exception {
        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                List<Ignite> nodes = G.allGrids();

                if (nodes.size() != expSize) {
                    info("Wait all nodes [size=" + nodes.size() + ", exp=" + expSize + ']');

                    return false;
                }

                for (Ignite node: nodes) {
                    int sizeOnNode = node.cluster().nodes().size();

                    if (sizeOnNode != expSize) {
                        info("Wait for size on node [node=" + node.name() + ", size=" + sizeOnNode + ", exp=" + expSize + ']');

                        return false;
                    }
                }

                return true;
            }
        }, 5000));
    }

    /**
     *
     */
    private static class DummyCallable implements IgniteCallable<Object> {
        /** */
        private byte[] data;

        /**
         * @param data Data.
         */
        DummyCallable(byte[] data) {
            this.data = data;
        }

        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            return data;
        }
    }
}
