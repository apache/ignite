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

package org.apache.ignite.spi.discovery.tcp;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;

import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.events.EventType.*;

/**
 * Client-based discovery tests.
 */
public class TcpClientDiscoverySelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final AtomicInteger srvIdx = new AtomicInteger();

    /** */
    private static final AtomicInteger clientIdx = new AtomicInteger();

    /** */
    private static Collection<UUID> srvNodeIds;

    /** */
    private static Collection<UUID> clientNodeIds;

    /** */
    private static int clientsPerSrv;

    /** */
    private static CountDownLatch srvJoinedLatch;

    /** */
    private static CountDownLatch srvLeftLatch;

    /** */
    private static CountDownLatch srvFailedLatch;

    /** */
    private static CountDownLatch clientJoinedLatch;

    /** */
    private static CountDownLatch clientLeftLatch;

    /** */
    private static CountDownLatch clientFailedLatch;

    /** */
    private static CountDownLatch msgLatch;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setLocalHost("127.0.0.1");

        if (gridName.startsWith("server")) {
            TcpDiscoverySpi disco = new TcpDiscoverySpi();

            disco.setIpFinder(IP_FINDER);

            cfg.setDiscoverySpi(disco);
        }
        else if (gridName.startsWith("client")) {
            TcpClientDiscoverySpi disco = new TcpClientDiscoverySpi();

            TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

            String addr = new ArrayList<>(IP_FINDER.getRegisteredAddresses()).
                get((clientIdx.get() - 1) / clientsPerSrv).toString();

            if (addr.startsWith("/"))
                addr = addr.substring(1);

            ipFinder.setAddresses(Arrays.asList(addr));

            disco.setIpFinder(ipFinder);

            cfg.setDiscoverySpi(disco);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        Collection<InetSocketAddress> addrs = IP_FINDER.getRegisteredAddresses();

        if (!F.isEmpty(addrs))
            IP_FINDER.unregisterAddresses(addrs);

        srvIdx.set(0);
        clientIdx.set(0);

        srvNodeIds = new GridConcurrentHashSet<>();
        clientNodeIds = new GridConcurrentHashSet<>();

        clientsPerSrv = 2;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllClients(true);
        stopAllServers(true);

        assert G.allGrids().isEmpty();
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientNodeJoin() throws Exception {
        startServerNodes(3);
        startClientNodes(3);

        checkNodes(3, 3);

        srvJoinedLatch = new CountDownLatch(3);
        clientJoinedLatch = new CountDownLatch(3);

        attachListeners(3, 3);

        startClientNodes(1);

        await(srvJoinedLatch);
        await(clientJoinedLatch);

        checkNodes(3, 4);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientNodeLeave() throws Exception {
        startServerNodes(3);
        startClientNodes(3);

        checkNodes(3, 3);

        srvLeftLatch = new CountDownLatch(3);
        clientLeftLatch = new CountDownLatch(2);

        attachListeners(3, 3);

        stopGrid("client-2");

        await(srvLeftLatch);
        await(clientLeftLatch);

        checkNodes(3, 2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientNodeFail() throws Exception {
        startServerNodes(3);
        startClientNodes(3);

        checkNodes(3, 3);

        srvFailedLatch = new CountDownLatch(3);
        clientFailedLatch = new CountDownLatch(2);

        attachListeners(3, 3);

        failClient(2);

        await(srvFailedLatch);
        await(clientFailedLatch);

        checkNodes(3, 2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testServerNodeJoin() throws Exception {
        startServerNodes(3);
        startClientNodes(3);

        checkNodes(3, 3);

        srvJoinedLatch = new CountDownLatch(3);
        clientJoinedLatch = new CountDownLatch(3);

        attachListeners(3, 3);

        startServerNodes(1);

        await(srvJoinedLatch);
        await(clientJoinedLatch);

        checkNodes(4, 3);
    }

    /**
     * @throws Exception If failed.
     */
    public void testServerNodeLeave() throws Exception {
        startServerNodes(3);
        startClientNodes(3);

        checkNodes(3, 3);

        srvLeftLatch = new CountDownLatch(2);
        clientLeftLatch = new CountDownLatch(3);

        attachListeners(3, 3);

        stopGrid("server-2");

        await(srvLeftLatch);
        await(clientLeftLatch);

        checkNodes(2, 3);
    }

    /**
     * @throws Exception If failed.
     */
    public void testServerNodeFail() throws Exception {
        startServerNodes(3);
        startClientNodes(3);

        checkNodes(3, 3);

        srvFailedLatch = new CountDownLatch(2);
        clientFailedLatch = new CountDownLatch(3);

        attachListeners(3, 3);

        assert U.<Map>field(G.ignite("server-2").configuration().getDiscoverySpi(), "clientMsgWorkers").isEmpty();

        failServer(2);

        await(srvFailedLatch);
        await(clientFailedLatch);

        checkNodes(2, 3);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientReconnect() throws Exception {
        clientsPerSrv = 1;

        startServerNodes(3);
        startClientNodes(3);

        checkNodes(3, 3);

        resetClientIpFinder(2);

        srvFailedLatch = new CountDownLatch(2);
        clientFailedLatch = new CountDownLatch(3);

        attachListeners(2, 3);

        failServer(2);

        await(srvFailedLatch);
        await(clientFailedLatch);

        checkNodes(2, 3);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientNodeJoinOneServer() throws Exception {
        startServerNodes(1);

        srvJoinedLatch = new CountDownLatch(1);

        attachListeners(1, 0);

        startClientNodes(1);

        await(srvJoinedLatch);

        checkNodes(1, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientNodeLeaveOneServer() throws Exception {
        startServerNodes(1);
        startClientNodes(1);

        checkNodes(1, 1);

        srvLeftLatch = new CountDownLatch(1);

        attachListeners(1, 0);

        stopGrid("client-0");

        await(srvLeftLatch);

        checkNodes(1, 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientNodeFailOneServer() throws Exception {
        startServerNodes(1);
        startClientNodes(1);

        checkNodes(1, 1);

        srvFailedLatch = new CountDownLatch(1);

        attachListeners(1, 0);

        failClient(0);

        await(srvFailedLatch);

        checkNodes(1, 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMetrics() throws Exception {
        startServerNodes(3);
        startClientNodes(3);

        checkNodes(3, 3);

        attachListeners(3, 3);

        assertTrue(checkMetrics(3, 3, 0));

        G.ignite("client-0").compute().broadcast(F.noop());

        assertTrue(GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                return checkMetrics(3, 3, 1);
            }
        }, 10000));

        checkMetrics(3, 3, 1);

        G.ignite("server-0").compute().broadcast(F.noop());

        assertTrue(GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                return checkMetrics(3, 3, 2);
            }
        }, 10000));
    }

    /**
     * @param srvCnt Number of Number of server nodes.
     * @param clientCnt Number of client nodes.
     * @param execJobsCnt Expected number of executed jobs.
     * @return Whether metrics are correct.
     */
    private boolean checkMetrics(int srvCnt, int clientCnt, int execJobsCnt) {
        for (int i = 0; i < srvCnt; i++) {
            Ignite g = G.ignite("server-" + i);

            for (ClusterNode n : g.cluster().nodes()) {
                if (n.metrics().getTotalExecutedJobs() != execJobsCnt)
                    return false;
            }
        }

        for (int i = 0; i < clientCnt; i++) {
            Ignite g = G.ignite("client-" + i);

            for (ClusterNode n : g.cluster().nodes()) {
                if (n.metrics().getTotalExecutedJobs() != execJobsCnt)
                    return false;
            }
        }

        return true;
    }

    /**
     * @throws Exception If failed.
     */
    public void testDataExchangeFromServer() throws Exception {
        testDataExchange("server-0");
    }

    /**
     * TODO: IGNITE-587.
     *
     * @throws Exception If failed.
     */
    public void testDataExchangeFromClient() throws Exception {
        testDataExchange("client-0");
    }

    /**
     * @throws Exception If failed.
     */
    private void testDataExchange(String masterName) throws Exception {
        startServerNodes(2);
        startClientNodes(2);

        checkNodes(2, 2);

        IgniteMessaging msg = grid(masterName).message();

        UUID id = null;

        try {
            id = msg.remoteListen(null, new MessageListener());

            msgLatch = new CountDownLatch(4);

            msg.send(null, "Message 1");

            await(msgLatch);

            startServerNodes(1);
            startClientNodes(1);

            checkNodes(3, 3);

            msgLatch = new CountDownLatch(6);

            msg.send(null, "Message 2");

            await(msgLatch);
        }
        finally {
            if (id != null)
                msg.stopRemoteListen(id);
        }
    }

    /**
     * @param idx Index.
     * @throws Exception In case of error.
     */
    private void resetClientIpFinder(int idx) throws Exception {
        TcpClientDiscoverySpi disco =
            (TcpClientDiscoverySpi)G.ignite("client-" + idx).configuration().getDiscoverySpi();

        TcpDiscoveryVmIpFinder ipFinder = (TcpDiscoveryVmIpFinder)disco.getIpFinder();

        String addr = IP_FINDER.getRegisteredAddresses().iterator().next().toString();

        if (addr.startsWith("/"))
            addr = addr.substring(1);

        ipFinder.setAddresses(Arrays.asList(addr));
    }

    /**
     * @param cnt Number of nodes.
     * @throws Exception In case of error.
     */
    private void startServerNodes(int cnt) throws Exception {
        for (int i = 0; i < cnt; i++) {
            Ignite g = startGrid("server-" + srvIdx.getAndIncrement());

            srvNodeIds.add(g.cluster().localNode().id());
        }
    }

    /**
     * @param cnt Number of nodes.
     * @throws Exception In case of error.
     */
    private void startClientNodes(int cnt) throws Exception {
        for (int i = 0; i < cnt; i++) {
            Ignite g = startGrid("client-" + clientIdx.getAndIncrement());

            clientNodeIds.add(g.cluster().localNode().id());
        }
    }

    /**
     * @param idx Index.
     */
    private void failServer(int idx) {
        ((TcpDiscoverySpi)G.ignite("server-" + idx).configuration().getDiscoverySpi()).simulateNodeFailure();
    }

    /**
     * @param idx Index.
     */
    private void failClient(int idx) {
        ((TcpClientDiscoverySpi)G.ignite("client-" + idx).configuration().getDiscoverySpi()).simulateNodeFailure();
    }

    /**
     * @param srvCnt Number of server nodes.
     * @param clientCnt Number of client nodes.
     */
    private void attachListeners(int srvCnt, int clientCnt) throws Exception {
        if (srvJoinedLatch != null) {
            for (int i = 0; i < srvCnt; i++) {
                G.ignite("server-" + i).events().localListen(new IgnitePredicate<Event>() {
                    @Override public boolean apply(Event evt) {
                        info("Joined event fired on server: " + evt);

                        srvJoinedLatch.countDown();

                        return true;
                    }
                }, EVT_NODE_JOINED);
            }
        }

        if (srvLeftLatch != null) {
            for (int i = 0; i < srvCnt; i++) {
                G.ignite("server-" + i).events().localListen(new IgnitePredicate<Event>() {
                    @Override public boolean apply(Event evt) {
                        info("Left event fired on server: " + evt);

                        srvLeftLatch.countDown();

                        return true;
                    }
                }, EVT_NODE_LEFT);
            }
        }

        if (srvFailedLatch != null) {
            for (int i = 0; i < srvCnt; i++) {
                G.ignite("server-" + i).events().localListen(new IgnitePredicate<Event>() {
                    @Override public boolean apply(Event evt) {
                        info("Failed event fired on server: " + evt);

                        srvFailedLatch.countDown();

                        return true;
                    }
                }, EVT_NODE_FAILED);
            }
        }

        if (clientJoinedLatch != null) {
            for (int i = 0; i < clientCnt; i++) {
                G.ignite("client-" + i).events().localListen(new IgnitePredicate<Event>() {
                    @Override public boolean apply(Event evt) {
                        info("Joined event fired on client: " + evt);

                        clientJoinedLatch.countDown();

                        return true;
                    }
                }, EVT_NODE_JOINED);
            }
        }

        if (clientLeftLatch != null) {
            for (int i = 0; i < clientCnt; i++) {
                G.ignite("client-" + i).events().localListen(new IgnitePredicate<Event>() {
                    @Override public boolean apply(Event evt) {
                        info("Left event fired on client: " + evt);

                        clientLeftLatch.countDown();

                        return true;
                    }
                }, EVT_NODE_LEFT);
            }
        }

        if (clientFailedLatch != null) {
            for (int i = 0; i < clientCnt; i++) {
                G.ignite("client-" + i).events().localListen(new IgnitePredicate<Event>() {
                    @Override public boolean apply(Event evt) {
                        info("Failed event fired on client: " + evt);

                        clientFailedLatch.countDown();

                        return true;
                    }
                }, EVT_NODE_FAILED);
            }
        }
    }

    /**
     * @param srvCnt Number of server nodes.
     * @param clientCnt Number of client nodes.
     */
    private void checkNodes(int srvCnt, int clientCnt) {
        for (int i = 0; i < srvCnt; i++) {
            Ignite g = G.ignite("server-" + i);

            assertTrue(srvNodeIds.contains(g.cluster().localNode().id()));

            assertFalse(g.cluster().localNode().isClient());

            checkRemoteNodes(g, srvCnt + clientCnt - 1);
        }

        for (int i = 0; i < clientCnt; i++) {
            Ignite g = G.ignite("client-" + i);

            assertTrue(clientNodeIds.contains(g.cluster().localNode().id()));

            assertTrue(g.cluster().localNode().isClient());

            checkRemoteNodes(g, srvCnt + clientCnt - 1);
        }
    }

    /**
     * @param ignite Grid.
     * @param expCnt Expected nodes count.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private void checkRemoteNodes(Ignite ignite, int expCnt) {
        Collection<ClusterNode> nodes = ignite.cluster().forRemotes().nodes();

        assertEquals(expCnt, nodes.size());

        for (ClusterNode node : nodes) {
            UUID id = node.id();

            if (clientNodeIds.contains(id))
                assertTrue(node.isClient());
            else if (srvNodeIds.contains(id))
                assertFalse(node.isClient());
            else
                assert false : "Unexpected node ID: " + id;
        }
    }

    /**
     * @param latch Latch.
     * @throws InterruptedException If interrupted.
     */
    private void await(CountDownLatch latch) throws InterruptedException {
        assertTrue("Latch count: " + latch.getCount(), latch.await(10000, MILLISECONDS));
    }

    /**
     */
    private static class MessageListener implements IgniteBiPredicate<UUID, Object> {
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public boolean apply(UUID uuid, Object msg) {
            X.println(">>> Received [locNodeId=" + ignite.configuration().getNodeId() + ", msg=" + msg + ']');

            msgLatch.countDown();

            return true;
        }
    }
}
