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
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.io.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.discovery.tcp.internal.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.spi.discovery.tcp.messages.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.events.EventType.*;

/**
 * Client-based discovery tests.
 */
public class TcpClientDiscoverySpiSelfTest extends GridCommonAbstractTest {
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

    /** */
    private UUID nodeId;

    /** */
    private TcpDiscoveryVmIpFinder clientIpFinder;

    /** */
    private long joinTimeout = TcpDiscoverySpi.DFLT_JOIN_TIMEOUT;

    /** */
    private long netTimeout = TcpDiscoverySpi.DFLT_NETWORK_TIMEOUT;

    /** */
    private boolean longSockTimeouts;

    /** */
    private int maxMissedClientHbs = TcpDiscoverySpi.DFLT_MAX_MISSED_CLIENT_HEARTBEATS;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TestTcpDiscoverySpi();

        disco.setMaxMissedClientHeartbeats(maxMissedClientHbs);

        if (gridName.startsWith("server"))
            disco.setIpFinder(IP_FINDER);
        else if (gridName.startsWith("client")) {
            cfg.setClientMode(true);

            TcpDiscoveryVmIpFinder ipFinder;

            if (clientIpFinder != null)
                ipFinder = clientIpFinder;
            else {
                ipFinder = new TcpDiscoveryVmIpFinder();

                String addr = new ArrayList<>(IP_FINDER.getRegisteredAddresses()).
                    get((clientIdx.get() - 1) / clientsPerSrv).toString();

                if (addr.startsWith("/"))
                    addr = addr.substring(1);

                ipFinder.setAddresses(Collections.singletonList(addr));
            }

            disco.setIpFinder(ipFinder);

            String nodeId = cfg.getNodeId().toString();

            nodeId = "cc" + nodeId.substring(2);

            cfg.setNodeId(UUID.fromString(nodeId));
        }
        else
            throw new IllegalArgumentException();

        if (longSockTimeouts) {
            disco.setAckTimeout(2000);
            disco.setSocketTimeout(2000);
        }

        disco.setJoinTimeout(joinTimeout);
        disco.setNetworkTimeout(netTimeout);

        cfg.setDiscoverySpi(disco);

        if (nodeId != null)
            cfg.setNodeId(nodeId);

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

        nodeId = null;
        clientIpFinder = null;
        joinTimeout = TcpDiscoverySpi.DFLT_JOIN_TIMEOUT;
        netTimeout = TcpDiscoverySpi.DFLT_NETWORK_TIMEOUT;
        longSockTimeouts = false;

        assert G.allGrids().isEmpty();
    }

    /**
     *
     * @throws Exception
     */
    public void testJoinTimeout() throws Exception {
        clientIpFinder = new TcpDiscoveryVmIpFinder();
        joinTimeout = 1000;

        try {
            startClientNodes(1);

            fail("Client cannot be start because no server nodes run");
        }
        catch (IgniteCheckedException e) {
            IgniteSpiException spiEx = e.getCause(IgniteSpiException.class);

            assert spiEx != null : e;

            assert spiEx.getMessage().contains("Join process timed out") : spiEx.getMessage();
        }
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

        assert ((TcpDiscoverySpi)G.ignite("server-2").configuration().getDiscoverySpi()).clientWorkerCount() == 0;

        failServer(2);

        await(srvFailedLatch);
        await(clientFailedLatch);

        checkNodes(2, 3);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPing() throws Exception {
        startServerNodes(2);
        startClientNodes(1);

        Ignite srv0 = G.ignite("server-0");
        Ignite srv1 = G.ignite("server-1");
        Ignite client = G.ignite("client-0");

        assert ((IgniteEx)srv0).context().discovery().pingNode(client.cluster().localNode().id());
        assert ((IgniteEx)srv1).context().discovery().pingNode(client.cluster().localNode().id());

        assert ((IgniteEx)client).context().discovery().pingNode(srv0.cluster().localNode().id());
        assert ((IgniteEx)client).context().discovery().pingNode(srv1.cluster().localNode().id());
    }

    /**
     * @throws Exception If failed.
     */
    public void testPingFailedNodeFromClient() throws Exception {
        startServerNodes(2);
        startClientNodes(1);

        Ignite srv0 = G.ignite("server-0");
        Ignite srv1 = G.ignite("server-1");
        Ignite client = G.ignite("client-0");

        final CountDownLatch latch = new CountDownLatch(1);

        ((TcpDiscoverySpi)srv1.configuration().getDiscoverySpi()).addIncomeConnectionListener(new IgniteInClosure<Socket>() {
            @Override public void apply(Socket sock) {
                try {
                    latch.await();
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        assert ((IgniteEx)client).context().discovery().pingNode(srv0.cluster().localNode().id());
        assert !((IgniteEx)client).context().discovery().pingNode(srv1.cluster().localNode().id());

        latch.countDown();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPingFailedClientNode() throws Exception {
        startServerNodes(2);
        startClientNodes(1);

        Ignite srv0 = G.ignite("server-0");
        Ignite srv1 = G.ignite("server-1");
        Ignite client = G.ignite("client-0");

        ((TcpDiscoverySpi)srv0.configuration().getDiscoverySpi()).setAckTimeout(1000);

        ((TestTcpDiscoverySpi)client.configuration().getDiscoverySpi()).pauseSocketWrite();

        assert !((IgniteEx)srv1).context().discovery().pingNode(client.cluster().localNode().id());
        assert !((IgniteEx)srv0).context().discovery().pingNode(client.cluster().localNode().id());

        ((TestTcpDiscoverySpi)client.configuration().getDiscoverySpi()).resumeAll();

        assert ((IgniteEx)srv1).context().discovery().pingNode(client.cluster().localNode().id());
        assert ((IgniteEx)srv0).context().discovery().pingNode(client.cluster().localNode().id());
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientReconnectOnRouterFail() throws Exception {
        clientsPerSrv = 1;

        startServerNodes(3);
        startClientNodes(3);

        checkNodes(3, 3);

        setClientRouter(2, 0);

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
    public void testClientReconnectOnNetworkProblem() throws Exception {
        clientsPerSrv = 1;

        startServerNodes(3);
        startClientNodes(3);

        checkNodes(3, 3);

        setClientRouter(2, 0);

        srvFailedLatch = new CountDownLatch(2);
        clientFailedLatch = new CountDownLatch(3);

        attachListeners(2, 3);

        ((TcpDiscoverySpi)G.ignite("client-2").configuration().getDiscoverySpi()).brakeConnection();

        G.ignite("client-2").message().remoteListen(null, new MessageListener()); // Send some discovery message.

        checkNodes(3, 3);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientReconnectOneServerOneClient() throws Exception {
        clientsPerSrv = 1;

        startServerNodes(1);
        startClientNodes(1);

        checkNodes(1, 1);

        srvLeftLatch = new CountDownLatch(1);
        srvFailedLatch = new CountDownLatch(1);

        attachListeners(1, 0);

        ((TcpDiscoverySpi)G.ignite("client-0").configuration().getDiscoverySpi()).brakeConnection();

        assertFalse(srvFailedLatch.await(2000, TimeUnit.MILLISECONDS));

        assertEquals(1L, srvLeftLatch.getCount());

        checkNodes(1, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientReconnectTopologyChange1() throws Exception {
        maxMissedClientHbs = 100;

        clientsPerSrv = 1;

        startServerNodes(2);
        startClientNodes(1);

        checkNodes(2, 1);

        srvLeftLatch = new CountDownLatch(3);
        srvFailedLatch = new CountDownLatch(1);

        attachListeners(2, 0);

        Ignite ignite = G.ignite("client-0");

        TestTcpDiscoverySpi spi = ((TestTcpDiscoverySpi)ignite.configuration().getDiscoverySpi());

        spi.pauseAll();

        try {
            spi.brakeConnection();

            Ignite g = startGrid("server-" + srvIdx.getAndIncrement());

            g.close();

            spi.resumeAll();

            assertFalse(srvFailedLatch.await(2000, TimeUnit.MILLISECONDS));

            assertEquals(1L, srvLeftLatch.getCount());

            checkNodes(2, 1);
        }
        finally {
            spi.resumeAll();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientReconnectTopologyChange2() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-998");

        maxMissedClientHbs = 100;

        clientsPerSrv = 1;

        startServerNodes(1);
        startClientNodes(1);

        checkNodes(1, 1);

        srvLeftLatch = new CountDownLatch(2);
        srvFailedLatch = new CountDownLatch(1);

        attachListeners(1, 0);

        Ignite ignite = G.ignite("client-0");

        TestTcpDiscoverySpi spi = ((TestTcpDiscoverySpi)ignite.configuration().getDiscoverySpi());

        spi.pauseAll();

        try {
            spi.brakeConnection();

            Ignite g = startGrid("server-" + srvIdx.getAndIncrement());

            g.close();

            spi.resumeAll();

            assertFalse(srvFailedLatch.await(2000, TimeUnit.MILLISECONDS));

            assertEquals(1L, srvLeftLatch.getCount());

            checkNodes(1, 1);
        }
        finally {
            spi.resumeAll();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetMissedMessagesOnReconnect() throws Exception {
        clientsPerSrv = 1;

        startServerNodes(3);
        startClientNodes(2);

        checkNodes(3, 2);

        clientLeftLatch = new CountDownLatch(1);
        srvLeftLatch = new CountDownLatch(2);

        attachListeners(2, 2);

        ((TestTcpDiscoverySpi)G.ignite("client-1").configuration().getDiscoverySpi()).pauseAll();

        stopGrid("server-2");

        await(srvLeftLatch);
        await(srvLeftLatch);

        Thread.sleep(500);

        assert G.ignite("client-0").cluster().nodes().size() == 4;
        assert G.ignite("client-1").cluster().nodes().size() == 5;

        clientLeftLatch = new CountDownLatch(1);

        ((TestTcpDiscoverySpi)G.ignite("client-1").configuration().getDiscoverySpi()).resumeAll();

        await(clientLeftLatch);

        checkNodes(2, 2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientSegmentation() throws Exception {
        clientsPerSrv = 1;

        startServerNodes(3);
        startClientNodes(3);

        checkNodes(3, 3);

        srvFailedLatch = new CountDownLatch(2 + 2);
        clientFailedLatch = new CountDownLatch(2 + 2);

        attachListeners(2, 2);

        final CountDownLatch client2StoppedLatch = new CountDownLatch(1);

        IgnitionListener lsnr = new IgnitionListener() {
            @Override public void onStateChange(@Nullable String name, IgniteState state) {
                if (state == IgniteState.STOPPED_ON_SEGMENTATION)
                    client2StoppedLatch.countDown();
            }
        };
        G.addListener(lsnr);

        final TcpDiscoverySpi disco = (TcpDiscoverySpi)G.ignite("client-2").configuration().getDiscoverySpi();

        try {
            failServer(2);

            await(srvFailedLatch);
            await(clientFailedLatch);

            await(client2StoppedLatch);

            checkNodes(2, 2);
        }
        finally {
            G.removeListener(lsnr);
        }

        assert disco.getRemoteNodes().isEmpty();
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
    public void testClientAndRouterFail() throws Exception {
        startServerNodes(2);
        startClientNodes(2);

        checkNodes(2, 2);

        srvFailedLatch = new CountDownLatch(2);
        clientFailedLatch = new CountDownLatch(2);

        attachListeners(1, 1);

        ((TcpDiscoverySpi)G.ignite("server-1").configuration().getDiscoverySpi()).addSendMessageListener(new IgniteInClosure<TcpDiscoveryAbstractMessage>() {
            @Override public void apply(TcpDiscoveryAbstractMessage msg) {
                try {
                    Thread.sleep(1000000);
                }
                catch (InterruptedException ignored) {
                    Thread.interrupted();
                }
            }
        });
        failClient(1);
        failServer(1);

        await(srvFailedLatch);
        await(clientFailedLatch);

        checkNodes(1, 1);
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
                if (n.metrics().getTotalExecutedJobs() != (n.isClient() ? 0 : execJobsCnt))
                    return false;
            }
        }

        for (int i = 0; i < clientCnt; i++) {
            Ignite g = G.ignite("client-" + i);

            for (ClusterNode n : g.cluster().nodes()) {
                if (n.metrics().getTotalExecutedJobs() != (n.isClient() ? 0 : execJobsCnt))
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
     * @throws Exception If failed.
     */
    public void testDataExchangeFromClient() throws Exception {
        testDataExchange("client-0");
    }

    /**
     * @param masterName Node name
     * @throws Exception If failed.
     */
    private void testDataExchange(String masterName) throws Exception {
        startServerNodes(2);
        startClientNodes(2);

        checkNodes(2, 2);

        IgniteMessaging msg = grid(masterName).message();

        UUID id = msg.remoteListen(null, new MessageListener());

        try {
            msgLatch = new CountDownLatch(2);

            msg.send(null, "Message 1");

            await(msgLatch);

            startServerNodes(1);
            startClientNodes(1);

            checkNodes(3, 3);

            msgLatch = new CountDownLatch(3);

            msg.send(null, "Message 2");

            await(msgLatch);
        }
        finally {
            msg.stopRemoteListen(id);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDataExchangeFromServer2() throws Exception {
        startServerNodes(2);

        IgniteMessaging msg = grid("server-1").message();

        UUID id = msg.remoteListen(null, new MessageListener());

        try {
            startClientNodes(1);

            assertEquals(G.ignite("server-0").cluster().localNode().id(), ((TcpDiscoveryNode)G.ignite("client-0")
                .cluster().localNode()).clientRouterNodeId());

            checkNodes(2, 1);

            msgLatch = new CountDownLatch(3);

            msg.send(null, "Message");

            await(msgLatch);
        }
        finally {
            msg.stopRemoteListen(id);
        }
    }


    /**
     * @throws Exception If any error occurs.
     */
    public void testDuplicateId() throws Exception {
        startServerNodes(2);

        nodeId = G.ignite("server-1").cluster().localNode().id();

        try {
            startGrid("client-0");

            assert false;
        }
        catch (IgniteCheckedException e) {
            IgniteSpiException spiEx = e.getCause(IgniteSpiException.class);

            assert spiEx != null : e;
            assert spiEx.getMessage().contains("same ID") : spiEx.getMessage();
        }
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testTimeoutWaitingNodeAddedMessage() throws Exception {
        longSockTimeouts = true;

        startServerNodes(2);

        final CountDownLatch cnt = new CountDownLatch(1);

        ((TcpDiscoverySpi)G.ignite("server-1").configuration().getDiscoverySpi()).addSendMessageListener(
            new IgniteInClosure<TcpDiscoveryAbstractMessage>() {
                @Override public void apply(TcpDiscoveryAbstractMessage msg) {
                    try {
                        cnt.await(10, MINUTES);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();

                        throw new IgniteInterruptedException(e);
                    }
                }
            });

        try {
            netTimeout = 500;

            startGrid("client-0");

            assert false;
        }
        catch (IgniteCheckedException e) {
            cnt.countDown();

            IgniteSpiException spiEx = e.getCause(IgniteSpiException.class);

            assert spiEx != null : e;
            assert spiEx.getMessage().contains("Join process timed out") : spiEx.getMessage();
        }
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testGridStartTime() throws Exception {
        startServerNodes(2);

        startClientNodes(2);

        long startTime = -1;

        for (Ignite g : G.allGrids()) {
            IgniteEx kernal = (IgniteEx)g;

            assertTrue(kernal.context().discovery().gridStartTime() > 0);

            if (startTime == -1)
                startTime = kernal.context().discovery().gridStartTime();
            else
                assertEquals(startTime, kernal.context().discovery().gridStartTime());
        }
    }

    /**
     * @param clientIdx Client index.
     * @param srvIdx Server index.
     * @throws Exception In case of error.
     */
    private void setClientRouter(int clientIdx, int srvIdx) throws Exception {
        TcpDiscoverySpi disco =
            (TcpDiscoverySpi)G.ignite("client-" + clientIdx).configuration().getDiscoverySpi();

        TcpDiscoveryVmIpFinder ipFinder = (TcpDiscoveryVmIpFinder)disco.getIpFinder();

        String addr = new ArrayList<>(IP_FINDER.getRegisteredAddresses()).get(srvIdx).toString();

        if (addr.startsWith("/"))
            addr = addr.substring(1);

        ipFinder.setAddresses(Collections.singletonList(addr));
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
        ((TcpDiscoverySpi)G.ignite("client-" + idx).configuration().getDiscoverySpi()).simulateNodeFailure();
    }

    /**
     * @param srvCnt Number of server nodes.
     * @param clientCnt Number of client nodes.
     * @throws Exception If failed.
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
        long topVer = -1;

        for (int i = 0; i < srvCnt; i++) {
            Ignite g = G.ignite("server-" + i);

            assertTrue(srvNodeIds.contains(g.cluster().localNode().id()));

            assertFalse(g.cluster().localNode().isClient());

            checkRemoteNodes(g, srvCnt + clientCnt - 1);

            if (topVer < 0)
                topVer = g.cluster().topologyVersion();
            else
                assertEquals(topVer, g.cluster().topologyVersion());
        }

        for (int i = 0; i < clientCnt; i++) {
            Ignite g = G.ignite("client-" + i);

            ((TcpDiscoverySpi)g.configuration().getDiscoverySpi()).waitForClientMessagePrecessed();

            assertTrue(clientNodeIds.contains(g.cluster().localNode().id()));

            assertTrue(g.cluster().localNode().isClient());

            checkRemoteNodes(g, srvCnt + clientCnt - 1);

            if (topVer < 0)
                topVer = g.cluster().topologyVersion();
            else
                assertEquals(topVer, g.cluster().topologyVersion());
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
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public boolean apply(UUID uuid, Object msg) {
            X.println(">>> Received [locNodeId=" + ignite.configuration().getNodeId() + ", msg=" + msg + ']');

            msgLatch.countDown();

            return true;
        }
    }

    /**
     *
     */
    private static class TestTcpDiscoverySpi extends TcpDiscoverySpi {
        /** */
        private final Object mux = new Object();

        /** */
        private final AtomicBoolean writeLock = new AtomicBoolean();

        /** */
        private final AtomicBoolean openSockLock = new AtomicBoolean();

        /**
         * @param lock Lock.
         */
        private void waitFor(AtomicBoolean lock) {
            try {
                synchronized (mux) {
                    while (lock.get())
                        mux.wait();
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new RuntimeException(e);
            }
        }

        /**
         * @param isPause Is lock.
         * @param locks Locks.
         */
        private void pauseResumeOperation(boolean isPause, AtomicBoolean... locks) {
            synchronized (mux) {
                for (AtomicBoolean lock : locks)
                    lock.set(isPause);

                mux.notifyAll();
            }
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg,
            GridByteArrayOutputStream bout) throws IOException, IgniteCheckedException {
            waitFor(writeLock);

            super.writeToSocket(sock, msg, bout);
        }

        /** {@inheritDoc} */
        @Override protected Socket openSocket(InetSocketAddress sockAddr) throws IOException {
            waitFor(openSockLock);

            return super.openSocket(sockAddr);
        }

        /**
         *
         */
        public void pauseSocketWrite() {
            pauseResumeOperation(true, writeLock);
        }

        /**
         *
         */
        public void pauseAll() {
            pauseResumeOperation(true, openSockLock, writeLock);

            impl.workerThread().suspend();
        }

        /**
         *
         */
        public void resumeAll() {
            pauseResumeOperation(false, openSockLock, writeLock);

            impl.workerThread().resume();
        }
    }
}
