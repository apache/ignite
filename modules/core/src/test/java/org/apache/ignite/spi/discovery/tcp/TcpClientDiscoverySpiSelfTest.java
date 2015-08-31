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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.IgniteState;
import org.apache.ignite.Ignition;
import org.apache.ignite.IgnitionListener;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.io.GridByteArrayOutputStream;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.lang.IgniteInClosure2X;
import org.apache.ignite.internal.util.typedef.CIX2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutException;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutHelper;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientReconnectMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryJoinRequestMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddFinishedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddedMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_RECONNECTED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.events.EventType.EVT_NODE_SEGMENTED;

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

    /** */
    private IgniteInClosure2X<TcpDiscoveryAbstractMessage, Socket> afterWrite;

    /** */
    private boolean reconnectDisabled;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = getDiscoverySpi();

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

        if (useFailureDetectionTimeout())
            cfg.setFailureDetectionTimeout(failureDetectionTimeout());
        else {
            if (longSockTimeouts) {
                disco.setAckTimeout(2000);
                disco.setSocketTimeout(2000);
            }
            else {
                disco.setAckTimeout(gridName.startsWith("client") ? TcpDiscoverySpi.DFLT_ACK_TIMEOUT_CLIENT :
                    TcpDiscoverySpi.DFLT_ACK_TIMEOUT);
                disco.setSocketTimeout(gridName.startsWith("client") ? TcpDiscoverySpi.DFLT_SOCK_TIMEOUT_CLIENT :
                    TcpDiscoverySpi.DFLT_SOCK_TIMEOUT);
            }
        }

        disco.setJoinTimeout(joinTimeout);
        disco.setNetworkTimeout(netTimeout);

        disco.setClientReconnectDisabled(reconnectDisabled);

        if (disco instanceof TestTcpDiscoverySpi)
            ((TestTcpDiscoverySpi)disco).afterWrite(afterWrite);

        cfg.setDiscoverySpi(disco);

        if (nodeId != null)
            cfg.setNodeId(nodeId);

        return cfg;
    }

    /**
     * Returns TCP Discovery SPI instance to use in a test.
     * @return TCP Discovery SPI.
     */
    protected TcpDiscoverySpi getDiscoverySpi() {
        return new TestTcpDiscoverySpi();
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
     * Checks whether to use failure detection timeout instead of setting explicit timeouts.
     *
     * @return {@code true} if use.
     */
    protected boolean useFailureDetectionTimeout() {
        return false;
    }

    /**
     * Gets failure detection timeout to use.
     *
     * @return Failure detection timeout.
     */
    protected long failureDetectionTimeout() {
        return 0;
    }

    /**
     * @throws Exception If failed.
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

        ((TcpDiscoverySpi)srv1.configuration().getDiscoverySpi()).addIncomeConnectionListener(new IgniteInClosure
            <Socket>() {
            @Override public void apply(Socket sock) {
                try {
                    latch.await();
                } catch (InterruptedException e) {
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

        checkNodes(2, 1);

        Ignite srv0 = G.ignite("server-0");
        Ignite srv1 = G.ignite("server-1");
        Ignite client = G.ignite("client-0");

        if (!useFailureDetectionTimeout())
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

        spi.pauseAll(false);

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

        spi.pauseAll(false);

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

        ((TestTcpDiscoverySpi)G.ignite("client-1").configuration().getDiscoverySpi()).pauseAll(true);

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

        reconnectDisabled = true;

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
            log.info("Fail server: " + 2);

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
                } catch (InterruptedException ignored) {
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

            assertEquals(G.ignite("server-0").cluster().localNode().id(),
                ((TcpDiscoveryNode) G.ignite("client-0").cluster().localNode()).clientRouterNodeId());

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
     * @throws Exception If failed.
     */
    public void testJoinError() throws Exception {
        startServerNodes(1);

        Ignite ignite = G.ignite("server-0");

        TestTcpDiscoverySpi srvSpi = ((TestTcpDiscoverySpi)ignite.configuration().getDiscoverySpi());

        srvSpi.failNodeAddedMessage();

        startClientNodes(1);

        checkNodes(1, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinError2() throws Exception {
        startServerNodes(1);

        Ignite ignite = G.ignite("server-0");

        TestTcpDiscoverySpi srvSpi = ((TestTcpDiscoverySpi)ignite.configuration().getDiscoverySpi());

        srvSpi.failNodeAddedMessage();
        srvSpi.failClientReconnectMessage();

        startClientNodes(1);

        checkNodes(1, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinError3() throws Exception {
        startServerNodes(1);

        Ignite ignite = G.ignite("server-0");

        TestTcpDiscoverySpi srvSpi = ((TestTcpDiscoverySpi)ignite.configuration().getDiscoverySpi());

        srvSpi.failNodeAddFinishedMessage();

        startClientNodes(1);

        checkNodes(1, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinErrorMissedAddFinishedMessage1() throws Exception {
        missedAddFinishedMessage(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinErrorMissedAddFinishedMessage2() throws Exception {
        missedAddFinishedMessage(false);
    }

    /**
     * @param singleSrv If {@code true} starts one server node two otherwise.
     * @throws Exception If failed.
     */
    private void missedAddFinishedMessage(boolean singleSrv) throws Exception {
        int srvs = singleSrv ? 1 : 2;

        startServerNodes(srvs);

        afterWrite = new CIX2<TcpDiscoveryAbstractMessage, Socket>() {
            private boolean first = true;

            @Override public void applyx(TcpDiscoveryAbstractMessage msg, Socket sock) throws IgniteCheckedException {
                if (first && (msg instanceof TcpDiscoveryJoinRequestMessage)) {
                    first = false;

                    log.info("Close socket after message write [msg=" + msg + "]");

                    try {
                        sock.close();
                    }
                    catch (IOException e) {
                        throw new IgniteCheckedException(e);
                    }

                    log.info("Delay after message write [msg=" + msg + "]");

                    U.sleep(5000); // Wait when server process join request.
                }
            }
        };

        Ignite srv = singleSrv ? G.ignite("server-0") : G.ignite("server-1");

        TcpDiscoveryNode srvNode = (TcpDiscoveryNode)srv.cluster().localNode();

        assertEquals(singleSrv ? 1 : 2, srvNode.order());

        clientIpFinder = new TcpDiscoveryVmIpFinder();

        clientIpFinder.setAddresses(Collections.singleton("localhost:" + srvNode.discoveryPort()));

        startClientNodes(1);

        TcpDiscoveryNode clientNode = (TcpDiscoveryNode)G.ignite("client-0").cluster().localNode();

        assertEquals(srvNode.id(), clientNode.clientRouterNodeId());

        checkNodes(srvs, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientMessageWorkerStartSingleServer() throws Exception {
        clientMessageWorkerStart(1, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientMessageWorkerStartTwoServers1() throws Exception {
        clientMessageWorkerStart(2, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientMessageWorkerStartTwoServers2() throws Exception {
        clientMessageWorkerStart(2, 2);
    }

    /**
     * @param srvs Number of server nodes.
     * @param connectTo What server connect to.
     * @throws Exception If failed.
     */
    private void clientMessageWorkerStart(int srvs, int connectTo) throws Exception {
        startServerNodes(srvs);

        Ignite srv = G.ignite("server-" + (connectTo - 1));

        final TcpDiscoveryNode srvNode = (TcpDiscoveryNode)srv.cluster().localNode();

        assertEquals((long)connectTo, srvNode.order());

        TestTcpDiscoverySpi srvSpi = ((TestTcpDiscoverySpi)srv.configuration().getDiscoverySpi());

        final String client0 = "client-" + clientIdx.getAndIncrement();

        srvSpi.delayJoinAckFor = client0;

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                clientIpFinder = new TcpDiscoveryVmIpFinder();

                clientIpFinder.setAddresses(Collections.singleton("localhost:" + srvNode.discoveryPort()));

                Ignite client = startGrid(client0);

                clientIpFinder = null;

                clientNodeIds.add(client.cluster().localNode().id());

                TestTcpDiscoverySpi clientSpi = ((TestTcpDiscoverySpi)client.configuration().getDiscoverySpi());

                assertFalse(clientSpi.invalidResponse());

                TcpDiscoveryNode clientNode = (TcpDiscoveryNode)client.cluster().localNode();

                assertEquals(srvNode.id(), clientNode.clientRouterNodeId());

                return null;
            }
        });

        final String client1 = "client-" + clientIdx.getAndIncrement();

        while (!fut.isDone()) {
            startGrid(client1);

            stopGrid(client1);
        }

        fut.get();

        checkNodes(srvs, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinMutlithreaded() throws Exception {
        startServerNodes(1);

        final int CLIENTS = 30;

        clientsPerSrv = CLIENTS;

        GridTestUtils.runMultiThreaded(new Callable<Void>() {
            @Override public Void call() throws Exception {
                Ignite g = startGrid("client-" + clientIdx.getAndIncrement());

                clientNodeIds.add(g.cluster().localNode().id());

                return null;
            }
        }, CLIENTS, "start-client");

        checkNodes(1, CLIENTS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReconnectAfterFail() throws Exception {
        reconnectAfterFail(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReconnectAfterFailTopologyChanged() throws Exception {
        reconnectAfterFail(true);
    }

    /**
     * @param changeTop If {@code true} topology is changed after client disconnects.
     * @throws Exception If failed.
     */
    private void reconnectAfterFail(final boolean changeTop) throws Exception {
        startServerNodes(1);

        startClientNodes(1);

        Ignite srv = G.ignite("server-0");

        TestTcpDiscoverySpi srvSpi = ((TestTcpDiscoverySpi)srv.configuration().getDiscoverySpi());

        Ignite client = G.ignite("client-0");

        final ClusterNode clientNode = client.cluster().localNode();

        final UUID clientId = clientNode.id();

        final TestTcpDiscoverySpi clientSpi = ((TestTcpDiscoverySpi)client.configuration().getDiscoverySpi());

        assertEquals(2L, clientNode.order());

        final CountDownLatch failLatch = new CountDownLatch(1);

        final CountDownLatch joinLatch = new CountDownLatch(1);

        srv.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                info("Server event: " + evt);

                DiscoveryEvent evt0 = (DiscoveryEvent)evt;

                if (evt0.eventNode().id().equals(clientId) && (evt.type() == EVT_NODE_FAILED)) {
                    if (evt.type() == EVT_NODE_FAILED)
                        failLatch.countDown();
                }
                else if (evt.type() == EVT_NODE_JOINED) {
                    TcpDiscoveryNode node = (TcpDiscoveryNode)evt0.eventNode();

                    if ("client-0".equals(node.attribute(IgniteNodeAttributes.ATTR_GRID_NAME))) {
                        assertEquals(changeTop ? 5L : 4L, node.order());

                        joinLatch.countDown();
                    }
                }

                return true;
            }
        }, EVT_NODE_FAILED, EVT_NODE_JOINED);

        final CountDownLatch reconnectLatch = new CountDownLatch(1);

        final CountDownLatch disconnectLatch = new CountDownLatch(1);

        client.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                info("Client event: " + evt);

                if (evt.type() == EVT_CLIENT_NODE_DISCONNECTED) {
                    assertEquals(1, reconnectLatch.getCount());

                    disconnectLatch.countDown();

                    if (changeTop)
                        clientSpi.pauseAll(false);
                }
                else if (evt.type() == EVT_CLIENT_NODE_RECONNECTED) {
                    assertEquals(0, disconnectLatch.getCount());

                    reconnectLatch.countDown();
                }

                return true;
            }
        }, EVT_CLIENT_NODE_DISCONNECTED, EVT_CLIENT_NODE_RECONNECTED);

        srvSpi.failNode(client.cluster().localNode().id(), null);

        if (changeTop) {
            Ignite g = startGrid("server-" + srvIdx.getAndIncrement());

            srvNodeIds.add(g.cluster().localNode().id());

            clientSpi.resumeAll();
        }

        assertTrue(disconnectLatch.await(5000, MILLISECONDS));
        assertTrue(reconnectLatch.await(5000, MILLISECONDS));
        assertTrue(failLatch.await(5000, MILLISECONDS));
        assertTrue(joinLatch.await(5000, MILLISECONDS));

        long topVer = changeTop ? 5L : 4L;

        assertEquals(topVer, client.cluster().localNode().order());

        assertEquals(topVer, client.cluster().topologyVersion());

        Collection<ClusterNode> clientTop = client.cluster().topology(topVer);

        assertEquals(changeTop ? 3 : 2, clientTop.size());

        clientNodeIds.remove(clientId);

        clientNodeIds.add(client.cluster().localNode().id());

        checkNodes(changeTop ? 2 : 1, 1);

        Ignite g = startGrid("server-" + srvIdx.getAndIncrement());

        srvNodeIds.add(g.cluster().localNode().id());

        checkNodes(changeTop ? 3 : 2, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReconnectAfterFailConcurrentJoin() throws Exception {
        startServerNodes(1);

        startClientNodes(1);

        Ignite srv = G.ignite("server-0");

        TestTcpDiscoverySpi srvSpi = ((TestTcpDiscoverySpi)srv.configuration().getDiscoverySpi());

        Ignite client = G.ignite("client-0");

        final ClusterNode clientNode = client.cluster().localNode();

        assertEquals(2L, clientNode.order());

        final CountDownLatch reconnectLatch = new CountDownLatch(1);
        final CountDownLatch disconnectLatch = new CountDownLatch(1);

        client.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                info("Client event: " + evt);

                if (evt.type() == EVT_CLIENT_NODE_DISCONNECTED) {
                    assertEquals(1, reconnectLatch.getCount());

                    disconnectLatch.countDown();
                }
                else if (evt.type() == EVT_CLIENT_NODE_RECONNECTED) {
                    assertEquals(0, disconnectLatch.getCount());

                    reconnectLatch.countDown();
                }

                return true;
            }
        }, EVT_CLIENT_NODE_DISCONNECTED, EVT_CLIENT_NODE_RECONNECTED);

        final int CLIENTS = 20;

        clientsPerSrv = CLIENTS + 1;

        final CountDownLatch latch = new CountDownLatch(1);

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                latch.await();

                Ignite g = startGrid("client-" + clientIdx.getAndIncrement());

                clientNodeIds.add(g.cluster().localNode().id());

                return null;
            }
        }, CLIENTS, "start-client");

        srvSpi.failNode(client.cluster().localNode().id(), null);

        latch.countDown();

        assertTrue(disconnectLatch.await(awaitTime(), MILLISECONDS));
        assertTrue(reconnectLatch.await(awaitTime(), MILLISECONDS));

        clientNodeIds.add(client.cluster().localNode().id());

        fut.get();

        checkNodes(1, CLIENTS + 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientFailReconnectDisabled() throws Exception {
        reconnectDisabled = true;

        startServerNodes(1);

        startClientNodes(1);

        Ignite srv = G.ignite("server-0");

        TestTcpDiscoverySpi srvSpi = ((TestTcpDiscoverySpi)srv.configuration().getDiscoverySpi());

        Ignite client = G.ignite("client-0");

        final CountDownLatch segmentedLatch = new CountDownLatch(1);

        client.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                if (evt.type() == EVT_NODE_SEGMENTED)
                    segmentedLatch.countDown();

                return false;
            }
        }, EVT_NODE_SEGMENTED);

        srvFailedLatch = new CountDownLatch(1);

        attachListeners(1, 0);

        log.info("Fail client node.");

        srvSpi.failNode(client.cluster().localNode().id(), null);

        assertTrue(srvFailedLatch.await(5000, MILLISECONDS));
        assertTrue(segmentedLatch.await(5000, MILLISECONDS));

        checkNodes(1, 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReconnectSegmentedAfterJoinTimeoutServerFailed() throws Exception {
        reconnectSegmentedAfterJoinTimeout(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReconnectSegmentedAfterJoinTimeoutNetworkError() throws Exception {
        reconnectSegmentedAfterJoinTimeout(false);
    }

    /**
     * @param failSrv If {@code true} fails server, otherwise server does not send join message.
     * @throws Exception If failed.
     */
    protected void reconnectSegmentedAfterJoinTimeout(boolean failSrv) throws Exception {
        netTimeout = 4000;
        joinTimeout = 5000;

        startServerNodes(1);

        startClientNodes(1);

        final Ignite srv = G.ignite("server-0");
        Ignite client = G.ignite("client-0");

        TestTcpDiscoverySpi srvSpi = ((TestTcpDiscoverySpi)srv.configuration().getDiscoverySpi());
        TestTcpDiscoverySpi clientSpi = ((TestTcpDiscoverySpi)client.configuration().getDiscoverySpi());

        final CountDownLatch disconnectLatch = new CountDownLatch(1);
        final CountDownLatch segmentedLatch = new CountDownLatch(1);
        final AtomicBoolean err = new AtomicBoolean(false);

        if (!failSrv) {
            srvFailedLatch = new CountDownLatch(1);

            attachListeners(1, 0);
        }

        client.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                if (evt.type() == EVT_CLIENT_NODE_DISCONNECTED) {
                    log.info("Disconnected event.");

                    assertEquals(1, segmentedLatch.getCount());
                    assertEquals(1, disconnectLatch.getCount());
                    assertFalse(err.get());

                    disconnectLatch.countDown();
                }
                else if (evt.type() == EVT_NODE_SEGMENTED) {
                    log.info("Segmented event.");

                    assertEquals(1, segmentedLatch.getCount());
                    assertEquals(0, disconnectLatch.getCount());
                    assertFalse(err.get());

                    segmentedLatch.countDown();
                }
                else {
                    log.error("Unexpected event: " + evt);

                    err.set(true);
                }

                return true;
            }
        }, EVT_CLIENT_NODE_DISCONNECTED, EVT_CLIENT_NODE_RECONNECTED, EVT_NODE_SEGMENTED);

        if (failSrv) {
            log.info("Fail server.");

            failServer(0);
        }
        else {
            log.info("Fail client connection.");

            srvSpi.failClientReconnect.set(1_000_000);
            srvSpi.skipNodeAdded = true;

            clientSpi.brakeConnection();
        }

        assertTrue(disconnectLatch.await(awaitTime(), MILLISECONDS));

        assertTrue(segmentedLatch.await(awaitTime(), MILLISECONDS));

        waitSegmented(client);

        assertFalse(err.get());

        if (!failSrv) {
            await(srvFailedLatch);

            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return srv.cluster().nodes().size() == 1;
                }
            }, awaitTime());

            checkNodes(1, 0);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testReconnectClusterRestart() throws Exception {
        netTimeout = 3000;
        joinTimeout = 60_000;

        final CountDownLatch disconnectLatch = new CountDownLatch(1);
        final CountDownLatch reconnectLatch = new CountDownLatch(1);
        final AtomicBoolean err = new AtomicBoolean(false);

        startServerNodes(1);

        startClientNodes(1);

        Ignite srv = G.ignite("server-0");
        Ignite client = G.ignite("client-0");

        client.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                if (evt.type() == EVT_CLIENT_NODE_DISCONNECTED) {
                    log.info("Disconnected event.");

                    assertEquals(1, reconnectLatch.getCount());
                    assertEquals(1, disconnectLatch.getCount());

                    disconnectLatch.countDown();
                }
                else if (evt.type() == EVT_CLIENT_NODE_RECONNECTED) {
                    log.info("Reconnected event.");

                    assertEquals(1, reconnectLatch.getCount());
                    assertEquals(0, disconnectLatch.getCount());
                    assertFalse(err.get());

                    reconnectLatch.countDown();
                }
                else {
                    log.error("Unexpected event: " + evt);

                    err.set(true);
                }

                return true;
            }
        }, EVT_CLIENT_NODE_DISCONNECTED, EVT_CLIENT_NODE_RECONNECTED, EVT_NODE_SEGMENTED);

        log.info("Stop server.");

        srv.close();

        assertTrue(disconnectLatch.await(awaitTime(), MILLISECONDS));

        srvNodeIds.clear();
        srvIdx.set(0);

        Thread.sleep(3000);

        log.info("Restart server.");

        startServerNodes(1);

        assertTrue(reconnectLatch.await(awaitTime(), MILLISECONDS));

        clientNodeIds.clear();
        clientNodeIds.add(client.cluster().localNode().id());

        checkNodes(1, 1);

        assertFalse(err.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testDisconnectAfterNetworkTimeout() throws Exception {
        netTimeout = 5000;
        joinTimeout = 60_000;
        maxMissedClientHbs = 2;

        startServerNodes(1);

        startClientNodes(1);

        final Ignite srv = G.ignite("server-0");
        Ignite client = G.ignite("client-0");

        TestTcpDiscoverySpi srvSpi = ((TestTcpDiscoverySpi)srv.configuration().getDiscoverySpi());
        TestTcpDiscoverySpi clientSpi = ((TestTcpDiscoverySpi)client.configuration().getDiscoverySpi());

        final CountDownLatch disconnectLatch = new CountDownLatch(1);
        final CountDownLatch reconnectLatch = new CountDownLatch(1);
        final AtomicBoolean err = new AtomicBoolean(false);

        client.events().localListen(new IgnitePredicate<Event>() {
            @Override
            public boolean apply(Event evt) {
                if (evt.type() == EVT_CLIENT_NODE_DISCONNECTED) {
                    log.info("Disconnected event.");

                    assertEquals(1, reconnectLatch.getCount());
                    assertEquals(1, disconnectLatch.getCount());
                    assertFalse(err.get());

                    disconnectLatch.countDown();
                }
                else if (evt.type() == EVT_CLIENT_NODE_RECONNECTED) {
                    log.info("Reconnected event.");

                    assertEquals(1, reconnectLatch.getCount());
                    assertEquals(0, disconnectLatch.getCount());
                    assertFalse(err.get());

                    reconnectLatch.countDown();
                }
                else {
                    log.error("Unexpected event: " + evt);

                    err.set(true);
                }

                return true;
            }
        }, EVT_CLIENT_NODE_DISCONNECTED, EVT_CLIENT_NODE_RECONNECTED, EVT_NODE_SEGMENTED);

        log.info("Fail client connection1.");

        srvSpi.failClientReconnect.set(1_000_000);
        srvSpi.skipNodeAdded = true;

        clientSpi.brakeConnection();

        assertTrue(disconnectLatch.await(awaitTime(), MILLISECONDS));

        log.info("Fail client connection2.");

        srvSpi.failClientReconnect.set(0);
        srvSpi.skipNodeAdded = false;

        clientSpi.brakeConnection();

        assertTrue(reconnectLatch.await(awaitTime(), MILLISECONDS));

        clientNodeIds.clear();

        clientNodeIds.add(client.cluster().localNode().id());

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override
            public boolean apply() {
                return srv.cluster().nodes().size() == 2;
            }
        }, awaitTime());

        checkNodes(1, 1);

        assertFalse(err.get());
    }

    /**
     * @param ignite Ignite.
     * @throws Exception If failed.
     */
    private void waitSegmented(final Ignite ignite) throws Exception {
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return IgniteState.STOPPED_ON_SEGMENTATION == Ignition.state(ignite.name());
            }
        }, 5000);

        assertEquals(IgniteState.STOPPED_ON_SEGMENTATION, Ignition.state(ignite.name()));
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
    protected void startServerNodes(int cnt) throws Exception {
        for (int i = 0; i < cnt; i++) {
            Ignite g = startGrid("server-" + srvIdx.getAndIncrement());

            srvNodeIds.add(g.cluster().localNode().id());
        }
    }

    /**
     * @param cnt Number of nodes.
     * @throws Exception In case of error.
     */
    protected void startClientNodes(int cnt) throws Exception {
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
    protected void checkNodes(int srvCnt, int clientCnt) {
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

        assertEquals("Unexpected state for node: " + ignite.name(), expCnt, nodes.size());

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
    protected void await(CountDownLatch latch) throws InterruptedException {
        assertTrue("Latch count: " + latch.getCount(), latch.await(awaitTime(), MILLISECONDS));
    }

    /**
     * Time to wait for operation completion.
     *
     * @return Time in milliseconds.
     */
    protected long awaitTime() {
        return 10_000;
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

        /** */
        private AtomicInteger failNodeAdded = new AtomicInteger();

        /** */
        private AtomicInteger failNodeAddFinished = new AtomicInteger();

        /** */
        private AtomicInteger failClientReconnect = new AtomicInteger();

        /** */
        private IgniteInClosure2X<TcpDiscoveryAbstractMessage, Socket> afterWrite;

        /** */
        private volatile boolean invalidRes;

        /** */
        private volatile String delayJoinAckFor;

        /** */
        private volatile boolean skipNodeAdded;

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
         * @param afterWrite After write callback.
         */
        void afterWrite(IgniteInClosure2X<TcpDiscoveryAbstractMessage, Socket> afterWrite) {
            this.afterWrite = afterWrite;
        }

        /**
         * @return {@code True} if received unexpected ack.
         */
        boolean invalidResponse() {
            return invalidRes;
        }

        /**
         *
         */
        void failNodeAddedMessage() {
            failNodeAdded.set(1);
        }

        /**
         *
         */
        void failNodeAddFinishedMessage() {
            failNodeAddFinished.set(1);
        }

        /**
         *
         */
        void failClientReconnectMessage() {
            failClientReconnect.set(1);
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
            GridByteArrayOutputStream bout, long timeout) throws IOException, IgniteCheckedException {
            waitFor(writeLock);

            boolean fail = false;

            if (skipNodeAdded &&
                (msg instanceof TcpDiscoveryNodeAddedMessage || msg instanceof TcpDiscoveryNodeAddFinishedMessage)) {
                log.info("Skip message: " + msg);

                return;
            }

            if (msg instanceof TcpDiscoveryNodeAddedMessage)
                fail = failNodeAdded.getAndDecrement() > 0;
            else if (msg instanceof TcpDiscoveryNodeAddFinishedMessage)
                fail = failNodeAddFinished.getAndDecrement() > 0;
            else if (msg instanceof TcpDiscoveryClientReconnectMessage)
                fail = failClientReconnect.getAndDecrement() > 0;

            if (fail) {
                log.info("Close socket on message write [msg=" + msg + "]");

                sock.close();
            }

            super.writeToSocket(sock, msg, bout, timeout);

            if (afterWrite != null)
                afterWrite.apply(msg, sock);
        }

        /** {@inheritDoc} */
        @Override protected Socket openSocket(InetSocketAddress sockAddr,
            IgniteSpiOperationTimeoutHelper timeoutHelper) throws IOException, IgniteSpiOperationTimeoutException {
            waitFor(openSockLock);

            return super.openSocket(sockAddr, new IgniteSpiOperationTimeoutHelper(this));
        }

        /**
         *
         */
        public void pauseSocketWrite() {
            pauseResumeOperation(true, writeLock);
        }

        /**
         * @param suspend If {@code true} suspends worker threads.
         */
        public void pauseAll(boolean suspend) {
            pauseResumeOperation(true, openSockLock, writeLock);

            if (suspend)
                impl.workerThread().suspend();
        }

        /**
         *
         */
        public void resumeAll() {
            pauseResumeOperation(false, openSockLock, writeLock);

            impl.workerThread().resume();
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(TcpDiscoveryAbstractMessage msg, Socket sock, int res, long timeout)
            throws IOException {
            if (delayJoinAckFor != null && msg instanceof TcpDiscoveryJoinRequestMessage) {
                TcpDiscoveryJoinRequestMessage msg0 = (TcpDiscoveryJoinRequestMessage)msg;

                if (delayJoinAckFor.equals(msg0.node().attribute(IgniteNodeAttributes.ATTR_GRID_NAME))) {
                    log.info("Delay response [sock=" + sock + ", msg=" + msg0 + ", res=" + res + ']');

                    delayJoinAckFor = null;

                    try {
                        Thread.sleep(2000);
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }

            super.writeToSocket(msg, sock, res, timeout);
        }

        /** {@inheritDoc} */
        @Override protected int readReceipt(Socket sock, long timeout) throws IOException {
            int res = super.readReceipt(sock, timeout);

            if (res != TcpDiscoveryImpl.RES_OK) {
                invalidRes = true;

                log.info("Received unexpected response: " + res);
            }

            return res;
        }
    }
}