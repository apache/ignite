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

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.ignite.spi.discovery.tcp.ipfinder.zk.curator.TestingCluster;
import org.apache.ignite.spi.discovery.tcp.ipfinder.zk.curator.TestingZooKeeperServer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteState;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CommunicationFailureContext;
import org.apache.ignite.configuration.CommunicationFailureResolver;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.SecurityCredentialsAttrFilterPredicate;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.discovery.CustomEventListener;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.DiscoveryLocalJoinData;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpiInternalListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.TestCacheNodeExcludingFilter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.lang.IgniteInClosure2X;
import org.apache.ignite.internal.util.lang.gridfunc.PredicateMapView;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.DiscoverySpiNodeAuthenticator;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpi;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpiAbstractTestSuite;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpiMBean;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpiTestSuite2;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZkTestClientCnxnSocketNIO;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_RECONNECTED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_SECURITY_SUBJECT_V2;
import static org.apache.ignite.spi.discovery.zk.internal.ZookeeperDiscoveryImpl.IGNITE_ZOOKEEPER_DISCOVERY_SPI_ACK_THRESHOLD;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.zookeeper.ZooKeeper.ZOOKEEPER_CLIENT_CNXN_SOCKET;

/**
 *
 */
@SuppressWarnings("deprecation")
public class ZookeeperDiscoverySpiTest extends GridCommonAbstractTest {
    /** */
    private static final String IGNITE_ZK_ROOT = ZookeeperDiscoverySpi.DFLT_ROOT_PATH;

    /** */
    private static final int ZK_SRVS = 3;

    /** */
    private static TestingCluster zkCluster;

    /** To run test with real local ZK. */
    private static final boolean USE_TEST_CLUSTER = true;

    /** */
    private boolean client;

    /** */
    private static ThreadLocal<Boolean> clientThreadLoc = new ThreadLocal<>();

    /** */
    private static ConcurrentHashMap<UUID, Map<T2<Integer, Long>, DiscoveryEvent>> evts = new ConcurrentHashMap<>();

    /** */
    private static volatile boolean err;

    /** */
    private boolean testSockNio;

    /** */
    private CacheAtomicityMode atomicityMode;

    /** */
    private int backups = -1;

    /** */
    private boolean testCommSpi;

    /** */
    private boolean failCommSpi;

    /** */
    private long sesTimeout;

    /** */
    private long joinTimeout;

    /** */
    private boolean clientReconnectDisabled;

    /** */
    private ConcurrentHashMap<String, ZookeeperDiscoverySpi> spis = new ConcurrentHashMap<>();

    /** */
    private Map<String, Object> userAttrs;

    /** */
    private boolean dfltConsistenId;

    /** */
    private UUID nodeId;

    /** */
    private boolean persistence;

    /** */
    private IgniteOutClosure<CommunicationFailureResolver> commFailureRslvr;

    /** */
    private IgniteOutClosure<DiscoverySpiNodeAuthenticator> auth;

    /** */
    private String zkRootPath;

    /** The number of clusters started in one test (increments when the first node in the cluster starts). */
    private final AtomicInteger clusterNum = new AtomicInteger(0);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(final String igniteInstanceName) throws Exception {
        if (testSockNio)
            System.setProperty(ZOOKEEPER_CLIENT_CNXN_SOCKET, ZkTestClientCnxnSocketNIO.class.getName());

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (nodeId != null)
            cfg.setNodeId(nodeId);

        if (!dfltConsistenId)
            cfg.setConsistentId(igniteInstanceName);

        ZookeeperDiscoverySpi zkSpi = new ZookeeperDiscoverySpi();

        if (joinTimeout != 0)
            zkSpi.setJoinTimeout(joinTimeout);

        zkSpi.setSessionTimeout(sesTimeout > 0 ? sesTimeout : 10_000);

        zkSpi.setClientReconnectDisabled(clientReconnectDisabled);

        // Set authenticator for basic sanity tests.
        if (auth != null) {
            zkSpi.setAuthenticator(auth.apply());

            zkSpi.setInternalListener(new IgniteDiscoverySpiInternalListener() {
                @Override public void beforeJoin(ClusterNode locNode, IgniteLogger log) {
                    ZookeeperClusterNode locNode0 = (ZookeeperClusterNode)locNode;

                    Map<String, Object> attrs = new HashMap<>(locNode0.getAttributes());

                    attrs.put(ATTR_SECURITY_CREDENTIALS, new SecurityCredentials(null, null, igniteInstanceName));

                    locNode0.setAttributes(attrs);
                }

                @Override public boolean beforeSendCustomEvent(DiscoverySpi spi, IgniteLogger log, DiscoverySpiCustomMessage msg) {
                    return false;
                }
            });
        }

        spis.put(igniteInstanceName, zkSpi);

        if (USE_TEST_CLUSTER) {
            assert zkCluster != null;

            zkSpi.setZkConnectionString(zkCluster.getConnectString());

            if (zkRootPath != null)
                zkSpi.setZkRootPath(zkRootPath);
        }
        else
            zkSpi.setZkConnectionString("localhost:2181");

        cfg.setDiscoverySpi(zkSpi);

        cfg.setCacheConfiguration(getCacheConfiguration());

        Boolean clientMode = clientThreadLoc.get();

        if (clientMode != null)
            cfg.setClientMode(clientMode);
        else
            cfg.setClientMode(client);

        if (userAttrs != null)
            cfg.setUserAttributes(userAttrs);

        Map<IgnitePredicate<? extends Event>, int[]> lsnrs = new HashMap<>();

        if (cfg.isClientMode()) {
            UUID currNodeId = cfg.getNodeId();

            lsnrs.put(new IgnitePredicate<Event>() {
                /** Last remembered uuid before node reconnected. */
                private UUID nodeId = currNodeId;

                @Override public boolean apply(Event evt) {
                    if(evt.type() == EVT_CLIENT_NODE_RECONNECTED){
                        evts.remove(nodeId);

                        nodeId = evt.node().id();
                    }

                    return false;
                }
            }, new int[] {EVT_CLIENT_NODE_RECONNECTED});
        }

        lsnrs.put(new IgnitePredicate<Event>() {
            /** */
            @IgniteInstanceResource
            private Ignite ignite;

            @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
            @Override public boolean apply(Event evt) {
                try {
                    DiscoveryEvent discoveryEvt = (DiscoveryEvent)evt;

                    UUID locId = ((IgniteKernal)ignite).context().localNodeId();

                    Map<T2<Integer, Long>, DiscoveryEvent> nodeEvts = evts.get(locId);

                    if (nodeEvts == null) {
                        Object old = evts.put(locId, nodeEvts = new LinkedHashMap<>());

                        assertNull(old);

                        // If the current node has failed, the local join will never happened.
                        if (evt.type() != EVT_NODE_FAILED ||
                            discoveryEvt.eventNode().consistentId().equals(ignite.configuration().getConsistentId())) {
                            synchronized (nodeEvts) {
                                DiscoveryLocalJoinData locJoin = ((IgniteEx)ignite).context().discovery().localJoin();

                                if (locJoin.event().node().order() == 1)
                                    clusterNum.incrementAndGet();

                                nodeEvts.put(new T2<>(clusterNum.get(), locJoin.event().topologyVersion()),
                                    locJoin.event());
                            }
                        }
                    }

                    synchronized (nodeEvts) {
                        DiscoveryEvent old = nodeEvts.put(new T2<>(clusterNum.get(), discoveryEvt.topologyVersion()),
                            discoveryEvt);

                        assertNull(old);
                    }
                }
                catch (Throwable e) {
                    error("Unexpected error [evt=" + evt + ", err=" + e + ']', e);

                    err = true;
                }

                return true;
            }
        }, new int[]{EVT_NODE_JOINED, EVT_NODE_FAILED, EVT_NODE_LEFT});

        cfg.setLocalEventListeners(lsnrs);

        if (persistence) {
            DataStorageConfiguration memCfg = new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setMaxSize(100 * 1024 * 1024).
                    setPersistenceEnabled(true))
                .setPageSize(1024)
                .setWalMode(WALMode.LOG_ONLY);

            cfg.setDataStorageConfiguration(memCfg);
        }

        if (testCommSpi)
            cfg.setCommunicationSpi(new ZkTestCommunicationSpi());

        if (failCommSpi)
            cfg.setCommunicationSpi(new PeerToPeerCommunicationFailureSpi());

        if (commFailureRslvr != null)
            cfg.setCommunicationFailureResolver(commFailureRslvr.apply());

        return cfg;
    }

    /** */
    private CacheConfiguration getCacheConfiguration() {
        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        if (atomicityMode != null)
            ccfg.setAtomicityMode(atomicityMode);

        if (backups > 0)
            ccfg.setBackups(backups);

        return ccfg;
    }

    /**
     * @param clientMode Client mode flag for started nodes.
     */
    private void clientMode(boolean clientMode) {
        client = clientMode;
    }

    /**
     * @param clientMode Client mode flag for nodes started from current thread.
     */
    private void clientModeThreadLocal(boolean clientMode) {
        clientThreadLoc.set(clientMode);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        System.setProperty(ZookeeperDiscoveryImpl.IGNITE_ZOOKEEPER_DISCOVERY_SPI_ACK_TIMEOUT, "1000");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopZkCluster();

        System.clearProperty(ZookeeperDiscoveryImpl.IGNITE_ZOOKEEPER_DISCOVERY_SPI_ACK_TIMEOUT);
    }

    /**
     *
     */
    private void stopZkCluster() {
        if (zkCluster != null) {
            try {
                zkCluster.close();
            }
            catch (Exception e) {
                U.error(log, "Failed to stop Zookeeper client: " + e, e);
            }

            zkCluster = null;
        }
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

        if (USE_TEST_CLUSTER && zkCluster == null) {
            zkCluster = ZookeeperDiscoverySpiAbstractTestSuite.createTestingCluster(ZK_SRVS);

            zkCluster.start();

            waitForZkClusterReady(zkCluster);
        }

        reset();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        clearAckEveryEventSystemProperty();

        try {
            assertFalse("Unexpected error, see log for details", err);

            checkEventsConsistency();

            checkInternalStructuresCleanup();

            //TODO uncomment when https://issues.apache.org/jira/browse/IGNITE-8193 is fixed
//            checkZkNodesCleanup();
        }
        finally {
            stopAllGrids();

            reset();
        }
    }


    /**
     * Wait for Zookeeper testing cluster ready for communications.
     *
     * @param zkCluster Zk cluster.
     */
    private static void waitForZkClusterReady(TestingCluster zkCluster) throws InterruptedException {
        try (CuratorFramework curator = CuratorFrameworkFactory
            .newClient(zkCluster.getConnectString(), new RetryNTimes(10, 1_000))) {
            curator.start();

            assertTrue("Failed to wait for Zookeeper testing cluster ready.",
                curator.blockUntilConnected(30, SECONDS));
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void checkInternalStructuresCleanup() throws Exception {
        for (Ignite node : IgnitionEx.allGridsx()) {
            final AtomicReference<?> res = GridTestUtils.getFieldValue(spi(node), "impl", "commErrProcFut");

            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return res.get() == null;
                }
            }, 70_000);

            assertNull(res.get());
        }
    }

    /**
     * @param nodeIdx Node index.
     */
    private void checkStartFail(final int nodeIdx) {
        Throwable err = GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(nodeIdx);

                return null;
            }
        }, IgniteCheckedException.class, null);

        IgniteSpiException spiErr = X.cause(err, IgniteSpiException.class);

        assertNotNull(spiErr);
        assertTrue(spiErr.getMessage().contains("Authentication failed"));
    }

    /**
     * @param expNodes Expected nodes number.
     * @throws Exception If failed.
     */
    private void checkTestSecuritySubject(int expNodes) throws Exception {
        waitForTopology(expNodes);

        List<Ignite> nodes = G.allGrids();

        JdkMarshaller marsh = new JdkMarshaller();

        for (Ignite ignite : nodes) {
            Collection<ClusterNode> nodes0 = ignite.cluster().nodes();

            assertEquals(nodes.size(), nodes0.size());

            for (ClusterNode node : nodes0) {
                byte[] secSubj = node.attribute(ATTR_SECURITY_SUBJECT_V2);

                assertNotNull(secSubj);

                ZkTestNodeAuthenticator.TestSecurityContext secCtx = marsh.unmarshal(secSubj, null);

                assertEquals(node.attribute(ATTR_IGNITE_INSTANCE_NAME), secCtx.nodeName);
            }
        }
    }

    /**
     * @param srvs Servers number.
     * @param clients Clients number.
     * @throws Exception If failed.
     */
    private void customEvents_FastStopProcess(int srvs, int clients) throws Exception {
        ackEveryEventSystemProperty();

        Map<UUID, List<T3<AffinityTopologyVersion, UUID, DiscoveryCustomMessage>>> rcvdMsgs =
            new ConcurrentHashMap<>();

        Ignite crd = startGrid(0);

        UUID crdId = crd.cluster().localNode().id();

        if (srvs > 1)
            startGridsMultiThreaded(1, srvs - 1);

        if (clients > 0) {
            client = true;

            startGridsMultiThreaded(srvs, clients);
        }

        awaitPartitionMapExchange();

        List<Ignite> nodes = G.allGrids();

        assertEquals(srvs + clients, nodes.size());

        for (Ignite node : nodes)
            registerTestEventListeners(node, rcvdMsgs);

        int payload = 0;

        AffinityTopologyVersion topVer = ((IgniteKernal)crd).context().discovery().topologyVersionEx();

        for (Ignite node : nodes) {
            UUID sndId = node.cluster().localNode().id();

            info("Send from node: " + sndId);

            GridDiscoveryManager discoveryMgr = ((IgniteKernal)node).context().discovery();

            {
                List<T3<AffinityTopologyVersion, UUID, DiscoveryCustomMessage>> expCrdMsgs = new ArrayList<>();
                List<T3<AffinityTopologyVersion, UUID, DiscoveryCustomMessage>> expNodesMsgs = Collections.emptyList();

                TestFastStopProcessCustomMessage msg = new TestFastStopProcessCustomMessage(false, payload++);

                expCrdMsgs.add(new T3<AffinityTopologyVersion, UUID, DiscoveryCustomMessage>(topVer, sndId, msg));

                discoveryMgr.sendCustomEvent(msg);

                doSleep(200); // Wait some time to check extra messages are not received.

                checkEvents(crd, rcvdMsgs, expCrdMsgs);

                for (Ignite node0 : nodes) {
                    if (node0 != crd)
                        checkEvents(node0, rcvdMsgs, expNodesMsgs);
                }

                rcvdMsgs.clear();
            }
            {
                List<T3<AffinityTopologyVersion, UUID, DiscoveryCustomMessage>> expCrdMsgs = new ArrayList<>();
                List<T3<AffinityTopologyVersion, UUID, DiscoveryCustomMessage>> expNodesMsgs = new ArrayList<>();

                TestFastStopProcessCustomMessage msg = new TestFastStopProcessCustomMessage(true, payload++);

                expCrdMsgs.add(new T3<AffinityTopologyVersion, UUID, DiscoveryCustomMessage>(topVer, sndId, msg));

                discoveryMgr.sendCustomEvent(msg);

                TestFastStopProcessCustomMessageAck ackMsg = new TestFastStopProcessCustomMessageAck(msg.payload);

                expCrdMsgs.add(new T3<AffinityTopologyVersion, UUID, DiscoveryCustomMessage>(topVer, crdId, ackMsg));
                expNodesMsgs.add(new T3<AffinityTopologyVersion, UUID, DiscoveryCustomMessage>(topVer, crdId, ackMsg));

                doSleep(200); // Wait some time to check extra messages are not received.

                checkEvents(crd, rcvdMsgs, expCrdMsgs);

                for (Ignite node0 : nodes) {
                    if (node0 != crd)
                        checkEvents(node0, rcvdMsgs, expNodesMsgs);
                }

                rcvdMsgs.clear();
            }

            waitForEventsAcks(crd);
        }
    }

    /**
     * @param node Node to check.
     * @param rcvdMsgs Received messages.
     * @param expMsgs Expected messages.
     * @throws Exception If failed.
     */
    private void checkEvents(
        Ignite node,
        final Map<UUID, List<T3<AffinityTopologyVersion, UUID, DiscoveryCustomMessage>>> rcvdMsgs,
        final List<T3<AffinityTopologyVersion, UUID, DiscoveryCustomMessage>> expMsgs) throws Exception {
        final UUID nodeId = node.cluster().localNode().id();

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                List<T3<AffinityTopologyVersion, UUID, DiscoveryCustomMessage>> msgs = rcvdMsgs.get(nodeId);

                int size = msgs == null ? 0 : msgs.size();

                return size >= expMsgs.size();
            }
        }, 5000));

        List<T3<AffinityTopologyVersion, UUID, DiscoveryCustomMessage>> msgs = rcvdMsgs.get(nodeId);

        if (msgs == null)
            msgs = Collections.emptyList();

        assertEqualsCollections(expMsgs, msgs);
    }

    /**
     * @param node Node.
     * @param rcvdMsgs Map to store received events.
     */
    private void registerTestEventListeners(Ignite node,
        final Map<UUID, List<T3<AffinityTopologyVersion, UUID, DiscoveryCustomMessage>>> rcvdMsgs) {
        GridDiscoveryManager discoveryMgr = ((IgniteKernal)node).context().discovery();

        final UUID nodeId = node.cluster().localNode().id();

        discoveryMgr.setCustomEventListener(TestFastStopProcessCustomMessage.class,
            new CustomEventListener<TestFastStopProcessCustomMessage>() {
                @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd, TestFastStopProcessCustomMessage msg) {
                    List<T3<AffinityTopologyVersion, UUID, DiscoveryCustomMessage>> list = rcvdMsgs.get(nodeId);

                    if (list == null)
                        rcvdMsgs.put(nodeId, list = new ArrayList<>());

                    list.add(new T3<>(topVer, snd.id(), (DiscoveryCustomMessage)msg));
                }
            }
        );
        discoveryMgr.setCustomEventListener(TestFastStopProcessCustomMessageAck.class,
            new CustomEventListener<TestFastStopProcessCustomMessageAck>() {
                @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd, TestFastStopProcessCustomMessageAck msg) {
                    List<T3<AffinityTopologyVersion, UUID, DiscoveryCustomMessage>> list = rcvdMsgs.get(nodeId);

                    if (list == null)
                        rcvdMsgs.put(nodeId, list = new ArrayList<>());

                    list.add(new T3<>(topVer, snd.id(), (DiscoveryCustomMessage)msg));
                }
            }
        );
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
            if (IgnitionEx.state(name).equals(IgniteState.STARTED))
                Thread.sleep(2000);
            else
                break;
        }
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
            final ZookeeperDiscoverySpi spi = waitSpi(getTestIgniteInstanceName(i));

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

                failedZkNodes.add(aliveZkNodePath(spi));
            }
            else {
                expEvts[expEvtCnt] = joinEvent(initNodes + expEvtCnt + 1);

                expEvtCnt++;
            }
        }

        waitNoAliveZkNodes(log, zkCluster.getConnectString(), failedZkNodes, 30_000);

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
     * @param node Node.
     * @return Corresponding znode.
     */
    private static String aliveZkNodePath(Ignite node) {
        return aliveZkNodePath(node.configuration().getDiscoverySpi());
    }

    /**
     * @param spi SPI.
     * @return Znode related to given SPI.
     */
    private static String aliveZkNodePath(DiscoverySpi spi) {
        String path = GridTestUtils.getFieldValue(spi, "impl", "rtState", "locNodeZkPath");

        return path.substring(path.lastIndexOf('/') + 1);
    }

    /**
     * @param log Logger.
     * @param connectString Zookeeper connect string.
     * @param failedZkNodes Znodes which should be removed.
     * @param timeout Timeout.
     * @throws Exception If failed.
     */
    private static void waitNoAliveZkNodes(final IgniteLogger log,
        String connectString,
        final List<String> failedZkNodes,
        long timeout)
        throws Exception
    {
        final ZookeeperClient zkClient = new ZookeeperClient(log, connectString, 10_000, null);

        try {
            assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    try {
                        List<String> c = zkClient.getChildren(IGNITE_ZK_ROOT + "/" + ZkIgnitePaths.ALIVE_NODES_DIR);

                        for (String failedZkNode : failedZkNodes) {
                            if (c.contains(failedZkNode)) {
                                log.info("Alive node is not removed [node=" + failedZkNode + ", all=" + c + ']');

                                return false;
                            }
                        }

                        return true;
                    }
                    catch (Exception e) {
                        e.printStackTrace();

                        fail();

                        return true;
                    }
                }
            }, timeout));
        }
        finally {
            zkClient.close();
        }
    }

    /**
     * @param initNodes Number of initially started nnodes.
     * @throws Exception If failed.
     */
    private void concurrentStartStop(final int initNodes) throws Exception {
        startGrids(initNodes);

        final int NODES = 5;

        long topVer = initNodes;

        for (int i = 0; i < 10; i++) {
            info("Iteration: " + i);

            DiscoveryEvent[] expEvts = new DiscoveryEvent[NODES];

            startGridsMultiThreaded(initNodes, NODES);

            for (int j = 0; j < NODES; j++)
                expEvts[j] = joinEvent(++topVer);

            checkEvents(ignite(0), expEvts);

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
                expEvts[j] = failEvent(++topVer);

            checkEventsConsistency();
        }
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
    public void testTopologyChangeMultithreaded_RestartZk() throws Exception { //~~!
        try {
            topologyChangeWithRestarts(true, false);
        }
        finally {
            zkCluster.close();

            zkCluster = null;
        }
    }

    /**
     * @param restartZk If {@code true} in background restarts on of ZK servers.
     * @param closeClientSock If {@code true} in background closes zk clients' sockets.
     * @throws Exception If failed.
     */
    private void topologyChangeWithRestarts(boolean restartZk, boolean closeClientSock) throws Exception {
        sesTimeout = 30_000;

        if (closeClientSock)
            testSockNio = true;

        long stopTime = System.currentTimeMillis() + 60_000;

        AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<?> fut1 = null;

        IgniteInternalFuture<?> fut2 = null;

        try {
            fut1 = restartZk ? startRestartZkServers(stopTime, stop) : null;
            fut2 = closeClientSock ? startCloseZkClientSocket(stopTime, stop) : null;

            int INIT_NODES = 10;

            startGridsMultiThreaded(INIT_NODES);

            final int MAX_NODES = 20;

            final List<Integer> startedNodes = new ArrayList<>();

            for (int i = 0; i < INIT_NODES; i++)
                startedNodes.add(i);

            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            final AtomicInteger startIdx = new AtomicInteger(INIT_NODES);

            while (System.currentTimeMillis() < stopTime) {
                if (startedNodes.size() >= MAX_NODES) {
                    int stopNodes = rnd.nextInt(5) + 1;

                    log.info("Next, stop nodes: " + stopNodes);

                    final List<Integer> idxs = new ArrayList<>();

                    while (idxs.size() < stopNodes) {
                        Integer stopIdx = rnd.nextInt(startedNodes.size());

                        if (!idxs.contains(stopIdx))
                            idxs.add(startedNodes.get(stopIdx));
                    }

                    GridTestUtils.runMultiThreaded(new IgniteInClosure<Integer>() {
                        @Override public void apply(Integer threadIdx) {
                            int stopNodeIdx = idxs.get(threadIdx);

                            info("Stop node: " + stopNodeIdx);

                            stopGrid(stopNodeIdx);
                        }
                    }, stopNodes, "stop-node");

                    startedNodes.removeAll(idxs);
                }
                else {
                    int startNodes = rnd.nextInt(5) + 1;

                    log.info("Next, start nodes: " + startNodes);

                    GridTestUtils.runMultiThreaded(new Callable<Void>() {
                        @Override public Void call() throws Exception {
                            int idx = startIdx.incrementAndGet();

                            log.info("Start node: " + idx);

                            startGrid(idx);

                            synchronized (startedNodes) {
                                startedNodes.add(idx);
                            }

                            return null;
                        }
                    }, startNodes, "start-node");
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
     * @throws Exception If failed.
     */
    private void checkZkNodesCleanup() throws Exception {
        final ZookeeperClient zkClient = new ZookeeperClient(getTestResources().getLogger(),
            zkCluster.getConnectString(),
            30_000,
            null);

        final String basePath = IGNITE_ZK_ROOT + "/";

        final String aliveDir = basePath + ZkIgnitePaths.ALIVE_NODES_DIR + "/";

        try {
            List<String> znodes = listSubTree(zkClient.zk(), IGNITE_ZK_ROOT);

            boolean foundAlive = false;

            for (String znode : znodes) {
                if (znode.startsWith(aliveDir)) {
                    foundAlive = true;

                    break;
                }
            }

            assertTrue(foundAlive); // Sanity check to make sure we check correct directory.

            assertTrue("Failed to wait for unused znodes cleanup", GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    try {
                        List<String> znodes = listSubTree(zkClient.zk(), IGNITE_ZK_ROOT);

                        for (String znode : znodes) {
                            if (znode.startsWith(aliveDir) || znode.length() < basePath.length())
                                continue;

                            znode = znode.substring(basePath.length());

                            if (!znode.contains("/")) // Ignore roots.
                                continue;

                            // TODO ZK: https://issues.apache.org/jira/browse/IGNITE-8193
                            if (znode.startsWith("jd/"))
                                continue;

                            log.info("Found unexpected znode: " + znode);

                            return false;
                        }

                        return true;
                    }
                    catch (Exception e) {
                        error("Unexpected error: " + e, e);

                        fail("Unexpected error: " + e);
                    }

                    return false;
                }
            }, 10_000));
        }
        finally {
            zkClient.close();
        }
    }

    /**
     *
     */
    private void initLargeAttribute() {
        userAttrs = new HashMap<>();

        int[] attr = new int[1024 * 1024 + ThreadLocalRandom.current().nextInt(1024 * 512)];

        for (int i = 0; i < attr.length; i++)
            attr[i] = i;

        userAttrs.put("testAttr", attr);
    }

    /**
     * @param closeSock Test mode flag.
     * @throws Exception If failed.
     */
    private void clientReconnectSessionExpire(boolean closeSock) throws Exception {
        startGrid(0);

        sesTimeout = 2000;
        clientMode(true);
        testSockNio = true;

        Ignite client = startGrid(1);

        client.cache(DEFAULT_CACHE_NAME).put(1, 1);

        reconnectClientNodes(log, Collections.singletonList(client), closeSock);

        assertEquals(1, client.cache(DEFAULT_CACHE_NAME).get(1));

        client.compute().broadcast(new DummyCallable(null));
    }

    /**
     * @param nodes Nodes number.
     * @throws Exception If failed.
     */
    private void communicationFailureResolve_Simple(int nodes) throws Exception {
        assert nodes > 1;

        sesTimeout = 2000;
        commFailureRslvr = NoOpCommunicationFailureResolver.FACTORY;

        startGridsMultiThreaded(nodes);

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int i = 0; i < 3; i++) {
            info("Iteration: " + i);

            int idx1 = rnd.nextInt(nodes);

            int idx2;

            do {
                idx2 = rnd.nextInt(nodes);
            }
            while (idx1 == idx2);

            ZookeeperDiscoverySpi spi = spi(ignite(idx1));

            spi.resolveCommunicationFailure(ignite(idx2).cluster().localNode(), new Exception("test"));

            checkInternalStructuresCleanup();
        }
    }

    /**
     * @param startNodes Number of nodes to start.
     * @param killNodes Nodes to kill by resolve process.
     * @throws Exception If failed.
     */
    private void communicationFailureResolve_KillNodes(int startNodes, Collection<Long> killNodes) throws Exception {
        testCommSpi = true;

        commFailureRslvr = TestNodeKillCommunicationFailureResolver.factory(killNodes);

        startGrids(startNodes);

        ZkTestCommunicationSpi commSpi = ZkTestCommunicationSpi.testSpi(ignite(0));

        commSpi.checkRes = new BitSet(startNodes);

        ZookeeperDiscoverySpi spi = null;
        UUID killNodeId = null;

        for (Ignite node : G.allGrids()) {
            ZookeeperDiscoverySpi spi0 = spi(node);

            if (!killNodes.contains(node.cluster().localNode().order()))
                spi = spi0;
            else
                killNodeId = node.cluster().localNode().id();
        }

        assertNotNull(spi);
        assertNotNull(killNodeId);

        try {
            spi.resolveCommunicationFailure(spi.getNode(killNodeId), new Exception("test"));

            fail("Exception is not thrown");
        }
        catch (IgniteSpiException e) {
            assertTrue("Unexpected exception: " + e, e.getCause() instanceof ClusterTopologyCheckedException);
        }

        int expNodes = startNodes - killNodes.size();

        waitForTopology(expNodes);

        for (Ignite node : G.allGrids())
            assertFalse(killNodes.contains(node.cluster().localNode().order()));

        startGrid(startNodes);

        waitForTopology(expNodes + 1);
    }


    /**
     * @param startNodes Initial nodes number.
     * @param breakNodes Node indices where communication server is closed.
     * @throws Exception If failed.
     */
    private void defaultCommunicationFailureResolver_BreakCommunication(int startNodes, final int...breakNodes) throws Exception {
        sesTimeout = 5000;

        startGridsMultiThreaded(startNodes);

        final CyclicBarrier b = new CyclicBarrier(breakNodes.length);

        GridTestUtils.runMultiThreaded(new IgniteInClosure<Integer>() {
            @Override public void apply(Integer threadIdx) {
                try {
                    b.await();

                    int nodeIdx = breakNodes[threadIdx];

                    info("Close communication: " + nodeIdx);

                    ((TcpCommunicationSpi)ignite(nodeIdx).configuration().getCommunicationSpi()).simulateNodeFailure();
                }
                catch (Exception e) {
                    fail("Unexpected error: " + e);
                }
            }
        }, breakNodes.length, "break-communication");

        waitForTopology(startNodes - breakNodes.length);
    }

    /**
     * @param crd Coordinator node.
     * @param expCaches Expected caches info.
     * @throws Exception If failed.
     */
    private void checkResolverCachesInfo(Ignite crd, Map<String, T3<Integer, Integer, Integer>> expCaches)
        throws Exception
    {
        CacheInfoCommunicationFailureResolver rslvr =
            (CacheInfoCommunicationFailureResolver)crd.configuration().getCommunicationFailureResolver();

        assertNotNull(rslvr);

        ZookeeperDiscoverySpi spi = spi(crd);

        rslvr.latch = new CountDownLatch(1);

        ZkTestCommunicationSpi.testSpi(crd).initCheckResult(crd.cluster().nodes().size(), 0);

        spi.resolveCommunicationFailure(spi.getRemoteNodes().iterator().next(), new Exception("test"));

        assertTrue(rslvr.latch.await(10, SECONDS));

        rslvr.checkCachesInfo(expCaches);

        rslvr.reset();
    }

    /**
     * @param joinTimeout Join timeout.
     * @throws Exception If failed.
     */
    private void startNoServer_WaitForServers(long joinTimeout) throws Exception {
        this.joinTimeout = joinTimeout;

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                clientModeThreadLocal(true);

                startGrid(0);

                return null;
            }
        });

        U.sleep(3000);

        waitSpi(getTestIgniteInstanceName(0));

        clientModeThreadLocal(false);

        startGrid(1);

        fut.get();

        waitForTopology(2);
    }

    /**
     * @param srvs Number of servers.
     * @param clients Number of clients.
     * @throws Exception If failed.
     */
    private void disconnectOnServersLeft(int srvs, int clients) throws Exception {
        startGridsMultiThreaded(srvs);

        clientMode(true);

        startGridsMultiThreaded(srvs, clients);

        for (int i = 0; i < 5; i++) {
            info("Iteration: " + i);

            final CountDownLatch disconnectLatch = new CountDownLatch(clients);
            final CountDownLatch reconnectLatch = new CountDownLatch(clients);

            IgnitePredicate<Event> p = new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    if (evt.type() == EVT_CLIENT_NODE_DISCONNECTED) {
                        log.info("Disconnected: " + evt);

                        disconnectLatch.countDown();
                    }
                    else if (evt.type() == EVT_CLIENT_NODE_RECONNECTED) {
                        log.info("Reconnected: " + evt);

                        reconnectLatch.countDown();

                        return false;
                    }

                    return true;
                }
            };

            for (int c = 0; c < clients; c++) {
                Ignite client = ignite(srvs + c);

                assertTrue(client.configuration().isClientMode());

                client.events().localListen(p, EVT_CLIENT_NODE_DISCONNECTED, EVT_CLIENT_NODE_RECONNECTED);
            }

            log.info("Stop all servers.");

            GridTestUtils.runMultiThreaded(new IgniteInClosure<Integer>() {
                @Override public void apply(Integer threadIdx) {
                    stopGrid(getTestIgniteInstanceName(threadIdx), true, false);
                }
            }, srvs, "stop-server");

            waitReconnectEvent(log, disconnectLatch);

            evts.clear();

            clientMode(false);

            log.info("Restart servers.");

            startGridsMultiThreaded(0, srvs);

            waitReconnectEvent(log, reconnectLatch);

            waitForTopology(srvs + clients);

            log.info("Reconnect finished.");
        }
    }

    /**
     * Class represents available connections between cluster nodes.
     * This is needed to simulate network problems in {@link PeerToPeerCommunicationFailureSpi}.
     */
    static class ConnectionsFailureMatrix {
        /** Available connections per each node id. */
        private Map<UUID, Set<UUID>> availableConnections = new HashMap<>();

        /**
         * @param from Cluster node 1.
         * @param to Cluster node 2.
         * @return {@code True} if there is connection between nodes {@code from} and {@code to}.
         */
        public boolean hasConnection(ClusterNode from, ClusterNode to) {
            return availableConnections.getOrDefault(from.id(), Collections.emptySet()).contains(to.id());
        }

        /**
         * Adds connection between nodes {@code from} and {@code to}.
         * @param from Cluster node 1.
         * @param to Cluster node 2.
         */
        public void addConnection(ClusterNode from, ClusterNode to) {
            availableConnections.computeIfAbsent(from.id(), s -> new HashSet<>()).add(to.id());
        }

        /**
         * Removes connection between nodes {@code from} and {@code to}.
         * @param from Cluster node 1.
         * @param to Cluster node 2.
         */
        public void removeConnection(ClusterNode from, ClusterNode to) {
            availableConnections.getOrDefault(from.id(), Collections.emptySet()).remove(to.id());
        }

        /**
         * Adds connections between all nodes presented in given {@code nodeSet}.
         *
         * @param nodeSet Set of the cluster nodes.
         */
        public void addAll(List<ClusterNode> nodeSet) {
            for (int i = 0; i < nodeSet.size(); i++) {
                for (int j = 0; j < nodeSet.size(); j++) {
                    if (i == j)
                        continue;

                    addConnection(nodeSet.get(i), nodeSet.get(j));
                }
            }
        }

        /**
         * Builds connections failure matrix from two part of the cluster nodes.
         * Each part has all connections inside, but hasn't any connection to another part.
         *
         * @param part1 Part 1.
         * @param part2 Part 2.
         * @return Connections failure matrix.
         */
        static ConnectionsFailureMatrix buildFrom(List<ClusterNode> part1, List<ClusterNode> part2) {
            ConnectionsFailureMatrix matrix = new ConnectionsFailureMatrix();
            matrix.addAll(part1);
            matrix.addAll(part2);
            return matrix;
        }
    }

    /**
     * Communication SPI with possibility to simulate network problems between some of the cluster nodes.
     */
    static class PeerToPeerCommunicationFailureSpi extends TcpCommunicationSpi {
        /** Flag indicates that connections according to {@code matrix} should be failed. */
        private static volatile boolean failure;

        /** Connections failure matrix. */
        private static volatile ConnectionsFailureMatrix matrix;

        /**
         * Start failing connections according to given matrix {@code with}.
         * @param with Failure matrix.
         */
        static void fail(ConnectionsFailureMatrix with) {
            matrix = with;
            failure = true;
        }

        /**
         * Resets failure matrix.
         */
        static void unfail() {
            failure = false;
        }

        /** {@inheritDoc} */
        @Override public IgniteFuture<BitSet> checkConnection(List<ClusterNode> nodes) {
            // Creates connections statuses according to failure matrix.
            BitSet bitSet = new BitSet();

            ClusterNode localNode = getLocalNode();

            int idx = 0;

            for (ClusterNode remoteNode : nodes) {
                if (localNode.id().equals(remoteNode.id()))
                    bitSet.set(idx);
                else {
                    if (matrix.hasConnection(localNode, remoteNode))
                        bitSet.set(idx);
                }
                idx++;
            }

            return new IgniteFinishedFutureImpl<>(bitSet);
        }

        /** {@inheritDoc} */
        @Override protected GridCommunicationClient createTcpClient(
            ClusterNode node,
            int connIdx
        ) throws IgniteCheckedException {
            if (failure && !matrix.hasConnection(getLocalNode(), node)) {
                processClientCreationError(node, null, new IgniteCheckedException("Test", new SocketTimeoutException()));

                return null;
            }

            return new FailingCommunicationClient(getLocalNode(), node,
                super.createTcpClient(node, connIdx));
        }

        /**
         * Communication client with possibility to simulate network error between peers.
         */
        class FailingCommunicationClient implements GridCommunicationClient {
            /** Delegate. */
            private final GridCommunicationClient delegate;

            /** Local node which sends messages. */
            private final ClusterNode localNode;

            /** Remote node which receives messages. */
            private final ClusterNode remoteNode;

            FailingCommunicationClient(ClusterNode localNode, ClusterNode remoteNode, GridCommunicationClient delegate) {
                this.delegate = delegate;
                this.localNode = localNode;
                this.remoteNode = remoteNode;
            }

            /** {@inheritDoc} */
            @Override public void doHandshake(IgniteInClosure2X<InputStream, OutputStream> handshakeC) throws IgniteCheckedException {
                if (failure && !matrix.hasConnection(localNode, remoteNode))
                    throw new IgniteCheckedException("Test", new SocketTimeoutException());

                delegate.doHandshake(handshakeC);
            }

            /** {@inheritDoc} */
            @Override public boolean close() {
                return delegate.close();
            }

            /** {@inheritDoc} */
            @Override public void forceClose() {
                delegate.forceClose();
            }

            /** {@inheritDoc} */
            @Override public boolean closed() {
                return delegate.closed();
            }

            /** {@inheritDoc} */
            @Override public boolean reserve() {
                return delegate.reserve();
            }

            /** {@inheritDoc} */
            @Override public void release() {
                delegate.release();
            }

            /** {@inheritDoc} */
            @Override public long getIdleTime() {
                return delegate.getIdleTime();
            }

            /** {@inheritDoc} */
            @Override public void sendMessage(ByteBuffer data) throws IgniteCheckedException {
                if (failure && !matrix.hasConnection(localNode, remoteNode))
                    throw new IgniteCheckedException("Test", new SocketTimeoutException());

                delegate.sendMessage(data);
            }

            /** {@inheritDoc} */
            @Override public void sendMessage(byte[] data, int len) throws IgniteCheckedException {
                if (failure && !matrix.hasConnection(localNode, remoteNode))
                    throw new IgniteCheckedException("Test", new SocketTimeoutException());

                delegate.sendMessage(data, len);
            }

            /** {@inheritDoc} */
            @Override public boolean sendMessage(@Nullable UUID nodeId, Message msg, @Nullable IgniteInClosure<IgniteException> c) throws IgniteCheckedException {
                // This will enforce SPI to create new client.
                if (failure && !matrix.hasConnection(localNode, remoteNode))
                    return true;

                return delegate.sendMessage(nodeId, msg, c);
            }

            /** {@inheritDoc} */
            @Override public boolean async() {
                return delegate.async();
            }

            /** {@inheritDoc} */
            @Override public int connectionIndex() {
                return delegate.connectionIndex();
            }
        }
    }

    /**
     * @param srvs Number of server nodes in test.
     * @throws Exception If failed.
     */
    private void reconnectServersRestart(int srvs) throws Exception {
        startGridsMultiThreaded(srvs);

        clientMode(true);

        final int CLIENTS = 10;

        startGridsMultiThreaded(srvs, CLIENTS);

        clientMode(false);

        long stopTime = System.currentTimeMillis() + 30_000;

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        final int NODES = srvs + CLIENTS;

        int iter = 0;

        while (System.currentTimeMillis() < stopTime) {
            int restarts = rnd.nextInt(10) + 1;

            info("Test iteration [iter=" + iter++ + ", restarts=" + restarts + ']');

            for (int i = 0; i < restarts; i++) {
                GridTestUtils.runMultiThreaded(new IgniteInClosure<Integer>() {
                    @Override public void apply(Integer threadIdx) {
                        stopGrid(getTestIgniteInstanceName(threadIdx), true, false);
                    }
                }, srvs, "stop-server");

                startGridsMultiThreaded(0, srvs);
            }

            final Ignite srv = ignite(0);

            assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return srv.cluster().nodes().size() == NODES;
                }
            }, 30_000));

            waitForTopology(NODES);

            awaitPartitionMapExchange();
        }

        evts.clear();
    }

    /**
     * @param dfltConsistenId Default consistent ID flag.
     * @throws Exception If failed.
     */
    private void startWithPersistence(boolean dfltConsistenId) throws Exception {
        this.dfltConsistenId = dfltConsistenId;

        persistence = true;

        for (int i = 0; i < 3; i++) {
            info("Iteration: " + i);

            clientMode(false);

            startGridsMultiThreaded(4, i == 0);

            clientMode(true);

            startGridsMultiThreaded(4, 3);

            waitForTopology(7);

            stopGrid(1);

            waitForTopology(6);

            stopGrid(4);

            waitForTopology(5);

            stopGrid(0);

            waitForTopology(4);

            checkEventsConsistency();

            stopAllGrids();

            evts.clear();
        }
    }

    /**
     * @param clients Clients.
     * @param c Closure to run.
     * @throws Exception If failed.
     */
    private void reconnectClientNodes(List<Ignite> clients, Callable<Void> c)
        throws Exception {
        final CountDownLatch disconnectLatch = new CountDownLatch(clients.size());
        final CountDownLatch reconnectLatch = new CountDownLatch(clients.size());

        IgnitePredicate<Event> p = new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                if (evt.type() == EVT_CLIENT_NODE_DISCONNECTED) {
                    log.info("Disconnected: " + evt);

                    disconnectLatch.countDown();
                }
                else if (evt.type() == EVT_CLIENT_NODE_RECONNECTED) {
                    log.info("Reconnected: " + evt);

                    reconnectLatch.countDown();
                }

                return true;
            }
        };

        for (Ignite client : clients)
            client.events().localListen(p, EVT_CLIENT_NODE_DISCONNECTED, EVT_CLIENT_NODE_RECONNECTED);

        c.call();

        waitReconnectEvent(log, disconnectLatch);

        waitReconnectEvent(log, reconnectLatch);

        for (Ignite client : clients)
            client.events().stopLocalListen(p);
    }

    /**
     * @param restartZk If {@code true} in background restarts on of ZK servers.
     * @param closeClientSock If {@code true} in background closes zk clients' sockets.
     * @throws Exception If failed.
     */
    private void randomTopologyChanges(boolean restartZk, boolean closeClientSock) throws Exception {
        sesTimeout = 30_000;

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

        failCommSpi = false;
        PeerToPeerCommunicationFailureSpi.unfail();

        evts.clear();

        try {
            cleanPersistenceDir();
        }
        catch (Exception e) {
            error("Failed to delete DB files: " + e, e);
        }

        clientThreadLoc.set(null);
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
                    U.sleep(rnd.nextLong(2500));

                    int idx = rnd.nextInt(ZK_SRVS);

                    log.info("Restart ZK server: " + idx);

                    zkCluster.getServers().get(idx).restart();

                    waitForZkClusterReady(zkCluster);
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
                    "impl", "rtState", "evtsData", "evts");

                if (!evts.isEmpty()) {
                    info("Unacked events: " + evts);

                    return false;
                }

                return true;
            }
        }, 10_000));
    }

    /**
     *
     */
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    private void checkEventsConsistency() {
        for (Map.Entry<UUID, Map<T2<Integer, Long>, DiscoveryEvent>> nodeEvtEntry : evts.entrySet()) {
            UUID nodeId = nodeEvtEntry.getKey();
            Map<T2<Integer, Long>, DiscoveryEvent> nodeEvts = nodeEvtEntry.getValue();

            for (Map.Entry<UUID, Map<T2<Integer, Long>, DiscoveryEvent>> nodeEvtEntry0 : evts.entrySet()) {
                if (!nodeId.equals(nodeEvtEntry0.getKey())) {
                    Map<T2<Integer, Long>, DiscoveryEvent> nodeEvts0 = nodeEvtEntry0.getValue();

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
    private void checkEventsConsistency(Map<T2<Integer, Long>, DiscoveryEvent> evts1, Map<T2<Integer, Long>, DiscoveryEvent> evts2) {
        for (Map.Entry<T2<Integer, Long>, DiscoveryEvent> e1 : evts1.entrySet()) {
            DiscoveryEvent evt1 = e1.getValue();
            DiscoveryEvent evt2 = evts2.get(e1.getKey());

            if (evt2 != null) {
                assertEquals(evt1.topologyVersion(), evt2.topologyVersion());
                assertEquals(evt1.eventNode().consistentId(), evt2.eventNode().consistentId());
                assertTrue(equalsTopologies(evt1.topologyNodes(), evt2.topologyNodes()));
            }
        }
    }

    /**
     * @param nodes1 Nodes.
     * @param nodes2 Nodes to be compared with {@code nodes1} for equality.
     *
     * @return True if nodes equal by consistent id.
     */
    private boolean equalsTopologies(Collection<ClusterNode> nodes1, Collection<ClusterNode> nodes2) {
        if(nodes1.size() != nodes2.size())
            return false;

        Set<Object> consistentIds1 = nodes1.stream()
            .map(ClusterNode::consistentId)
            .collect(Collectors.toSet());

        return nodes2.stream()
            .map(ClusterNode::consistentId)
            .allMatch(consistentIds1::contains);
    }

    /**
     * @param node Node.
     * @return Node's discovery SPI.
     */
    private static ZookeeperDiscoverySpi spi(Ignite node) {
        return (ZookeeperDiscoverySpi)node.configuration().getDiscoverySpi();
    }

    /**
     * @param nodeName Node name.
     * @return Node's discovery SPI.
     * @throws Exception If failed.
     */
    private ZookeeperDiscoverySpi waitSpi(final String nodeName) throws Exception {
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                ZookeeperDiscoverySpi spi = spis.get(nodeName);

                return spi != null && GridTestUtils.getFieldValue(spi, "impl") != null;

            }
        }, 5000);

        ZookeeperDiscoverySpi spi = spis.get(nodeName);

        assertNotNull("Failed to get SPI for node: " + nodeName, spi);

        return spi;
    }

    /**
     * @param topVer Topology version.
     * @return Expected event instance.
     */
    private static DiscoveryEvent joinEvent(long topVer) {
        DiscoveryEvent expEvt = new DiscoveryEvent(null, null, EventType.EVT_NODE_JOINED, null);

        expEvt.topologySnapshot(topVer, null);

        return expEvt;
    }

    /**
     * @param topVer Topology version.
     * @return Expected event instance.
     */
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
            @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
            @Override public boolean apply() {
                Map<T2<Integer, Long>, DiscoveryEvent> nodeEvts = evts.get(nodeId);

                if (nodeEvts == null) {
                    info("No events for node: " + nodeId);

                    return false;
                }

                synchronized (nodeEvts) {
                    for (DiscoveryEvent expEvt : expEvts) {
                        DiscoveryEvent evt0 = nodeEvts.get(new T2<>(clusterNum.get(), expEvt.topologyVersion()));

                        if (evt0 == null) {
                            info("No event for version: " + expEvt.topologyVersion());

                            return false;
                        }

                        assertEquals("Unexpected event [topVer=" + expEvt.topologyVersion() +
                            ", exp=" + U.gridEventName(expEvt.type()) +
                            ", evt=" + evt0 + ']', expEvt.type(), evt0.type());
                    }
                }

                return true;
            }
        }, 30000));
    }

    /**
     * @param spi Spi instance.
     */
    private static void closeZkClient(ZookeeperDiscoverySpi spi) {
        ZooKeeper zk = zkClient(spi);

        try {
            zk.close();
        }
        catch (Exception e) {
            fail("Unexpected error: " + e);
        }
    }

    /**
     * @param spi Spi instance.
     * @return Zookeeper client.
     */
    private static ZooKeeper zkClient(ZookeeperDiscoverySpi spi) {
        return GridTestUtils.getFieldValue(spi, "impl", "rtState", "zkClient", "zk");
    }

    /**
     * Reconnect client node.
     *
     * @param log  Logger.
     * @param clients Clients.
     * @param closeSock {@code True} to simulate reconnect by closing zk client's socket.
     * @throws Exception If failed.
     */
    private static void reconnectClientNodes(final IgniteLogger log,
        List<Ignite> clients,
        boolean closeSock)
        throws Exception {
        final CountDownLatch disconnectLatch = new CountDownLatch(clients.size());
        final CountDownLatch reconnectLatch = new CountDownLatch(clients.size());

        IgnitePredicate<Event> p = new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                if (evt.type() == EVT_CLIENT_NODE_DISCONNECTED) {
                    log.info("Disconnected: " + evt);

                    disconnectLatch.countDown();
                }
                else if (evt.type() == EVT_CLIENT_NODE_RECONNECTED) {
                    log.info("Reconnected: " + evt);

                    reconnectLatch.countDown();
                }

                return true;
            }
        };

        List<String> zkNodes = new ArrayList<>();

        for (Ignite client : clients) {
            client.events().localListen(p, EVT_CLIENT_NODE_DISCONNECTED, EVT_CLIENT_NODE_RECONNECTED);

            zkNodes.add(aliveZkNodePath(client));
        }

        long timeout = 15_000;

        if (closeSock) {
            for (Ignite client : clients) {
                ZookeeperDiscoverySpi spi = (ZookeeperDiscoverySpi)client.configuration().getDiscoverySpi();

                ZkTestClientCnxnSocketNIO.forNode(client.name()).closeSocket(true);

                timeout = Math.max(timeout, (long)(spi.getSessionTimeout() * 1.5f));
            }
        }
        else {
            /*
             * Use hack to simulate session expire without waiting session timeout:
             * create and close ZooKeeper with the same session ID as ignite node's ZooKeeper.
             */
            List<ZooKeeper> dummyClients = new ArrayList<>();

            for (Ignite client : clients) {
                ZookeeperDiscoverySpi spi = (ZookeeperDiscoverySpi)client.configuration().getDiscoverySpi();

                ZooKeeper zk = zkClient(spi);

                for (String s : spi.getZkConnectionString().split(",")) {
                    try {
                        ZooKeeper dummyZk = new ZooKeeper(
                            s,
                            10_000,
                            null,
                            zk.getSessionId(),
                            zk.getSessionPasswd());

                        dummyZk.exists("/a", false);

                        dummyClients.add(dummyZk);

                        break;
                    }
                    catch (Exception e) {
                        log.warning("Can't connect to server " + s + " [err=" + e + ']');
                    }
                }
            }

            for (ZooKeeper zk : dummyClients)
                zk.close();
        }

        waitNoAliveZkNodes(log,
            ((ZookeeperDiscoverySpi)clients.get(0).configuration().getDiscoverySpi()).getZkConnectionString(),
            zkNodes,
            timeout);

        if (closeSock) {
            for (Ignite client : clients)
                ZkTestClientCnxnSocketNIO.forNode(client.name()).allowConnect();
        }

        waitReconnectEvent(log, disconnectLatch);

        waitReconnectEvent(log, reconnectLatch);

        for (Ignite client : clients)
            client.events().stopLocalListen(p);
    }

    /**
     * @param zk ZooKeeper client.
     * @param root Root path.
     * @return All children znodes for given path.
     * @throws Exception If failed/
     */
    private List<String> listSubTree(ZooKeeper zk, String root) throws Exception {
        for (int i = 0; i < 30; i++) {
            try {
                return ZKUtil.listSubTreeBFS(zk, root);
            }
            catch (KeeperException.NoNodeException e) {
                info("NoNodeException when get znodes, will retry: " + e);
            }
        }

        throw new Exception("Failed to get znodes: " + root);
    }

    /**
     * @param log Logger.
     * @param latch Latch.
     * @throws Exception If failed.
     */
    private static void waitReconnectEvent(IgniteLogger log, CountDownLatch latch) throws Exception {
        if (!latch.await(30_000, MILLISECONDS)) {
            log.error("Failed to wait for reconnect event, will dump threads, latch count: " + latch.getCount());

            U.dumpThreads(log);

            fail("Failed to wait for disconnect/reconnect event.");
        }
    }

    /**
     * @param cacheName Cache name.
     * @return Configuration.
     */
    private CacheConfiguration<Object, Object> largeCacheConfiguration(String cacheName) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(cacheName);

        ccfg.setAffinity(new TestAffinityFunction(1024 * 1024));
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void waitForTopology(int expSize) throws Exception {
        super.waitForTopology(expSize);

        // checkZkNodesCleanup();
    }

    /**
     *
     */
    @SuppressWarnings("MismatchedReadAndWriteOfArray")
    static class TestAffinityFunction extends RendezvousAffinityFunction {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private int[] dummyData;

        /**
         * @param dataSize Dummy data size.
         */
        TestAffinityFunction(int dataSize) {
            dummyData = new int[dataSize];

            for (int i = 0; i < dataSize; i++)
                dummyData[i] = i;
        }
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

    /**
     *
     */
    private static class C1 implements Serializable {
        // No-op.
    }

    /**
     *
     */
    private static class C2 implements Serializable {
        // No-op.
    }

    /**
     *
     */
    static class ZkTestNodeAuthenticator implements DiscoverySpiNodeAuthenticator {
        /**
         * @param failAuthNodes Node names which should not pass authentication.
         * @return Factory.
         */
        static IgniteOutClosure<DiscoverySpiNodeAuthenticator> factory(final String...failAuthNodes) {
            return new IgniteOutClosure<DiscoverySpiNodeAuthenticator>() {
                @Override public DiscoverySpiNodeAuthenticator apply() {
                    return new ZkTestNodeAuthenticator(Arrays.asList(failAuthNodes));
                }
            };
        }

        /** */
        private final Collection<String> failAuthNodes;

        /**
         * @param failAuthNodes Node names which should not pass authentication.
         */
        ZkTestNodeAuthenticator(Collection<String> failAuthNodes) {
            this.failAuthNodes = failAuthNodes;
        }

        /** {@inheritDoc} */
        @Override public SecurityContext authenticateNode(ClusterNode node, SecurityCredentials cred) {
            assertNotNull(cred);

            String nodeName = node.attribute(ATTR_IGNITE_INSTANCE_NAME);

            assertEquals(nodeName, cred.getUserObject());

            boolean auth = !failAuthNodes.contains(nodeName);

            System.out.println(Thread.currentThread().getName() + " authenticateNode [node=" + node.id() + ", res=" + auth + ']');

            return auth ? new TestSecurityContext(nodeName) : null;
        }

        /** {@inheritDoc} */
        @Override public boolean isGlobalNodeAuthentication() {
            return false;
        }

        /**
         *
         */
        private static class TestSecurityContext implements SecurityContext, Serializable {
            /** Serial version uid. */
            private static final long serialVersionUID = 0L;

            /** */
            final String nodeName;

            /**
             * @param nodeName Authenticated node name.
             */
            TestSecurityContext(String nodeName) {
                this.nodeName = nodeName;
            }

            /** {@inheritDoc} */
            @Override public SecuritySubject subject() {
                return null;
            }

            /** {@inheritDoc} */
            @Override public boolean taskOperationAllowed(String taskClsName, SecurityPermission perm) {
                return true;
            }

            /** {@inheritDoc} */
            @Override public boolean cacheOperationAllowed(String cacheName, SecurityPermission perm) {
                return true;
            }

            /** {@inheritDoc} */
            @Override public boolean serviceOperationAllowed(String srvcName, SecurityPermission perm) {
                return true;
            }

            /** {@inheritDoc} */
            @Override public boolean systemOperationAllowed(SecurityPermission perm) {
                return true;
            }
        }
    }

    /**
     *
     */
    static class CacheInfoCommunicationFailureResolver implements CommunicationFailureResolver {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /** */
        Map<String, CacheConfiguration<?, ?>>  caches;

        /** */
        Map<String, List<List<ClusterNode>>> affMap;

        /** */
        Map<String, List<List<ClusterNode>>> ownersMap;

        /** */
        volatile CountDownLatch latch;

        /** {@inheritDoc} */
        @Override public void resolve(CommunicationFailureContext ctx) {
            assert latch != null;
            assert latch.getCount() == 1L : latch.getCount();

            caches = ctx.startedCaches();

            log.info("Resolver called, started caches: " + caches.keySet());

            assertNotNull(caches);

            affMap = new HashMap<>();
            ownersMap = new HashMap<>();

            for (String cache : caches.keySet()) {
                affMap.put(cache, ctx.cacheAffinity(cache));
                ownersMap.put(cache, ctx.cachePartitionOwners(cache));
            }

            latch.countDown();
        }

        /**
         * @param expCaches Expected caches information (when late assignment doen and rebalance finished).
         */
        void checkCachesInfo(Map<String, T3<Integer, Integer, Integer>> expCaches) {
            assertNotNull(caches);
            assertNotNull(affMap);
            assertNotNull(ownersMap);

            for (Map.Entry<String, T3<Integer, Integer, Integer>> e : expCaches.entrySet()) {
                String cacheName = e.getKey();

                int parts = e.getValue().get1();
                int backups = e.getValue().get2();
                int expNodes = e.getValue().get3();

                assertTrue(cacheName, caches.containsKey(cacheName));

                CacheConfiguration ccfg = caches.get(cacheName);

                assertEquals(cacheName, ccfg.getName());

                if (ccfg.getCacheMode() == CacheMode.REPLICATED)
                    assertEquals(Integer.MAX_VALUE, ccfg.getBackups());
                else
                    assertEquals(backups, ccfg.getBackups());

                assertEquals(parts, ccfg.getAffinity().partitions());

                List<List<ClusterNode>> aff = affMap.get(cacheName);

                assertNotNull(cacheName, aff);
                assertEquals(parts, aff.size());

                List<List<ClusterNode>> owners = ownersMap.get(cacheName);

                assertNotNull(cacheName, owners);
                assertEquals(parts, owners.size());

                for (int i = 0; i < parts; i++) {
                    List<ClusterNode> partAff = aff.get(i);

                    assertEquals(cacheName, expNodes, partAff.size());

                    List<ClusterNode> partOwners = owners.get(i);

                    assertEquals(cacheName, expNodes, partOwners.size());

                    assertTrue(cacheName, partAff.containsAll(partOwners));
                    assertTrue(cacheName, partOwners.containsAll(partAff));
                }
            }
        }

        /**
         *
         */
        void reset() {
            caches = null;
            affMap = null;
            ownersMap = null;
        }
    }

    /**
     *
     */
    static class NoOpCommunicationFailureResolver implements CommunicationFailureResolver {
        /** */
        static final IgniteOutClosure<CommunicationFailureResolver> FACTORY = new IgniteOutClosure<CommunicationFailureResolver>() {
            @Override public CommunicationFailureResolver apply() {
                return new NoOpCommunicationFailureResolver();
            }
        };

        /** {@inheritDoc} */
        @Override public void resolve(CommunicationFailureContext ctx) {
            // No-op.
        }
    }

    /**
     *
     */
    static class KillCoordinatorCommunicationFailureResolver implements CommunicationFailureResolver {
        /** */
        static final IgniteOutClosure<CommunicationFailureResolver> FACTORY = new IgniteOutClosure<CommunicationFailureResolver>() {
            @Override public CommunicationFailureResolver apply() {
                return new KillCoordinatorCommunicationFailureResolver();
            }
        };

        /** */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public void resolve(CommunicationFailureContext ctx) {
            List<ClusterNode> nodes = ctx.topologySnapshot();

            ClusterNode node = nodes.get(0);

            log.info("Resolver kills node: " + node.id());

            ctx.killNode(node);
        }
    }

    /**
     *
     */
    static class KillRandomCommunicationFailureResolver implements CommunicationFailureResolver {
        /** */
        static final IgniteOutClosure<CommunicationFailureResolver> FACTORY = new IgniteOutClosure<CommunicationFailureResolver>() {
            @Override public CommunicationFailureResolver apply() {
                return new KillRandomCommunicationFailureResolver();
            }
        };

        /** Last killed nodes. */
        static final Set<ClusterNode> LAST_KILLED_NODES = new HashSet<>();

        /** */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public void resolve(CommunicationFailureContext ctx) {
            LAST_KILLED_NODES.clear();

            List<ClusterNode> nodes = ctx.topologySnapshot();

            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            int killNodes = rnd.nextInt(nodes.size() / 2);

            log.info("Resolver kills nodes [total=" + nodes.size() + ", kill=" + killNodes + ']');

            long srvCnt = nodes.stream().filter(node -> !node.isClient()).count();

            Set<Integer> idxs = new HashSet<>();

            while (idxs.size() < killNodes) {
                int idx = rnd.nextInt(nodes.size());

                if(!nodes.get(idx).isClient() && !idxs.contains(idx) && --srvCnt < 1)
                    continue;

                idxs.add(idx);
            }

            for (int idx : idxs) {
                ClusterNode node = nodes.get(idx);

                log.info("Resolver kills node: " + node.id());

                LAST_KILLED_NODES.add(node);

                ctx.killNode(node);
            }
        }
    }

    /**
     *
     */
    static class TestNodeKillCommunicationFailureResolver implements CommunicationFailureResolver {
        /**
         * @param killOrders Killed nodes order.
         * @return Factory.
         */
        static IgniteOutClosure<CommunicationFailureResolver> factory(final Collection<Long> killOrders)  {
            return new IgniteOutClosure<CommunicationFailureResolver>() {
                @Override public CommunicationFailureResolver apply() {
                    return new TestNodeKillCommunicationFailureResolver(killOrders);
                }
            };
        }

        /** */
        final Collection<Long> killNodeOrders;

        /**
         * @param killOrders Killed nodes order.
         */
        TestNodeKillCommunicationFailureResolver(Collection<Long> killOrders) {
            this.killNodeOrders = killOrders;
        }

        /** {@inheritDoc} */
        @Override public void resolve(CommunicationFailureContext ctx) {
            List<ClusterNode> nodes = ctx.topologySnapshot();

            assertTrue(nodes.size() > 0);

            for (ClusterNode node : nodes) {
                if (killNodeOrders.contains(node.order()))
                    ctx.killNode(node);
            }
        }
    }

    /**
     *
     */
    static class ZkTestCommunicationSpi extends TestRecordingCommunicationSpi {
        /** */
        private volatile CountDownLatch pingStartLatch;

        /** */
        private volatile CountDownLatch pingLatch;

        /** */
        private volatile BitSet checkRes;

        /**
         * @param ignite Node.
         * @return Node's communication SPI.
         */
        static ZkTestCommunicationSpi testSpi(Ignite ignite) {
            return (ZkTestCommunicationSpi)ignite.configuration().getCommunicationSpi();
        }

        /**
         * @param nodes Number of nodes.
         * @param setBitIdxs Bits indexes to set in check result.
         */
        void initCheckResult(int nodes, Integer... setBitIdxs) {
            checkRes = new BitSet(nodes);

            for (Integer bitIdx : setBitIdxs)
                checkRes.set(bitIdx);
        }

        /** {@inheritDoc} */
        @Override public IgniteFuture<BitSet> checkConnection(List<ClusterNode> nodes) {
            CountDownLatch pingStartLatch = this.pingStartLatch;

            if (pingStartLatch != null)
                pingStartLatch.countDown();

            CountDownLatch pingLatch = this.pingLatch;

            try {
                if (pingLatch != null)
                    pingLatch.await();
            }
            catch (InterruptedException e) {
                throw new IgniteException(e);
            }

            BitSet checkRes = this.checkRes;

            if (checkRes != null) {
                this.checkRes = null;

                return new IgniteFinishedFutureImpl<>(checkRes);
            }

            return super.checkConnection(nodes);
        }
    }

    /**
     *
     */
    static class TestFastStopProcessCustomMessage implements DiscoveryCustomMessage {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final IgniteUuid id = IgniteUuid.randomUuid();

        /** */
        private final boolean createAck;

        /** */
        private final int payload;

        /**
         * @param createAck Create ack message flag.
         * @param payload Payload.
         */
        TestFastStopProcessCustomMessage(boolean createAck, int payload) {
            this.createAck = createAck;
            this.payload = payload;

        }

        /** {@inheritDoc} */
        @Override public IgniteUuid id() {
            return id;
        }

        /** {@inheritDoc} */
        @Nullable @Override public DiscoveryCustomMessage ackMessage() {
            return createAck ? new TestFastStopProcessCustomMessageAck(payload) : null;
        }

        /** {@inheritDoc} */
        @Override public boolean isMutable() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean stopProcess() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr,
            AffinityTopologyVersion topVer,
            DiscoCache discoCache) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestFastStopProcessCustomMessage that = (TestFastStopProcessCustomMessage)o;

            return createAck == that.createAck && payload == that.payload;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(createAck, payload);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestFastStopProcessCustomMessage.class, this);
        }
    }

    /**
     *
     */
    static class TestFastStopProcessCustomMessageAck implements DiscoveryCustomMessage {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final IgniteUuid id = IgniteUuid.randomUuid();

        /** */
        private final int payload;

        /**
         * @param payload Payload.
         */
        TestFastStopProcessCustomMessageAck(int payload) {
            this.payload = payload;
        }

        /** {@inheritDoc} */
        @Override public IgniteUuid id() {
            return id;
        }

        /** {@inheritDoc} */
        @Nullable @Override public DiscoveryCustomMessage ackMessage() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean isMutable() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean stopProcess() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr,
            AffinityTopologyVersion topVer,
            DiscoCache discoCache) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestFastStopProcessCustomMessageAck that = (TestFastStopProcessCustomMessageAck)o;
            return payload == that.payload;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(payload);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestFastStopProcessCustomMessageAck.class, this);
        }
    }
}
