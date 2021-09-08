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
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingCluster;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CommunicationFailureResolver;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.discovery.DiscoveryLocalJoinData;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpiInternalListener;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.lang.IgniteInClosure2X;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.DiscoverySpiNodeAuthenticator;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpi;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpiTestUtil;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZkTestClientCnxnSocketNIO;
import org.apache.zookeeper.ZooKeeper;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_RECONNECTED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS;
import static org.apache.ignite.spi.discovery.zk.internal.ZookeeperDiscoveryImpl.IGNITE_ZOOKEEPER_DISCOVERY_SPI_ACK_THRESHOLD;
import static org.apache.zookeeper.client.ZKClientConfig.ZOOKEEPER_CLIENT_CNXN_SOCKET;

/**
 * Base class for Zookeeper SPI discovery tests in this package. It is intended to provide common overrides for
 * superclass methods to be shared by all subclasses.
 */
class ZookeeperDiscoverySpiTestBase extends GridCommonAbstractTest {
    /** */
    protected UUID nodeId;

    /** */
    protected long joinTimeout;

    /** */
    protected static TestingCluster zkCluster;

    /** To run test with real local ZK. */
    protected static final boolean USE_TEST_CLUSTER = true;

    /** */
    protected static final int ZK_SRVS = 3;

    /** */
    protected ConcurrentHashMap<String, ZookeeperDiscoverySpi> spis = new ConcurrentHashMap<>();

    /** */
    protected Map<String, Object> userAttrs;

    /**
     * Map for checking discovery events per test. The
     * {@link org.apache.ignite.events.EventType#EVT_NODE_JOINED EVT_NODE_JOINED},
     * {@link org.apache.ignite.events.EventType#EVT_NODE_FAILED EVT_NODE_FAILED},
     * {@link org.apache.ignite.events.EventType#EVT_NODE_LEFT EVT_NODE_LEFT}
     * events should be handled only once per topology version.
     *
     * Need to be cleaned in case of cluster restart.
     */
    protected static ConcurrentHashMap<UUID, Map<T2<Integer, Long>, DiscoveryEvent>> evts = new ConcurrentHashMap<>();

    /** */
    protected static volatile boolean err;

    /** The number of clusters started in one test (increments when the first node in the cluster starts). */
    protected final AtomicInteger clusterNum = new AtomicInteger(0);

    /** */
    protected final ZookeeperDiscoverySpiTestHelper helper = new ZookeeperDiscoverySpiTestHelper(this::info, clusterNum);

    /** */
    protected boolean testSockNio;

    /** */
    protected boolean testCommSpi;

    /** */
    protected boolean failCommSpi;

    /** */
    protected boolean blockCommSpi;

    /** */
    protected long sesTimeout;

    /** */
    protected boolean clientReconnectDisabled;

    /** */
    protected boolean dfltConsistenId;

    /** */
    protected boolean persistence;

    /** */
    protected IgniteOutClosure<CommunicationFailureResolver> commFailureRslvr;

    /** */
    protected IgniteOutClosure<DiscoverySpiNodeAuthenticator> auth;

    /** */
    protected String zkRootPath;

    /** */
    protected CacheAtomicityMode atomicityMode;

    /** */
    protected int backups = -1;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        System.setProperty(ZookeeperDiscoveryImpl.IGNITE_ZOOKEEPER_DISCOVERY_SPI_ACK_TIMEOUT, "1000");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() {
        stopZkCluster();

        System.clearProperty(ZookeeperDiscoveryImpl.IGNITE_ZOOKEEPER_DISCOVERY_SPI_ACK_TIMEOUT);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        if (USE_TEST_CLUSTER && zkCluster == null) {
            zkCluster = ZookeeperDiscoverySpiTestUtil.createTestingCluster(ZK_SRVS, clusterCustomProperties());

            zkCluster.start();

            waitForZkClusterReady(zkCluster);
        }

        reset();
    }

    /**
     * @return Cluster configuration custom properties.
     */
    protected Map<String, Object>[] clusterCustomProperties() {
        return new Map[ZK_SRVS];
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

    /** {@inheritDoc} */
    @Override protected void waitForTopology(int expSize) throws Exception {
        super.waitForTopology(expSize);

        // checkZkNodesCleanup();
    }

    /**
     * Wait for Zookeeper testing cluster ready for communications.
     *
     * @param zkCluster Zk cluster.
     */
    static void waitForZkClusterReady(TestingCluster zkCluster) throws InterruptedException {
        try (CuratorFramework curator = CuratorFrameworkFactory
            .newClient(zkCluster.getConnectString(), new RetryNTimes(10, 1_000))) {
            curator.start();

            assertTrue("Failed to wait for Zookeeper testing cluster ready.",
                curator.blockUntilConnected(30, SECONDS));
        }
    }

    /** */
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    void checkEventsConsistency() {
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
     * @throws Exception If failed.
     */
    void checkInternalStructuresCleanup() throws Exception {
        for (Ignite node : IgnitionEx.allGridsx()) {
            final AtomicReference<?> res = GridTestUtils.getFieldValue(spi(node), "impl", "commErrProcFut");

            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return res.get() == null;
                }
            }, 30_000);

            assertNull(res.get());
        }
    }

    /** */
    void reset() {
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
        if (nodes1.size() != nodes2.size())
            return false;

        Set<Object> consistentIds1 = nodes1.stream()
            .map(ClusterNode::consistentId)
            .collect(Collectors.toSet());

        return nodes2.stream()
            .map(ClusterNode::consistentId)
            .allMatch(consistentIds1::contains);
    }

    /** */
    private void clearAckEveryEventSystemProperty() {
        System.setProperty(IGNITE_ZOOKEEPER_DISCOVERY_SPI_ACK_THRESHOLD, "1");
    }

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

            zkSpi.setZkConnectionString(getTestClusterZkConnectionString());

            if (zkRootPath != null)
                zkSpi.setZkRootPath(zkRootPath);
        }
        else
            zkSpi.setZkConnectionString(getRealClusterZkConnectionString());

        cfg.setDiscoverySpi(zkSpi);

        cfg.setCacheConfiguration(getCacheConfiguration());

        if (userAttrs != null)
            cfg.setUserAttributes(userAttrs);

        Map<IgnitePredicate<? extends Event>, int[]> lsnrs = new HashMap<>();

        if (cfg.isClientMode()) {
            UUID currNodeId = cfg.getNodeId();

            lsnrs.put(new IgnitePredicate<Event>() {
                /** Last remembered uuid before node reconnected. */
                private UUID nodeId = currNodeId;

                @Override public boolean apply(Event evt) {
                    if (evt.type() == EVT_CLIENT_NODE_RECONNECTED) {
                        evts.remove(nodeId);

                        nodeId = evt.node().id();
                    }

                    return true;
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

        if (!isMultiJvm())
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

        if (blockCommSpi) {
            cfg.setCommunicationSpi(new TcpBlockCommunicationSpi(igniteInstanceName.contains("block"))
                .setUsePairedConnections(true));

            cfg.setNetworkTimeout(500);
        }

        if (commFailureRslvr != null)
            cfg.setCommunicationFailureResolver(commFailureRslvr.apply());

        cfg.setIncludeEventTypes(EventType.EVTS_ALL);

        return cfg;
    }

    /**
     * @return Zookeeper cluster connection string
     */
    protected String getTestClusterZkConnectionString() {
        return zkCluster.getConnectString();
    }

    /**
     * @return Zookeeper cluster connection string
     */
    protected String getRealClusterZkConnectionString() {
        return "localhost:2181";
    }

    /**
     * @param node Node.
     * @return Node's discovery SPI.
     */
    static ZookeeperDiscoverySpi spi(Ignite node) {
        return (ZookeeperDiscoverySpi)node.configuration().getDiscoverySpi();
    }

    /** */
    void stopZkCluster() {
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
     * @throws Exception If failed.
     */
    protected void checkZkNodesCleanup() throws Exception {
        final ZookeeperClient zkClient = new ZookeeperClient(getTestResources().getLogger(),
            zkCluster.getConnectString(),
            30_000,
            null);

        final String basePath = ZookeeperDiscoverySpiTestHelper.IGNITE_ZK_ROOT + "/";

        final String aliveDir = basePath + ZkIgnitePaths.ALIVE_NODES_DIR + "/";

        try {
            List<String> znodes = listSubTree(zkClient.zk(), ZookeeperDiscoverySpiTestHelper.IGNITE_ZK_ROOT);

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
                        List<String> znodes = listSubTree(zkClient.zk(), ZookeeperDiscoverySpiTestHelper.IGNITE_ZK_ROOT);

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
     * Communication SPI with possibility to simulate network problems between some of the cluster nodes.
     */
    static class PeerToPeerCommunicationFailureSpi extends TcpCommunicationSpi {
        /** Flag indicates that connections according to {@code matrix} should be failed. */
        private static volatile boolean failure;

        /** Connections failure matrix. */
        private static volatile ZookeeperDiscoverySplitBrainTest.ConnectionsFailureMatrix matrix;

        /**
         * Start failing connections according to given matrix {@code with}.
         * @param with Failure matrix.
         */
        static void fail(ZookeeperDiscoverySplitBrainTest.ConnectionsFailureMatrix with) {
            matrix = with;
            failure = true;
        }

        /**
         * Resets failure matrix.
         */
        private static void unfail() {
            failure = false;
        }

        /** {@inheritDoc} */
        @Override public IgniteFuture<BitSet> checkConnection(List<ClusterNode> nodes) {
            // Creates connections statuses according to failure matrix.
            BitSet bitSet = new BitSet();

            ClusterNode locNode = getLocalNode();

            int idx = 0;

            for (ClusterNode remoteNode : nodes) {
                if (locNode.id().equals(remoteNode.id()))
                    bitSet.set(idx);
                else {
                    if (matrix.hasConnection(locNode, remoteNode))
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
                processSessionCreationError(node, null, new IgniteCheckedException("Test", new SocketTimeoutException()));

                return null;
            }

            return new FailingCommunicationClient(getLocalNode(), node,
                super.createTcpClient(node, connIdx));
        }

        /**
         * Communication client with possibility to simulate network error between peers.
         */
        static class FailingCommunicationClient implements GridCommunicationClient {
            /** Delegate. */
            private final GridCommunicationClient delegate;

            /** Local node which sends messages. */
            private final ClusterNode locNode;

            /** Remote node which receives messages. */
            private final ClusterNode remoteNode;

            /** */
            FailingCommunicationClient(ClusterNode locNode, ClusterNode remoteNode, GridCommunicationClient delegate) {
                this.delegate = delegate;
                this.locNode = locNode;
                this.remoteNode = remoteNode;
            }

            /** {@inheritDoc} */
            @Override public void doHandshake(IgniteInClosure2X<InputStream, OutputStream> handshakeC) throws IgniteCheckedException {
                if (failure && !matrix.hasConnection(locNode, remoteNode))
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
                if (failure && !matrix.hasConnection(locNode, remoteNode))
                    throw new IgniteCheckedException("Test", new SocketTimeoutException());

                delegate.sendMessage(data);
            }

            /** {@inheritDoc} */
            @Override public void sendMessage(byte[] data, int len) throws IgniteCheckedException {
                if (failure && !matrix.hasConnection(locNode, remoteNode))
                    throw new IgniteCheckedException("Test", new SocketTimeoutException());

                delegate.sendMessage(data, len);
            }

            /** {@inheritDoc} */
            @Override public boolean sendMessage(@Nullable UUID nodeId, Message msg,
                @Nullable IgniteInClosure<IgniteException> c) throws IgniteCheckedException {
                // This will enforce SPI to create new client.
                if (failure && !matrix.hasConnection(locNode, remoteNode))
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

    /** */
    static class ZkTestCommunicationSpi extends TestRecordingCommunicationSpi {
        /** */
        volatile CountDownLatch pingStartLatch;

        /** */
        volatile CountDownLatch pingLatch;

        /** */
        volatile BitSet checkRes;

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
     * Block communications.
     */
    private static class TcpBlockCommunicationSpi extends TcpCommunicationSpi {
        /**
         * Whether this instance should actually block.
         */
        private final boolean isBlocking;

        /** Blocked once. */
        private boolean alreadyBlocked;

        /**
         * @param isBlocking Whether this instance should actually block.
         */
        public TcpBlockCommunicationSpi(boolean isBlocking) {
            this.isBlocking = isBlocking;
        }

        /** {@inheritDoc} */
        @Override protected GridCommunicationClient createTcpClient(ClusterNode node, int connIdx)
            throws IgniteCheckedException {
            if (node.isClient() && blockHandshakeOnce()) {
                ZookeeperDiscoverySpi spi = spi(ignite());

                spi.resolveCommunicationFailure(spi.getRemoteNodes().iterator().next(), new Exception("test"));

                return null;
            }

            return super.createTcpClient(node, connIdx);
        }

        /** Check if this connection is blocked. */
        private boolean blockHandshakeOnce() {
            if (isBlocking && !alreadyBlocked) {
                alreadyBlocked = true;

                return true;
            }

            return false;
        }
    }
}
