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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
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
import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.TestingZooKeeperServer;
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
    private static ConcurrentHashMap<UUID, Map<Long, DiscoveryEvent>> evts = new ConcurrentHashMap<>();

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

        lsnrs.put(new IgnitePredicate<Event>() {
            /** */
            @IgniteInstanceResource
            private Ignite ignite;

            @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
            @Override public boolean apply(Event evt) {
                try {
                    DiscoveryEvent discoveryEvt = (DiscoveryEvent)evt;

                    UUID locId = ((IgniteKernal)ignite).context().localNodeId();

                    Map<Long, DiscoveryEvent> nodeEvts = evts.get(locId);

                    if (nodeEvts == null) {
                        Object old = evts.put(locId, nodeEvts = new TreeMap<>());

                        assertNull(old);

                        // If the current node has failed, the local join will never happened.
                        if (evt.type() != EVT_NODE_FAILED ||
                            discoveryEvt.eventNode().consistentId().equals(ignite.configuration().getConsistentId())) {
                            synchronized (nodeEvts) {
                                DiscoveryLocalJoinData locJoin = ((IgniteEx)ignite).context().discovery().localJoin();

                                nodeEvts.put(locJoin.event().topologyVersion(), locJoin.event());
                            }
                        }
                    }

                    synchronized (nodeEvts) {
                        DiscoveryEvent old = nodeEvts.put(discoveryEvt.topologyVersion(), discoveryEvt);

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
            reset();

            stopAllGrids();
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
        for (Ignite node : G.allGrids()) {
            final AtomicReference<?> res = GridTestUtils.getFieldValue(spi(node), "impl", "commErrProcFut");

            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return res.get() == null;
                }
            }, 30_000);

            assertNull(res.get());
        }
    }

    /**
     * Verifies that node attributes returned through public API are presented in standard form.
     *
     * It means there is no exotic classes that may unnecessary capture other classes from the context.
     *
     * For more information about the problem refer to
     * <a href="https://issues.apache.org/jira/browse/IGNITE-8857">IGNITE-8857</a>.
     */
    public void testNodeAttributesNotReferencingZookeeperClusterNode() throws Exception {
        userAttrs = new HashMap<>();
        userAttrs.put("testAttr", "testAttr");

        try {
            IgniteEx ignite = startGrid(0);

            Map<String, Object> attrs = ignite.cluster().localNode().attributes();

            assertTrue(attrs instanceof PredicateMapView);

            IgnitePredicate[] preds = GridTestUtils.getFieldValue(attrs, "preds");

            assertNotNull(preds);

            assertTrue(preds.length == 1);

            assertTrue(preds[0] instanceof SecurityCredentialsAttrFilterPredicate);
        }
        finally {
            userAttrs = null;
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testZkRootNotExists() throws Exception {
        zkRootPath = "/a/b/c";

        for (int i = 0; i < 3; i++) {
            reset();

            startGridsMultiThreaded(5);

            waitForTopology(5);

            stopAllGrids();

            checkEventsConsistency();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testMetadataUpdate() throws Exception {
        startGrid(0);

        GridTestUtils.runMultiThreaded(new Callable<Void>() {
            @Override public Void call() throws Exception {
                ignite(0).configuration().getMarshaller().marshal(new C1());
                ignite(0).configuration().getMarshaller().marshal(new C2());

                return null;
            }
        }, 64, "marshal");
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeAddresses() throws Exception {
        startGridsMultiThreaded(3);

        clientMode(true);

        startGridsMultiThreaded(3, 3);

        waitForTopology(6);

        for (Ignite node : G.allGrids()) {
            ClusterNode locNode0 = node.cluster().localNode();

            assertTrue(locNode0.addresses().size() > 0);
            assertTrue(locNode0.hostNames().size() > 0);

            for (ClusterNode node0 : node.cluster().nodes()) {
                assertTrue(node0.addresses().size() > 0);
                assertTrue(node0.hostNames().size() > 0);
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSetConsistentId() throws Exception {
        startGridsMultiThreaded(3);

        clientMode(true);

        startGridsMultiThreaded(3, 3);

        waitForTopology(6);

        for (Ignite node : G.allGrids()) {
            ClusterNode locNode0 = node.cluster().localNode();

            assertEquals(locNode0.attribute(ATTR_IGNITE_INSTANCE_NAME),
                locNode0.consistentId());

            for (ClusterNode node0 : node.cluster().nodes()) {
                assertEquals(node0.attribute(ATTR_IGNITE_INSTANCE_NAME),
                    node0.consistentId());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDefaultConsistentId() throws Exception {
        dfltConsistenId = true;

        startGridsMultiThreaded(3);

        clientMode(true);

        startGridsMultiThreaded(3, 3);

        waitForTopology(6);

        for (Ignite node : G.allGrids()) {
            ClusterNode locNode0 = node.cluster().localNode();

            assertNotNull(locNode0.consistentId());

            for (ClusterNode node0 : node.cluster().nodes())
                assertNotNull(node0.consistentId());
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testMbean() throws Exception {
        startGrids(3);

        MBeanServer srv = ManagementFactory.getPlatformMBeanServer();

        UUID crdNodeId = grid(0).localNode().id();

        try {
            for (int i = 0; i < 3; i++) {
                IgniteEx grid = grid(i);

                ObjectName spiName = U.makeMBeanName(grid.context().igniteInstanceName(), "SPIs",
                    ZookeeperDiscoverySpi.class.getSimpleName());

                ZookeeperDiscoverySpiMBean bean = JMX.newMBeanProxy(srv, spiName, ZookeeperDiscoverySpiMBean.class);

                assertNotNull(bean);

                assertEquals(String.valueOf(grid.cluster().node(crdNodeId)), bean.getCoordinatorNodeFormatted());
                assertEquals(String.valueOf(grid.cluster().localNode()), bean.getLocalNodeFormatted());
                assertEquals(zkCluster.getConnectString(), bean.getZkConnectionString());
                assertEquals((long)grid.configuration().getFailureDetectionTimeout(), bean.getZkSessionTimeout());
            }
        }
        finally {
            stopAllGrids();
        }
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

        clientMode(true);

        startGrid(1);

        for (Ignite node : G.allGrids()) {
            assertEquals(1, node.cluster().forClients().nodes().size());
            assertEquals(1, node.cluster().forServers().nodes().size());
        }

        clientMode(false);

        startGrid(2);

        clientMode(true);

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
    public void _testLocalAuthenticationFails() throws Exception {
        auth = ZkTestNodeAuthenticator.factory(getTestIgniteInstanceName(0));

        Throwable err = GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(0);

                return null;
            }
        }, IgniteCheckedException.class, null);

        IgniteSpiException spiErr = X.cause(err, IgniteSpiException.class);

        assertNotNull(spiErr);
        assertTrue(spiErr.getMessage().contains("Authentication failed for local node"));

        startGrid(1);
        startGrid(2);

        checkTestSecuritySubject(2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAuthentication() throws Exception {
        auth = ZkTestNodeAuthenticator.factory(getTestIgniteInstanceName(1),
            getTestIgniteInstanceName(5));

        startGrid(0);

        checkTestSecuritySubject(1);

        {
            clientMode(false);
            checkStartFail(1);

            clientMode(true);
            checkStartFail(1);

            clientMode(false);
        }

        startGrid(2);

        checkTestSecuritySubject(2);

        stopGrid(2);

        checkTestSecuritySubject(1);

        startGrid(2);

        checkTestSecuritySubject(2);

        stopGrid(0);

        checkTestSecuritySubject(1);

        checkStartFail(1);

        clientMode(false);

        startGrid(3);

        clientMode(true);

        startGrid(4);

        clientMode(false);

        startGrid(0);

        checkTestSecuritySubject(4);

        checkStartFail(1);
        checkStartFail(5);

        clientMode(true);

        checkStartFail(1);
        checkStartFail(5);
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
    public void testCustomEvents_FastStopProcess_1() throws Exception {
        customEvents_FastStopProcess(1, 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCustomEvents_FastStopProcess_2() throws Exception {
        customEvents_FastStopProcess(5, 5);
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

    /**
     * Verifies correct handling of SEGMENTATION event with STOP segmentation policy: node is stopped successfully,
     * all its threads are shut down.
     *
     * @throws Exception If failed.
     *
     * @see <a href="https://issues.apache.org/jira/browse/IGNITE-9040">IGNITE-9040</a> ticket for more context of the test.
     */
    public void testStopNodeOnSegmentaion() throws Exception {
        try {
            System.setProperty("IGNITE_WAL_LOG_TX_RECORDS", "true");

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

            clientMode(true);

            final IgniteEx client = startGrid(2);

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
        finally {
            System.clearProperty("IGNITE_WAL_LOG_TX_RECORDS");
        }
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
     * @throws Exception If failed.
     */
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
            zkCluster = ZookeeperDiscoverySpiTestSuite2.createTestingCluster(ZK_SRVS);

            zkCluster.start();
        }
    }

    /**
     * @throws Exception If failed.
     */
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

            zkCluster = ZookeeperDiscoverySpiTestSuite2.createTestingCluster(ZK_SRVS);

            zkCluster.start();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testQuorumRestore() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-8180");

        sesTimeout = 15_000;

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

            zkCluster = ZookeeperDiscoverySpiTestSuite2.createTestingCluster(ZK_SRVS);

            zkCluster.start();
        }
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
     * @throws Exception If failed.
     */
    public void testConcurrentStartWithClient() throws Exception {
        final int NODES = 20;

        for (int i = 0; i < 3; i++) {
            info("Iteration: " + i);

            final int srvIdx = ThreadLocalRandom.current().nextInt(NODES);

            final AtomicInteger idx = new AtomicInteger();

            GridTestUtils.runMultiThreaded(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    int threadIdx = idx.getAndIncrement();

                    clientModeThreadLocal(threadIdx == srvIdx || ThreadLocalRandom.current().nextBoolean());

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
    public void testConcurrentStartStop1() throws Exception {
       concurrentStartStop(1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentStartStop2() throws Exception {
        concurrentStartStop(5);
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentStartStop2_EventsThrottle() throws Exception {
        System.setProperty(ZookeeperDiscoveryImpl.IGNITE_ZOOKEEPER_DISCOVERY_SPI_MAX_EVTS, "1");

        try {
            concurrentStartStop(5);
        }
        finally {
            System.clearProperty(ZookeeperDiscoveryImpl.IGNITE_ZOOKEEPER_DISCOVERY_SPI_MAX_EVTS);
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

        clientMode(true);

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
    public void testTopologyChangeMultithreaded() throws Exception {
        topologyChangeWithRestarts(false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTopologyChangeMultithreaded_RestartZk() throws Exception {
        try {
            topologyChangeWithRestarts(true, false);
        }
        finally {
            zkCluster.close();

            zkCluster = null;
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTopologyChangeMultithreaded_RestartZk_CloseClients() throws Exception {
        try {
            topologyChangeWithRestarts(true, true);
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
    public void testRandomTopologyChanges() throws Exception {
        randomTopologyChanges(false, false);
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
     * @throws Exception If failed.
     */
    public void testRandomTopologyChanges_RestartZk() throws Exception {
        randomTopologyChanges(true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRandomTopologyChanges_CloseClients() throws Exception {
        randomTopologyChanges(false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeployService1() throws Exception {
        startGridsMultiThreaded(3);

        grid(0).services(grid(0).cluster()).deployNodeSingleton("test", new GridCacheAbstractFullApiSelfTest.DummyServiceImpl());
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeployService2() throws Exception {
        clientMode(false);

        startGrid(0);

        clientMode(true);

        startGrid(1);

        grid(0).services(grid(0).cluster()).deployNodeSingleton("test", new GridCacheAbstractFullApiSelfTest.DummyServiceImpl());
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeployService3() throws Exception {
        IgniteInternalFuture fut = GridTestUtils.runAsync(new Callable() {
            @Override public Object call() throws Exception {
                clientModeThreadLocal(true);

                startGrid(0);

                return null;
            }
        }, "start-node");

        clientModeThreadLocal(false);

        startGrid(1);

        fut.get();

        grid(0).services(grid(0).cluster()).deployNodeSingleton("test", new GridCacheAbstractFullApiSelfTest.DummyServiceImpl());
    }

    /**
     * @throws Exception If failed.
     */
    public void testLargeUserAttribute1() throws Exception {
        initLargeAttribute();

        startGrid(0);

        checkZkNodesCleanup();

        userAttrs = null;

        startGrid(1);

        waitForEventsAcks(ignite(0));

        waitForTopology(2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLargeUserAttribute2() throws Exception {
        startGrid(0);

        initLargeAttribute();

        startGrid(1);

        waitForEventsAcks(ignite(0));

        checkZkNodesCleanup();
    }

    /**
     * @throws Exception If failed.
     */
    public void testLargeUserAttribute3() throws Exception {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        long stopTime = System.currentTimeMillis() + 60_000;

        int nodes = 0;

        for (int i = 0; i < 25; i++) {
            info("Iteration: " + i);

            if (rnd.nextBoolean())
                initLargeAttribute();
            else
                userAttrs = null;

            clientMode(i > 5);

            startGrid(i);

            nodes++;

            if (System.currentTimeMillis() >= stopTime)
                break;
        }

        waitForTopology(nodes);
    }

    /**
     *
     */
    private void initLargeAttribute() {
        userAttrs = new HashMap<>();

        int[] attr = new int[1024 * 1024 + ThreadLocalRandom.current().nextInt(1024)];

        for (int i = 0; i < attr.length; i++)
            attr[i] = i;

        userAttrs.put("testAttr", attr);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLargeCustomEvent() throws Exception {
        Ignite srv0 = startGrid(0);

        // Send large message, single node in topology.
        IgniteCache<Object, Object> cache = srv0.createCache(largeCacheConfiguration("c1"));

        for (int i = 0; i < 100; i++)
            cache.put(i, i);

        assertEquals(1, cache.get(1));

        waitForEventsAcks(ignite(0));

        startGridsMultiThreaded(1, 3);

        srv0.destroyCache("c1");

        // Send large message, multiple nodes in topology.
        cache = srv0.createCache(largeCacheConfiguration("c1"));

        for (int i = 0; i < 100; i++)
            cache.put(i, i);

        waitForTopology(4);

        ignite(3).createCache(largeCacheConfiguration("c2"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientReconnectSessionExpire1_1() throws Exception {
        clientReconnectSessionExpire(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientReconnectSessionExpire1_2() throws Exception {
        clientReconnectSessionExpire(true);
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
     * @throws Exception If failed.
     */
    public void testForceClientReconnect() throws Exception {
        final int SRVS = 3;

        startGrids(SRVS);

        clientMode(true);

        startGrid(SRVS);

        reconnectClientNodes(Collections.singletonList(ignite(SRVS)), new Callable<Void>() {
            @Override public Void call() throws Exception {
                ZookeeperDiscoverySpi spi = waitSpi(getTestIgniteInstanceName(SRVS));

                spi.clientReconnect();

                return null;
            }
        });

        waitForTopology(SRVS + 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testForcibleClientFail() throws Exception {
        final int SRVS = 3;

        startGrids(SRVS);

        clientMode(true);

        startGrid(SRVS);

        reconnectClientNodes(Collections.singletonList(ignite(SRVS)), new Callable<Void>() {
            @Override public Void call() throws Exception {
                ZookeeperDiscoverySpi spi = waitSpi(getTestIgniteInstanceName(0));

                spi.failNode(ignite(SRVS).cluster().localNode().id(), "Test forcible node fail");

                return null;
            }
        });

        waitForTopology(SRVS + 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDuplicatedNodeId() throws Exception {
        UUID nodeId0 = nodeId = UUID.randomUUID();

        startGrid(0);

        int failingNodeIdx = 100;

        for (int i = 0; i < 5; i++) {
            final int idx = failingNodeIdx++;

            nodeId = nodeId0;

            info("Start node with duplicated ID [iter=" + i + ", nodeId=" + nodeId + ']');

            Throwable err = GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    startGrid(idx);

                    return null;
                }
            }, IgniteCheckedException.class, null);

            IgniteSpiException spiErr = X.cause(err, IgniteSpiException.class);

            assertNotNull(spiErr);
            assertTrue(spiErr.getMessage().contains("Node with the same ID already exists"));

            nodeId = null;

            info("Start node with unique ID [iter=" + i + ']');

            Ignite ignite = startGrid(idx);

            nodeId0 = ignite.cluster().localNode().id();

            waitForTopology(i + 2);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPing() throws Exception {
        sesTimeout = 5000;

        startGrids(3);

        final ZookeeperDiscoverySpi spi = waitSpi(getTestIgniteInstanceName(1));

        final UUID nodeId = ignite(2).cluster().localNode().id();

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
            @Override public void run() {
                assertTrue(spi.pingNode(nodeId));
            }
        }, 32, "ping");

        fut.get();

        fut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
            @Override public void run() {
                spi.pingNode(nodeId);
            }
        }, 32, "ping");

        U.sleep(100);

        stopGrid(2);

        fut.get();

        fut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
            @Override public void run() {
                assertFalse(spi.pingNode(nodeId));
            }
        }, 32, "ping");

        fut.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testWithPersistence1() throws Exception {
        startWithPersistence(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testWithPersistence2() throws Exception {
        startWithPersistence(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoOpCommunicationFailureResolve_1() throws Exception {
        communicationFailureResolve_Simple(2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoOpCommunicationErrorResolve_2() throws Exception {
        communicationFailureResolve_Simple(10);
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
     * Tests case when one node fails before sending communication status.
     *
     * @throws Exception If failed.
     */
    public void testNoOpCommunicationErrorResolve_3() throws Exception {
        sesTimeout = 2000;
        commFailureRslvr = NoOpCommunicationFailureResolver.FACTORY;

        startGridsMultiThreaded(3);

        sesTimeout = 10_000;

        testSockNio = true;
        sesTimeout = 5000;

        startGrid(3);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() {
                ZookeeperDiscoverySpi spi = spi(ignite(0));

                spi.resolveCommunicationFailure(ignite(1).cluster().localNode(), new Exception("test"));

                return null;
            }
        });

        U.sleep(1000);

        ZkTestClientCnxnSocketNIO nio = ZkTestClientCnxnSocketNIO.forNode(ignite(3));

        nio.closeSocket(true);

        try {
            stopGrid(3);

            fut.get();
        }
        finally {
            nio.allowConnect();
        }

        waitForTopology(3);
    }

    /**
     * Tests case when Coordinator fails while resolve process is in progress.
     *
     * @throws Exception If failed.
     */
    public void testNoOpCommunicationErrorResolve_4() throws Exception {
        testCommSpi = true;

        sesTimeout = 2000;
        commFailureRslvr = NoOpCommunicationFailureResolver.FACTORY;

        startGrid(0);

        startGridsMultiThreaded(1, 3);

        ZkTestCommunicationSpi commSpi = ZkTestCommunicationSpi.testSpi(ignite(3));

        commSpi.pingLatch = new CountDownLatch(1);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() {
                ZookeeperDiscoverySpi spi = spi(ignite(1));

                spi.resolveCommunicationFailure(ignite(2).cluster().localNode(), new Exception("test"));

                return null;
            }
        });

        U.sleep(1000);

        assertFalse(fut.isDone());

        stopGrid(0);

        commSpi.pingLatch.countDown();

        fut.get();

        waitForTopology(3);
    }

    /**
     * Tests that nodes join is delayed while resolve is in progress.
     *
     * @throws Exception If failed.
     */
    public void testNoOpCommunicationErrorResolve_5() throws Exception {
        testCommSpi = true;

        sesTimeout = 2000;
        commFailureRslvr = NoOpCommunicationFailureResolver.FACTORY;

        startGrid(0);

        startGridsMultiThreaded(1, 3);

        ZkTestCommunicationSpi commSpi = ZkTestCommunicationSpi.testSpi(ignite(3));

        commSpi.pingStartLatch = new CountDownLatch(1);
        commSpi.pingLatch = new CountDownLatch(1);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() {
                ZookeeperDiscoverySpi spi = spi(ignite(1));

                spi.resolveCommunicationFailure(ignite(2).cluster().localNode(), new Exception("test"));

                return null;
            }
        });

        assertTrue(commSpi.pingStartLatch.await(10, SECONDS));

        try {
            assertFalse(fut.isDone());

            final AtomicInteger nodeIdx = new AtomicInteger(3);

            IgniteInternalFuture<?> startFut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    startGrid(nodeIdx.incrementAndGet());

                    return null;
                }
            }, 3, "start-node");

            U.sleep(1000);

            assertFalse(startFut.isDone());

            assertEquals(4, ignite(0).cluster().nodes().size());

            commSpi.pingLatch.countDown();

            startFut.get();
            fut.get();

            waitForTopology(7);
        }
        finally {
            commSpi.pingLatch.countDown();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCommunicationErrorResolve_KillNode_1() throws Exception {
        communicationFailureResolve_KillNodes(2, Collections.singleton(2L));
    }

    /**
     * @throws Exception If failed.
     */
    public void testCommunicationErrorResolve_KillNode_2() throws Exception {
        communicationFailureResolve_KillNodes(3, Collections.singleton(2L));
    }

    /**
     * @throws Exception If failed.
     */
    public void testCommunicationErrorResolve_KillNode_3() throws Exception {
        communicationFailureResolve_KillNodes(10, Arrays.asList(2L, 4L, 6L));
    }

    /**
     * @throws Exception If failed.
     */
    public void testCommunicationErrorResolve_KillCoordinator_1() throws Exception {
        communicationFailureResolve_KillNodes(2, Collections.singleton(1L));
    }

    /**
     * @throws Exception If failed.
     */
    public void testCommunicationErrorResolve_KillCoordinator_2() throws Exception {
        communicationFailureResolve_KillNodes(3, Collections.singleton(1L));
    }

    /**
     * @throws Exception If failed.
     */
    public void testCommunicationErrorResolve_KillCoordinator_3() throws Exception {
        communicationFailureResolve_KillNodes(10, Arrays.asList(1L, 4L, 6L));
    }

    /**
     * @throws Exception If failed.
     */
    public void testCommunicationErrorResolve_KillCoordinator_4() throws Exception {
        communicationFailureResolve_KillNodes(10, Arrays.asList(1L, 2L, 3L));
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
     * @throws Exception If failed.
     */
    public void testCommunicationFailureResolve_KillCoordinator_5() throws Exception {
        sesTimeout = 2000;

        testCommSpi = true;
        commFailureRslvr = KillCoordinatorCommunicationFailureResolver.FACTORY;

        startGrids(10);

        int crd = 0;

        int nodeIdx = 10;

        for (int i = 0; i < 10; i++) {
            info("Iteration: " + i);

            for (Ignite node : G.allGrids())
                ZkTestCommunicationSpi.testSpi(node).initCheckResult(10);

            UUID crdId = ignite(crd).cluster().localNode().id();

            ZookeeperDiscoverySpi spi = spi(ignite(crd + 1));

            try {
                spi.resolveCommunicationFailure(spi.getNode(crdId), new Exception("test"));

                fail("Exception is not thrown");
            }
            catch (IgniteSpiException e) {
                assertTrue("Unexpected exception: " + e, e.getCause() instanceof ClusterTopologyCheckedException);
            }

            waitForTopology(9);

            startGrid(nodeIdx++);

            waitForTopology(10);

            crd++;
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCommunicationFailureResolve_KillRandom() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-8179");

        sesTimeout = 2000;

        testCommSpi = true;
        commFailureRslvr = KillRandomCommunicationFailureResolver.FACTORY;

        startGridsMultiThreaded(10);

        clientMode(true);

        startGridsMultiThreaded(10, 5);

        int nodeIdx = 15;

        for (int i = 0; i < 10; i++) {
            info("Iteration: " + i);

            ZookeeperDiscoverySpi spi = null;

            for (Ignite node : G.allGrids()) {
                ZkTestCommunicationSpi.testSpi(node).initCheckResult(100);

                spi = spi(node);
            }

            assert spi != null;

            try {
                spi.resolveCommunicationFailure(spi.getRemoteNodes().iterator().next(), new Exception("test"));
            }
            catch (IgniteSpiException ignore) {
                // No-op.
            }

            clientMode(ThreadLocalRandom.current().nextBoolean());

            startGrid(nodeIdx++);

            awaitPartitionMapExchange();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDefaultCommunicationFailureResolver1() throws Exception {
        testCommSpi = true;
        sesTimeout = 5000;

        startGrids(3);

        ZkTestCommunicationSpi.testSpi(ignite(0)).initCheckResult(3, 0, 1);
        ZkTestCommunicationSpi.testSpi(ignite(1)).initCheckResult(3, 0, 1);
        ZkTestCommunicationSpi.testSpi(ignite(2)).initCheckResult(3, 2);

        UUID killedId = nodeId(2);

        assertNotNull(ignite(0).cluster().node(killedId));

        ZookeeperDiscoverySpi spi = spi(ignite(0));

        spi.resolveCommunicationFailure(spi.getNode(ignite(1).cluster().localNode().id()), new Exception("test"));

        waitForTopology(2);

        assertNull(ignite(0).cluster().node(killedId));
    }

    /**
     * @throws Exception If failed.
     */
    public void testDefaultCommunicationFailureResolver2() throws Exception {
        testCommSpi = true;
        sesTimeout = 5000;

        startGrids(3);

        clientMode(true);

        startGridsMultiThreaded(3, 2);

        ZkTestCommunicationSpi.testSpi(ignite(0)).initCheckResult(5, 0, 1);
        ZkTestCommunicationSpi.testSpi(ignite(1)).initCheckResult(5, 0, 1);
        ZkTestCommunicationSpi.testSpi(ignite(2)).initCheckResult(5, 2, 3, 4);
        ZkTestCommunicationSpi.testSpi(ignite(3)).initCheckResult(5, 2, 3, 4);
        ZkTestCommunicationSpi.testSpi(ignite(4)).initCheckResult(5, 2, 3, 4);

        ZookeeperDiscoverySpi spi = spi(ignite(0));

        spi.resolveCommunicationFailure(spi.getNode(ignite(1).cluster().localNode().id()), new Exception("test"));

        waitForTopology(2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDefaultCommunicationFailureResolver3() throws Exception {
        defaultCommunicationFailureResolver_BreakCommunication(3, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDefaultCommunicationFailureResolver4() throws Exception {
        defaultCommunicationFailureResolver_BreakCommunication(3, 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDefaultCommunicationFailureResolver5() throws Exception {
        defaultCommunicationFailureResolver_BreakCommunication(10, 1, 3, 6);
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
     * @throws Exception If failed.
     */
    public void testCommunicationFailureResolve_CachesInfo1() throws Exception {
        testCommSpi = true;
        sesTimeout = 5000;

        final CacheInfoCommunicationFailureResolver rslvr = new CacheInfoCommunicationFailureResolver();

        commFailureRslvr = new IgniteOutClosure<CommunicationFailureResolver>() {
            @Override public CommunicationFailureResolver apply() {
                return rslvr;
            }
        };

        startGrids(2);

        awaitPartitionMapExchange();

        Map<String, T3<Integer, Integer, Integer>> expCaches = new HashMap<>();

        expCaches.put(DEFAULT_CACHE_NAME, new T3<>(RendezvousAffinityFunction.DFLT_PARTITION_COUNT, 0, 1));

        checkResolverCachesInfo(ignite(0), expCaches);

        List<CacheConfiguration> caches = new ArrayList<>();

        CacheConfiguration c1 = new CacheConfiguration("c1");
        c1.setBackups(1);
        c1.setAffinity(new RendezvousAffinityFunction(false, 64));
        caches.add(c1);

        CacheConfiguration c2 = new CacheConfiguration("c2");
        c2.setBackups(2);
        c2.setAffinity(new RendezvousAffinityFunction(false, 128));
        caches.add(c2);

        CacheConfiguration c3 = new CacheConfiguration("c3");
        c3.setCacheMode(CacheMode.REPLICATED);
        c3.setAffinity(new RendezvousAffinityFunction(false, 256));
        caches.add(c3);

        ignite(0).createCaches(caches);

        expCaches.put("c1", new T3<>(64, 1, 2));
        expCaches.put("c2", new T3<>(128, 2, 2));
        expCaches.put("c3", new T3<>(256, 1, 2));

        checkResolverCachesInfo(ignite(0), expCaches);

        startGrid(2);
        startGrid(3);

        awaitPartitionMapExchange();

        expCaches.put("c2", new T3<>(128, 2, 3));
        expCaches.put("c3", new T3<>(256, 1, 4));

        checkResolverCachesInfo(ignite(0), expCaches);

        CacheConfiguration c4 = new CacheConfiguration("c4");
        c4.setCacheMode(CacheMode.PARTITIONED);
        c4.setBackups(0);
        c4.setAffinity(new RendezvousAffinityFunction(false, 256));
        c4.setNodeFilter(new TestCacheNodeExcludingFilter(getTestIgniteInstanceName(0), getTestIgniteInstanceName(1)));

        ignite(2).createCache(c4);

        expCaches.put("c4", new T3<>(256, 0, 1));

        checkResolverCachesInfo(ignite(0), expCaches);

        stopGrid(0); // Stop current coordinator, check new coordinator will initialize required caches information.

        awaitPartitionMapExchange();

        expCaches.put("c3", new T3<>(256, 1, 3));

        checkResolverCachesInfo(ignite(1), expCaches);

        startGrid(0);

        expCaches.put("c3", new T3<>(256, 1, 4));

        checkResolverCachesInfo(ignite(1), expCaches);

        stopGrid(1);

        expCaches.put("c3", new T3<>(256, 1, 3));

        checkResolverCachesInfo(ignite(3), expCaches);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCommunicationFailureResolve_CachesInfo2() throws Exception {
        testCommSpi = true;
        sesTimeout = 5000;

        final CacheInfoCommunicationFailureResolver rslvr = new CacheInfoCommunicationFailureResolver();

        commFailureRslvr = new IgniteOutClosure<CommunicationFailureResolver>() {
            @Override public CommunicationFailureResolver apply() {
                return rslvr;
            }
        };

        Ignite srv0 = startGrid(0);

        CacheConfiguration ccfg = new CacheConfiguration("c1");
        ccfg.setBackups(1);

        srv0.createCache(ccfg);

        // Block rebalance to make sure node0 will be the only owner.
        TestRecordingCommunicationSpi.spi(srv0).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message msg) {
                return msg instanceof GridDhtPartitionSupplyMessage &&
                    ((GridDhtPartitionSupplyMessage) msg).groupId() == CU.cacheId("c1");
            }
        });

        startGrid(1);

        U.sleep(1000);

        ZookeeperDiscoverySpi spi = spi(srv0);

        rslvr.latch = new CountDownLatch(1);

        ZkTestCommunicationSpi.testSpi(srv0).initCheckResult(2, 0);

        spi.resolveCommunicationFailure(spi.getRemoteNodes().iterator().next(), new Exception("test"));

        assertTrue(rslvr.latch.await(10, SECONDS));

        List<List<ClusterNode>> cacheOwners = rslvr.ownersMap.get("c1");

        ClusterNode node0 = srv0.cluster().localNode();

        for (int p = 0; p < RendezvousAffinityFunction.DFLT_PARTITION_COUNT; p++) {
            List<ClusterNode> owners = cacheOwners.get(p);

            assertEquals(1, owners.size());
            assertEquals(node0, owners.get(0));
        }

        TestRecordingCommunicationSpi.spi(srv0).stopBlock();

        awaitPartitionMapExchange();

        Map<String, T3<Integer, Integer, Integer>> expCaches = new HashMap<>();

        expCaches.put(DEFAULT_CACHE_NAME, new T3<>(RendezvousAffinityFunction.DFLT_PARTITION_COUNT, 0, 1));
        expCaches.put("c1", new T3<>(RendezvousAffinityFunction.DFLT_PARTITION_COUNT, 1, 2));

        checkResolverCachesInfo(srv0, expCaches);
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
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void testCommunicationFailureResolve_ConcurrentDiscoveyEvents() throws Exception {
        sesTimeout = 5000;

        commFailureRslvr = NoOpCommunicationFailureResolver.FACTORY;

        final int INITIAL_NODES = 5;

        startGridsMultiThreaded(INITIAL_NODES);

        final CyclicBarrier b = new CyclicBarrier(4);

        GridCompoundFuture<?, ?> fut = new GridCompoundFuture<>();

        final AtomicBoolean stop = new AtomicBoolean();

        fut.add((IgniteInternalFuture)GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                b.await();

                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                for (int i = 0; i < 10; i++) {
                    startGrid(i + INITIAL_NODES);

                    Thread.sleep(rnd.nextLong(1000) + 10);

                    if (stop.get())
                        break;
                }

                return null;
            }
        }, "test-node-start"));

        fut.add((IgniteInternalFuture)GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                b.await();

                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                while (!stop.get()) {
                    startGrid(100);

                    Thread.sleep(rnd.nextLong(1000) + 10);

                    stopGrid(100);

                    Thread.sleep(rnd.nextLong(1000) + 10);
                }

                return null;
            }
        }, "test-node-restart"));

        fut.add((IgniteInternalFuture)GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                b.await();

                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                int idx = 0;

                while (!stop.get()) {
                    CacheConfiguration ccfg = new CacheConfiguration("c-" + idx++);
                    ccfg.setBackups(rnd.nextInt(5));

                    ignite(rnd.nextInt(INITIAL_NODES)).createCache(ccfg);

                    Thread.sleep(rnd.nextLong(1000) + 10);

                    ignite(rnd.nextInt(INITIAL_NODES)).destroyCache(ccfg.getName());

                    Thread.sleep(rnd.nextLong(1000) + 10);
                }

                return null;
            }
        }, "test-create-cache"));

        fut.add((IgniteInternalFuture)GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                try {
                    b.await();

                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    for (int i = 0; i < 5; i++) {
                        info("resolveCommunicationFailure: " + i);

                        ZookeeperDiscoverySpi spi = spi(ignite(rnd.nextInt(INITIAL_NODES)));

                        spi.resolveCommunicationFailure(ignite(rnd.nextInt(INITIAL_NODES)).cluster().localNode(),
                            new Exception("test"));
                    }

                    return null;
                }
                finally {
                    stop.set(true);
                }
            }
        }, 5, "test-resolve-failure"));

        fut.markInitialized();

        fut.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCommunicationFailureResolve_ConcurrentMultinode() throws Exception {
        sesTimeout = 5000;

        commFailureRslvr = NoOpCommunicationFailureResolver.FACTORY;

        startGridsMultiThreaded(5);

        client = true;

        startGridsMultiThreaded(5, 5);

        final int NODES = 10;

        GridTestUtils.runMultiThreaded(new Callable<Void>() {
            @Override public Void call() throws Exception {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                for (int i = 0; i < 5; i++) {
                    info("resolveCommunicationFailure: " + i);

                    ZookeeperDiscoverySpi spi = spi(ignite(rnd.nextInt(NODES)));

                    spi.resolveCommunicationFailure(spi.getRemoteNodes().iterator().next(), new Exception("test"));
                }

                return null;
            }
        }, 30, "test-resolve-failure");
    }

    /**
     * @throws Exception If failed.
     */
    public void testConnectionCheck() throws Exception {
       final int NODES = 5;

        startGridsMultiThreaded(NODES);

       for (int i = 0; i < NODES; i++) {
           Ignite node = ignite(i);

           TcpCommunicationSpi spi = (TcpCommunicationSpi)node.configuration().getCommunicationSpi();

           List<ClusterNode> nodes = new ArrayList<>(node.cluster().nodes());

           BitSet res = spi.checkConnection(nodes).get();

           for (int j = 0; j < NODES; j++)
               assertTrue(res.get(j));
       }
    }

    /**
     * @throws Exception If failed.
     */
    public void testReconnectDisabled_ConnectionLost() throws Exception {
        clientReconnectDisabled = true;

        startGrid(0);

        sesTimeout = 3000;
        testSockNio = true;
        client = true;

        Ignite client = startGrid(1);

        final CountDownLatch latch = new CountDownLatch(1);

        client.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                latch.countDown();

                return false;
            }
        }, EventType.EVT_NODE_SEGMENTED);

        ZkTestClientCnxnSocketNIO nio = ZkTestClientCnxnSocketNIO.forNode(client);

        nio.closeSocket(true);

        try {
            waitNoAliveZkNodes(log,
                zkCluster.getConnectString(),
                Collections.singletonList(aliveZkNodePath(client)),
                10_000);
        }
        finally {
            nio.allowConnect();
        }

        assertTrue(latch.await(10, SECONDS));
    }

    /**
     * @throws Exception If failed.
     */
    public void testServersLeft_FailOnTimeout() throws Exception {
        startGrid(0);

        final int CLIENTS = 5;

        joinTimeout = 3000;

        clientMode(true);

        startGridsMultiThreaded(1, CLIENTS);

        waitForTopology(CLIENTS + 1);

        final CountDownLatch latch = new CountDownLatch(CLIENTS);

        for (int i = 0; i < CLIENTS; i++) {
            Ignite node = ignite(i + 1);

            node.events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    latch.countDown();

                    return false;
                }
            }, EventType.EVT_NODE_SEGMENTED);
        }

        stopGrid(getTestIgniteInstanceName(0), true, false);

        assertTrue(latch.await(10, SECONDS));

        evts.clear();
    }

    /**
     *
     */
    public void testStartNoServers_FailOnTimeout() {
        joinTimeout = 3000;

        clientMode(true);

        long start = System.currentTimeMillis();

        Throwable err = GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(0);

                return null;
            }
        }, IgniteCheckedException.class, null);

        assertTrue(System.currentTimeMillis() >= start + joinTimeout);

        IgniteSpiException spiErr = X.cause(err, IgniteSpiException.class);

        assertNotNull(spiErr);
        assertTrue(spiErr.getMessage().contains("Failed to connect to cluster within configured timeout"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartNoServer_WaitForServers1() throws Exception {
        startNoServer_WaitForServers(0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartNoServer_WaitForServers2() throws Exception {
        startNoServer_WaitForServers(10_000);
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
     * @throws Exception If failed.
     */
    public void testDisconnectOnServersLeft_1() throws Exception {
        disconnectOnServersLeft(1, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDisconnectOnServersLeft_2() throws Exception {
        disconnectOnServersLeft(5, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDisconnectOnServersLeft_3() throws Exception {
        disconnectOnServersLeft(1, 10);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDisconnectOnServersLeft_4() throws Exception {
        disconnectOnServersLeft(5, 10);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDisconnectOnServersLeft_5() throws Exception {
        joinTimeout = 10_000;

        disconnectOnServersLeft(5, 10);
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
     * @throws Exception If failed.
     */
    public void testReconnectServersRestart_1() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-8178");

        reconnectServersRestart(1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReconnectServersRestart_2() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-8178");

        reconnectServersRestart(3);
    }

    /**
     * A simple split-brain test, where cluster spliited on 2 parts of server nodes (2 and 3).
     * There is also client which sees both parts of splitted cluster.
     *
     * Result cluster should be: 3 server nodes + 1 client.
     *
     * @throws Exception If failed.
     */
    public void testSimpleSplitBrain() throws Exception {
        failCommSpi = true;

        startGridsMultiThreaded(5);

        client = true;

        startGridsMultiThreaded(5, 3);

        client = false;

        awaitPartitionMapExchange();

        List<ClusterNode> all = G.allGrids().stream()
            .map(g -> g.cluster().localNode())
            .collect(Collectors.toList());;

        List<ClusterNode> part1 = all.subList(0, 3);
        List<ClusterNode> part2 = all.subList(3, all.size());

        ConnectionsFailureMatrix matrix = ConnectionsFailureMatrix.buildFrom(part1, part2);

        ClusterNode lastClient = startGrid(8).cluster().localNode();

        awaitPartitionMapExchange();

        // Make last client connected to other nodes.
        for (ClusterNode node : all) {
            if (node.id().equals(lastClient.id()))
                continue;

            matrix.addConnection(lastClient, node);
            matrix.addConnection(node, lastClient);
        }

        PeerToPeerCommunicationFailureSpi.fail(matrix);

        waitForTopology(4);
    }

    /**
     * A simple not actual split-brain test, where some connections between server nodes are lost.
     * Server nodes: 5.
     * Client nodes: 5.
     * Lost connections between server nodes: 2.
     *
     * Result cluster should be: 3 server nodes + 5 clients.
     *
     * @throws Exception If failed.
     */
    public void testNotActualSplitBrain() throws Exception {
        failCommSpi = true;

        startGridsMultiThreaded(5);

        List<ClusterNode> srvNodes = G.allGrids().stream()
            .map(g -> g.cluster().localNode())
            .collect(Collectors.toList());

        Assert.assertEquals(5, srvNodes.size());

        client = true;

        startGridsMultiThreaded(5, 3);

        client = false;

        awaitPartitionMapExchange();

        ConnectionsFailureMatrix matrix = new ConnectionsFailureMatrix();

        List<ClusterNode> allNodes = G.allGrids().stream().map(g -> g.cluster().localNode()).collect(Collectors.toList());

        matrix.addAll(allNodes);

        // Remove 2 connections between server nodes.
        matrix.removeConnection(srvNodes.get(0), srvNodes.get(1));
        matrix.removeConnection(srvNodes.get(1), srvNodes.get(0));
        matrix.removeConnection(srvNodes.get(2), srvNodes.get(3));
        matrix.removeConnection(srvNodes.get(3), srvNodes.get(2));

        PeerToPeerCommunicationFailureSpi.fail(matrix);

        waitForTopology(8);
    }

    /**
     * Almost split-brain test, server nodes splitted on 2 parts and there are some connections between these 2 parts.
     * Server nodes: 5.
     * Client nodes: 5.
     * Splitted on: 3 servers + 2 clients and 3 servers + 2 clients.
     * Extra connections between server nodes: 3.
     *
     * Result cluster should be: 3 server nodes + 2 clients.
     *
     * @throws Exception If failed.
     */
    public void testAlmostSplitBrain() throws Exception {
        failCommSpi = true;

        startGridsMultiThreaded(6);

        List<ClusterNode> srvNodes = G.allGrids().stream()
            .map(g -> g.cluster().localNode())
            .collect(Collectors.toList());

        Assert.assertEquals(6, srvNodes.size());

        List<ClusterNode> srvPart1 = srvNodes.subList(0, 3);
        List<ClusterNode> srvPart2 = srvNodes.subList(3, srvNodes.size());

        client = true;

        startGridsMultiThreaded(6, 5);

        client = false;

        awaitPartitionMapExchange();

        List<ClusterNode> clientNodes = G.allGrids().stream()
            .map(g -> g.cluster().localNode())
            .filter(ClusterNode::isClient)
            .collect(Collectors.toList());

        Assert.assertEquals(5, clientNodes.size());

        List<ClusterNode> clientPart1 = clientNodes.subList(0, 2);
        List<ClusterNode> clientPart2 = clientNodes.subList(2, 4);

        List<ClusterNode> splittedPart1 = new ArrayList<>();
        splittedPart1.addAll(srvPart1);
        splittedPart1.addAll(clientPart1);

        List<ClusterNode> splittedPart2 = new ArrayList<>();
        splittedPart2.addAll(srvPart2);
        splittedPart2.addAll(clientPart2);

        ConnectionsFailureMatrix matrix = new ConnectionsFailureMatrix();

        matrix.addAll(splittedPart1);
        matrix.addAll(splittedPart2);

        matrix.addConnection(srvPart1.get(0), srvPart2.get(1));
        matrix.addConnection(srvPart2.get(1), srvPart1.get(0));

        matrix.addConnection(srvPart1.get(1), srvPart2.get(2));
        matrix.addConnection(srvPart2.get(2), srvPart1.get(1));

        matrix.addConnection(srvPart1.get(2), srvPart2.get(0));
        matrix.addConnection(srvPart2.get(0), srvPart1.get(2));

        PeerToPeerCommunicationFailureSpi.fail(matrix);

        waitForTopology(5);
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
     * @throws Exception If failed.
     */
    public void testReconnectServersRestart_3() throws Exception {
        startGrid(0);

        clientMode(true);

        startGridsMultiThreaded(10, 10);

        stopGrid(getTestIgniteInstanceName(0), true, false);

        final int srvIdx = ThreadLocalRandom.current().nextInt(10);

        final AtomicInteger idx = new AtomicInteger();

        info("Restart nodes.");

        // Test concurrent start when there are disconnected nodes from previous cluster.
        GridTestUtils.runMultiThreaded(new Callable<Void>() {
            @Override public Void call() throws Exception {
                int threadIdx = idx.getAndIncrement();

                clientModeThreadLocal(threadIdx == srvIdx || ThreadLocalRandom.current().nextBoolean());

                startGrid(threadIdx);

                return null;
            }
        }, 10, "start-node");

        waitForTopology(20);

        evts.clear();
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartNoZk() throws Exception {
        stopZkCluster();

        sesTimeout = 30_000;

        zkCluster = ZookeeperDiscoverySpiTestSuite2.createTestingCluster(3);

        try {
            final AtomicInteger idx = new AtomicInteger();

            IgniteInternalFuture fut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    startGrid(idx.getAndIncrement());

                    return null;
                }
            }, 5, "start-node");

            U.sleep(5000);

            assertFalse(fut.isDone());

            zkCluster.start();

            fut.get();

            waitForTopology(5);
        }
        finally {
            zkCluster.start();
        }
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

        /** */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public void resolve(CommunicationFailureContext ctx) {
            List<ClusterNode> nodes = ctx.topologySnapshot();

            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            int killNodes = rnd.nextInt(nodes.size() / 2);

            log.info("Resolver kills nodes [total=" + nodes.size() + ", kill=" + killNodes + ']');

            Set<Integer> idxs = new HashSet<>();

            while (idxs.size() < killNodes)
                idxs.add(rnd.nextInt(nodes.size()));

            for (int idx : idxs) {
                ClusterNode node = nodes.get(idx);

                log.info("Resolver kills node: " + node.id());

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
