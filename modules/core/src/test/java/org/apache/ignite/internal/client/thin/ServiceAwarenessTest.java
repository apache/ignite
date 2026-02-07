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

package org.apache.ignite.internal.client.thin;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.client.ClientAuthenticationException;
import org.apache.ignite.client.ClientClusterGroup;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.ClientServices;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridJobExecuteRequest;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.client.thin.io.ClientConnectionMultiplexer;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.service.GridServiceProxy;
import org.apache.ignite.internal.processors.service.ServiceClusterDeploymentResultBatch;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.apache.logging.log4j.Level;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.runMultiThreadedAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Checks the service awareness feature of the thin client.
 */
public class ServiceAwarenessTest extends AbstractThinClientTest {
    /** */
    private static final String ATTR_NODE_IDX = "test.node.idx";

    /** Node-filter service name. */
    private static final String SRV_NAME = "node_filtered_svc";

    /** Number of grids at the test start. */
    private static final int BASE_NODES_CNT = 4;

    /** */
    private static final int TOP_UPD_NODES_CNT = 3;

    /** Number of node instances with the initial service deployment. */
    private static final int INIT_SRVC_NODES_CNT = 2;

    /** */
    protected boolean partitionAwareness = true;

    /** */
    private static ListeningTestLogger clientLogLsnr;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDiscoverySpi(new TestBlockingDiscoverySpi());
        cfg.setUserAttributes(Collections.singletonMap(ATTR_NODE_IDX, getTestIgniteInstanceIndex(igniteInstanceName)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected ClientConfiguration getClientConfiguration() {
        ClientConfiguration ccfg = super.getClientConfiguration();

        ccfg.setLogger(clientLogLsnr);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        clientLogLsnr = new ListeningTestLogger(log);
    }

    /** */
    private static ServiceConfiguration serviceCfg() {
        // Service is deployed on nodes with the name index equal to 1, 2 or >= GRIDS.
        return new ServiceConfiguration()
            .setName(SRV_NAME)
            .setService(new ServicesTest.TestService())
            .setMaxPerNodeCount(1)
            .setNodeFilter(new TestNodeFilter());
    }

    /** {@inheritDoc} */
    @Override protected boolean isClientPartitionAwarenessEnabled() {
        return partitionAwareness;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        ((GridTestLog4jLogger)log).setLevel(Level.INFO);

        stopAllGrids();

        partitionAwareness = true;

        clientLogLsnr.clearListeners();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrids(BASE_NODES_CNT);

        grid(1).services().deploy(serviceCfg());
    }

    /** */
    @Test
    public void testDelayedServiceRedeploy() throws Exception {
        TestBlockingDiscoverySpi testDisco = ((TestBlockingDiscoverySpi)grid(0).configuration().getDiscoverySpi());

        // Service topology on the client.
        Set<UUID> srvcTopOnClient = new GridConcurrentHashSet<>();

        registerServiceTopologyUpdateListener(srvcTopOnClient::addAll);

        AtomicBoolean svcRunFlag = new AtomicBoolean(true);

        try (IgniteClient client = startClient()) {
            ServicesTest.TestServiceInterface svc = client.services().serviceProxy(SRV_NAME, ServicesTest.TestServiceInterface.class);

            runAsync(() -> {
                while (svcRunFlag.get())
                    svc.testMethod();
            });

            waitForCondition(() -> srvcTopOnClient.size() == INIT_SRVC_NODES_CNT
                && srvcTopOnClient.contains(grid(1).localNode().id())
                && srvcTopOnClient.contains(grid(2).localNode().id()),
                getTestTimeout());

            // Delays service redeployment and the service topology update on the server side.
            testDisco.toBlock.add(ServiceClusterDeploymentResultBatch.class);

            startGrid(BASE_NODES_CNT);

            waitForCondition(() -> testDisco.blocked.size() == 1, getTestTimeout());

            // Ensure all the nodes have started but the service topology hasn't updated yet.
            for (Ignite ig : G.allGrids()) {
                assertEquals(ig.cluster().nodes().size(), BASE_NODES_CNT + 1);

                // Ensure there are still SRVC_FILTERED_NOIDES_CNT nodes with the service instance.
                assertEquals(((IgniteEx)ig).context().service().serviceTopology(SRV_NAME, 0).size(),
                    INIT_SRVC_NODES_CNT);
            }

            // Ensure the client's topology is not updated.
            assertTrue(srvcTopOnClient.size() == INIT_SRVC_NODES_CNT
                && !srvcTopOnClient.contains(grid(BASE_NODES_CNT).localNode().id()));

            testDisco.release();

            // Ensure the service topology has been updated to 3 instances per cluster.
            for (Ignite ig : G.allGrids()) {
                waitForCondition(
                    () -> {
                        try {
                            return ((IgniteEx)ig).context().service().serviceTopology(SRV_NAME, 0).size() == 3;
                        }
                        catch (Exception e) {
                            return false;
                        }
                    },
                    getTestTimeout()
                );
            }

            waitForCondition(() -> srvcTopOnClient.size() == 3 && srvcTopOnClient.contains(grid(1).localNode().id())
                && srvcTopOnClient.contains(grid(2).localNode().id())
                && srvcTopOnClient.contains(grid(BASE_NODES_CNT).localNode().id()), getTestTimeout());
        }
        finally {
            svcRunFlag.set(false);
        }
    }

    /**
     * Tests several nodes come while one thread is used to call the service.
     */
    @Test
    public void testNodesJoinSingleThreaded() throws Exception {
        doTestClusterTopChangesWhileServiceCalling(false, 1);
    }

    /**
     * Tests several nodes come while several threads are used to call the service.
     */
    @Test
    public void testNodesJoinMultiThreaded() throws Exception {
        doTestClusterTopChangesWhileServiceCalling(false, 4);
    }

    /**
     * Tests several nodes leaves while one thread is used to call the service.
     */
    @Test
    public void testNodesLeaveSingleThreaded() throws Exception {
        doTestClusterTopChangesWhileServiceCalling(true, 1);
    }

    /**
     * Tests several nodes leave while several threads are used to call the service.
     */
    @Test
    public void testNodesLeaveMultiThreaded() throws Exception {
        doTestClusterTopChangesWhileServiceCalling(true, 4);
    }

    /**
     * Tests change of the minor cluster topology version doesn't trigger the service topology update.
     */
    @Test
    public void testMinorTopologyVersionDoesntAffect() throws Exception {
        try (IgniteClient client = startClient()) {
            ServicesTest.TestServiceInterface svc = client.services().serviceProxy(SRV_NAME, ServicesTest.TestServiceInterface.class);

            Set<UUID> srvcTopOnClient = new GridConcurrentHashSet<>();

            registerServiceTopologyUpdateListener(srvcTopOnClient::addAll);

            ((GridTestLog4jLogger)log).setLevel(Level.DEBUG);

            while (srvcTopOnClient.isEmpty())
                svc.testMethod();

            // Last time ot the topology update.
            long time = System.nanoTime();

            srvcTopOnClient.clear();

            AffinityTopologyVersion prevTopVer = grid(0).context().discovery().topologyVersionEx();

            grid(0).createCache("testCache");

            awaitPartitionMapExchange();

            AffinityTopologyVersion newTopVer = grid(0).context().discovery().topologyVersionEx();

            assertTrue(newTopVer.topologyVersion() == prevTopVer.topologyVersion()
                && newTopVer.minorTopologyVersion() > prevTopVer.minorTopologyVersion());

            while (srvcTopOnClient.isEmpty())
                svc.testMethod();

            // Update only by the timeout.
            assertTrue(U.nanosToMillis(System.nanoTime() - time) > ClientServicesImpl.SRV_TOP_UPDATE_PERIOD / 2);
        }
    }

    /**
     * Tests the service topology update with a gap of service invocation during forced service redeployment.
     */
    @Test
    public void testForcedServiceRedeployWhileClientIsIdle() throws Exception {
        try (IgniteClient client = startClient()) {
            ServicesTest.TestServiceInterface svc = client.services().serviceProxy(SRV_NAME, ServicesTest.TestServiceInterface.class);

            Set<UUID> srvcTopOnClient = new GridConcurrentHashSet<>();

            registerServiceTopologyUpdateListener(srvcTopOnClient::addAll);

            ((GridTestLog4jLogger)log).setLevel(Level.DEBUG);

            assertTrue(waitForCondition(() -> {
                svc.testMethod();

                return !srvcTopOnClient.isEmpty();
            }, ClientServicesImpl.SRV_TOP_UPDATE_PERIOD));

            ((GridTestLog4jLogger)log).setLevel(Level.INFO);

            assertTrue(srvcTopOnClient.size() == INIT_SRVC_NODES_CNT
                && srvcTopOnClient.contains(grid(1).localNode().id())
                && srvcTopOnClient.contains(grid(2).localNode().id()));

            long prevTopVer = grid(0).context().discovery().topologyVersion();

            grid(1).services().cancel(SRV_NAME);

            waitForCondition(() -> grid(0).services().serviceDescriptors().isEmpty(), getTestTimeout());

            srvcTopOnClient.clear();

            grid(1).services().deploy(serviceCfg().setNodeFilter(null));

            waitForCondition(() -> !grid(0).services().serviceDescriptors().isEmpty(), getTestTimeout());

            assertEquals(prevTopVer, grid(0).context().discovery().topologyVersion());

            ((GridTestLog4jLogger)log).setLevel(Level.DEBUG);

            waitForCondition(() -> {
                svc.testMethod();

                return srvcTopOnClient.size() == BASE_NODES_CNT;
            }, getTestTimeout());

            for (Ignite ig : G.allGrids())
                assertTrue(srvcTopOnClient.contains(ig.cluster().localNode().id()));
        }
    }

    /** */
    private void doTestClusterTopChangesWhileServiceCalling(boolean shrinkTop, int svcInvokeThreads) throws Exception {
        // Start additional nodes to stop them.
        if (shrinkTop) {
            for (int nodeIdx = BASE_NODES_CNT; nodeIdx < BASE_NODES_CNT + TOP_UPD_NODES_CNT; nodeIdx++)
                startGrid(nodeIdx);
        }

        Set<UUID> expInitSvcTop = resolveServiceTopology();

        assertEquals(shrinkTop ? 5 : 2, expInitSvcTop.size());

        // Last detected service topology on the client side.
        AtomicReference<Set<UUID>> svcTop = new AtomicReference<>();

        registerServiceTopologyUpdateListener(svcTop::set);

        AtomicBoolean stopFlag = new AtomicBoolean();

        try (IgniteClient client = startClient()) {
            ServicesTest.TestServiceInterface svc = client.services().serviceProxy(SRV_NAME, ServicesTest.TestServiceInterface.class);

            ((GridTestLog4jLogger)log).setLevel(Level.DEBUG);

            IgniteInternalFuture<?> svcInvokeFut = runMultiThreadedAsync(
                () -> {
                    do {
                        try {
                            svc.testMethod();
                        }
                        catch (Exception e) {
                            String errMsg = e.getMessage();

                            // TODO: IGNITE-20802 : Exception should not occur.
                            // Client doesn't retry service invocation if the redirected-to service instance node leaves cluster.
                            boolean isErrCausedByNodeLeave = errMsg.contains("Failed to execute task due to grid shutdown")
                                || (errMsg.contains("Node has left grid") || errMsg.contains("Failed to send job due to node failure"))
                                && expInitSvcTop.stream().anyMatch(id -> errMsg.contains(id.toString()));

                            assertTrue(shrinkTop && isErrCausedByNodeLeave);
                        }
                    }
                    while (!stopFlag.get());
                },
                svcInvokeThreads,
                "ServiceTestLoader"
            );

            assertTrue(waitForCondition(() -> expInitSvcTop.equals(svcTop.get()), getTestTimeout()));

            Collection<IgniteInternalFuture<?>> topUpdFuts = ConcurrentHashMap.newKeySet();

            for (int i = 0; i < TOP_UPD_NODES_CNT; ++i) {
                int nodeIdx = BASE_NODES_CNT + i;

                topUpdFuts.add(runAsync(() -> {
                    if (shrinkTop)
                        stopGrid(nodeIdx);
                    else
                        startGrid(nodeIdx);
                }));
            }

            for (IgniteInternalFuture<?> topUpdFut : topUpdFuts)
                topUpdFut.get(getTestTimeout());

            Set<UUID> expUpdSvcTop = resolveServiceTopology();

            assertEquals(shrinkTop ? 2 : 5, expUpdSvcTop.size());

            assertTrue(waitForCondition(() -> expUpdSvcTop.equals(svcTop.get()), getTestTimeout()));

            stopFlag.set(true);

            svcInvokeFut.get(getTestTimeout());
        }
        finally {
            stopFlag.set(true);
        }
    }

    /**
     * Tests that the client invokes only the proper nodes when partitionAwareness is enabled and no
     * {@link ClientClusterGroup} is set.
     */
    @Test
    public void testWithNoSubCluster() {
        doTestServiceAwarenessForClusterGroup(null);
    }

    /**
     * Tests that the client invokes only the proper node if partitionAwareness is enabled and just one correct server
     * is passed as {@link ClientClusterGroup}.
     */
    @Test
    public void testWithOneCorrectServer() {
        doTestServiceAwarenessForClusterGroup(Collections.singletonList(grid(1).localNode().id()));
    }

    /**
     * Tests that the client invokes only the proper nodes if partitionAwareness is enabled and just a couple of correct
     * servers are passed as {@link ClientClusterGroup} to invoke the service on.
     */
    @Test
    public void testWithTwoCorrectServers() {
        doTestServiceAwarenessForClusterGroup(Arrays.asList(grid(1).localNode().id(), grid(2).localNode().id()));
    }

    /**
     * Tests that the client invokes only the proper node if partitionAwareness is enabled and one correct
     * server and one incorrect server (having no service instance) are passed as {@link ClientClusterGroup} to invoke
     * the service on.
     */
    @Test
    public void testWithOneCorrectOneIncorrectServers() {
        doTestServiceAwarenessForClusterGroup(Arrays.asList(grid(0).localNode().id(), grid(2).localNode().id()));
    }

    /**
     * Tests that the client invokes only the proper node if partitionAwareness is enabled and only incorrect
     * server (having no service instance) are passed as {@link ClientClusterGroup} to invoke the service on.
     */
    @Test
    public void testWithIncorrectServer() {
        doTestServiceAwarenessForClusterGroup(Collections.singletonList(grid(0).localNode().id()));
    }

    /** */
    private void doTestServiceAwarenessForClusterGroup(@Nullable Collection<UUID> grp) {
        // Counters of the invocation redirects.
        AtomicInteger redirectCnt = new AtomicInteger();

        // Service topology received by the client.
        Set<UUID> top = new GridConcurrentHashSet<>();

        // Requested server nodes with a service invocation.
        Collection<UUID> requestedServers = new GridConcurrentHashSet<>();

        // All or properly filtered nodes with the service instances.
        Set<UUID> filteredGrp = F.isEmpty(grp)
            ? top
            : grp.stream().filter(nid -> new TestNodeFilter().apply(grid(0).cluster().node(nid))).collect(Collectors.toSet());

        registerServiceTopologyUpdateListener(uuids -> {
            // Reset counters on the first topology update.
            if (top.isEmpty())
                redirectCnt.set(0);

            top.addAll(uuids);
        });

        // Listener of the service remote call (the redirection).
        G.allGrids().forEach(g -> ((IgniteEx)g).context().io().addMessageListener(GridTopic.TOPIC_JOB, new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                if (msg instanceof GridJobExecuteRequest
                    && ((GridJobExecuteRequest)msg).taskClassName().contains(GridServiceProxy.class.getName()))
                    redirectCnt.incrementAndGet();
            }
        }));

        partitionAwareness = false;

        ((GridTestLog4jLogger)log).setLevel(Level.DEBUG);

        callService(requestedServers, grp, filteredGrp, null);

        // Check no service awareness: continous redirections.
        assertEquals(F.isEmpty(filteredGrp) && !F.isEmpty(grp) ? 0 : 100, redirectCnt.get());

        // Ensure that client received no service topology update.
        assertTrue(top.isEmpty());

        assertTrue(requestedServers.size() == 1 && requestedServers.contains(grid(0).localNode().id()));

        partitionAwareness = true;

        callService(requestedServers, grp, filteredGrp, svc -> {
            // We assume that the topology will be received and used for the further requests.
            redirectCnt.set(0);
            requestedServers.clear();

            for (int i = 0; i < 1000; ++i)
                svc.testMethod();
        });

        // Ensure that only the target nodes were requested after the topology getting.
        if (!F.isEmpty(filteredGrp))
            assertEquals(filteredGrp, requestedServers);

        // Check the received topology.
        assertFalse(top.retainAll(filteredGrp));

        // Ensure there were no redirected sertvic calls any more.
        assertEquals(0, redirectCnt.get());
    }

    /**
     * Invokes the test service 100 times. If required, stores requestes server nodes. Excepts an exception if the
     * target nodes have no service instance.
     *
     * @param requestedEndpoints If not {@code null}, is filled with actually requested nodes.
     * @param svcGrp If not {@code null}, is used to pass as a nodes group to call the service on it.
     * @param filteredSvcGrp If not {@code null}, actual nodes group with the service instances.
     * @param afterCallAction If not {@code null}, is invoked after the service calls.
     */
    private void callService(
        @Nullable Collection<UUID> requestedEndpoints,
        @Nullable Collection<UUID> svcGrp,
        @Nullable Collection<UUID> filteredSvcGrp,
        @Nullable Consumer<ServicesTest.TestServiceInterface> afterCallAction
    ) {
        try (IgniteClient client = startClient(requestedEndpoints)) {
            ClientServices clientServices = F.isEmpty(svcGrp)
                ? client.services()
                : client.services(client.cluster().forNodeIds(svcGrp));

            ServicesTest.TestServiceInterface svc = clientServices.serviceProxy(SRV_NAME, ServicesTest.TestServiceInterface.class);

            if (F.isEmpty(filteredSvcGrp) && !F.isEmpty(svcGrp))
                assertThrows(null, () -> svc.testMethod(), ClientException.class, "Failed to find deployed service:");
            else {
                for (int i = 0; i < 100; ++i)
                    svc.testMethod();

                if (afterCallAction != null)
                    afterCallAction.accept(svc);
            }
        }
    }

    /** Extracts ids of received service instance nodes from the client log. */
    private static void registerServiceTopologyUpdateListener(Consumer<Set<UUID>> srvTopConsumer) {
        clientLogLsnr.registerListener(s -> {
            if (s.contains("Topology of service '" + SRV_NAME + "' has been updated. The service instance nodes: ")) {
                String nodes = s.substring(s.lastIndexOf(": [") + 3, s.length() - 2);

                srvTopConsumer.accept(Arrays.stream(nodes.split(", ")).map(UUID::fromString).collect(Collectors.toSet()));
            }
        });
    }

    /** */
    private IgniteClient startClient() {
        return new TcpIgniteClient((cfg, hnd) -> new TestTcpChannel(cfg, hnd, null),
            getClientConfiguration(grid(0)));
    }

    /** */
    private IgniteClient startClient(@Nullable Collection<UUID> requestedServerNodes) {
        return new TcpIgniteClient((cfg, hnd) -> new TestTcpChannel(cfg, hnd, requestedServerNodes),
            getClientConfiguration(grid(0)));
    }

    /** */
    private static Set<UUID> resolveServiceTopology() {
        return G.allGrids().stream()
            .map(g -> g.cluster().localNode())
            .filter(TestNodeFilter::test)
            .map(ClusterNode::id)
            .collect(Collectors.toSet());
    }

    /**
     * Accepts nodes with the name index equal to 1, 2 or >= GRIDS.
     */
    private static final class TestNodeFilter implements IgnitePredicate<ClusterNode> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            return test(node);
        }

        /** */
        static boolean test(ClusterNode node) {
            int nodeIdx = node.attribute(ATTR_NODE_IDX);

            return nodeIdx == 1 || nodeIdx == 2 || nodeIdx >= BASE_NODES_CNT;
        }
    }

    /** */
    private static final class TestBlockingDiscoverySpi extends TcpDiscoverySpi {
        /** */
        private final Set<Class<? extends DiscoveryCustomMessage>> toBlock = new HashSet<>();

        /** */
        private final List<DiscoveryCustomMessage> blocked = new CopyOnWriteArrayList<>();

        /** {@inheritDoc} */
        @Override public void sendCustomEvent(DiscoveryCustomMessage msg) throws IgniteException {
            DiscoveryCustomMessage realMsg = GridTestUtils.unwrap(msg);

            if (toBlock.stream().anyMatch(mt -> mt.isAssignableFrom(realMsg.getClass()))) {
                blocked.add(msg);

                return;
            }

            super.sendCustomEvent(msg);
        }

        /** */
        public void release() {
            toBlock.clear();

            blocked.forEach(this::sendCustomEvent);
        }
    }

    /**
     * A client connection channel able to register the server nodes requested to call a service.
     */
    private static final class TestTcpChannel extends TcpClientChannel {
        /** */
        private final @Nullable Collection<UUID> requestedServerNodes;

        /** Ctor. */
        private TestTcpChannel(
            ClientChannelConfiguration cfg,
            ClientConnectionMultiplexer connMgr,
            @Nullable Collection<UUID> requestedServerNodes)
            throws ClientConnectionException, ClientAuthenticationException, ClientProtocolError {
            super(cfg, connMgr);

            this.requestedServerNodes = requestedServerNodes;
        }

        /** {@inheritDoc} */
        @Override public <T> T service(ClientOperation op, Consumer<PayloadOutputChannel> payloadWriter,
            Function<PayloadInputChannel, T> payloadReader) throws ClientException {
            UUID srvNodeId = serverNodeId();

            if (op == ClientOperation.SERVICE_INVOKE && requestedServerNodes != null && srvNodeId != null)
                requestedServerNodes.add(srvNodeId);

            return super.service(op, payloadWriter, payloadReader);
        }
    }
}
