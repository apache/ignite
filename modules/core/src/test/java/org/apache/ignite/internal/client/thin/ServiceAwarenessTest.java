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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridJobExecuteRequest;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.discovery.CustomMessageWrapper;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.service.GridServiceProxy;
import org.apache.ignite.internal.processors.service.ServiceClusterDeploymentResultBatch;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.apache.logging.log4j.Level;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.runMultiThreaded;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Checks the service awareness feature of the thin client.
 */
public class ServiceAwarenessTest extends AbstractThinClientTest {
    /** Node-filter service name. */
    private static final String SRV_NAME = "node_filtered_svc";

    /** Number of grids at the test start. */
    private static final int GRIDS = 4;

    /** Number of node instances with the initial service deployment. */
    private static final int FILTERED_NODES_CNT = 2;

    /** */
    protected boolean partitionAwareness = true;

    /** */
    private static ListeningTestLogger clientLogLsnr;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDiscoverySpi(new TestBlockingDiscoverySpi());

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

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 30_000L;
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

        startGrids(GRIDS);

        grid(1).services().deploy(serviceCfg());
    }

    /** */
    @Test
    public void testDelayedServiceRedeploy() throws Exception {
        TestBlockingDiscoverySpi testDisco = ((TestBlockingDiscoverySpi)grid(0).configuration().getDiscoverySpi());

        // Service topology on the client.
        Set<UUID> srvcTopOnClient = new GridConcurrentHashSet<>();

        addClientLogLsnr(srvcTopOnClient::addAll);

        AtomicBoolean svcRunFlag = new AtomicBoolean(true);

        try (IgniteClient client = startClient(0)) {
            ServicesTest.TestServiceInterface svc = client.services().serviceProxy(SRV_NAME, ServicesTest.TestServiceInterface.class);

            runAsync(() -> {
                while (svcRunFlag.get())
                    svc.testMethod();
            });

            waitForCondition(() -> srvcTopOnClient.size() == FILTERED_NODES_CNT
                && srvcTopOnClient.contains(grid(1).localNode().id())
                && srvcTopOnClient.contains(grid(2).localNode().id()),
                getTestTimeout());

            // Delays service redeployment and the service topology update on the server side.
            testDisco.toBlock.add(ServiceClusterDeploymentResultBatch.class);

            startGrid(GRIDS);

            waitForCondition(() -> testDisco.blocked.size() == 1, getTestTimeout());

            // Ensure all the nodes have started but the service topology hasn't updated yet.
            for (Ignite ig : G.allGrids()) {
                assertEquals(ig.cluster().nodes().size(), GRIDS + 1);

                // Ensure there are still SRVC_FILTERED_NOIDES_CNT nodes with the service instance.
                assertEquals(((IgniteEx)ig).context().service().serviceTopology(SRV_NAME, 0).size(),
                    FILTERED_NODES_CNT);
            }

            // Ensure the client's topology is not updated.
            assertTrue(srvcTopOnClient.size() == FILTERED_NODES_CNT
                && !srvcTopOnClient.contains(grid(GRIDS).localNode().id()));

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
                && srvcTopOnClient.contains(grid(GRIDS).localNode().id()), getTestTimeout());
        }
        finally {
            svcRunFlag.set(false);
        }
    }

    /**
     * Tests several nodes come while one thread is used to call the service.
     */
    @Test
    public void testNodesComeOneThread() throws Exception {
        doTestClusterTopChangesWhileServiceCalling(3, true, false);
    }

    /**
     * Tests several nodes come while several threads are used to call the service.
     */
    @Test
    public void testNodesComeMultiThreads() throws Exception {
        doTestClusterTopChangesWhileServiceCalling(3, true, true);
    }

    /**
     * Tests several nodes leaves while one thread is used to call the service.
     */
    @Test
    public void testNodesLeaveOneThread() throws Exception {
        doTestClusterTopChangesWhileServiceCalling(3, false, false);
    }

    /**
     * Tests several nodes leave while several threads are used to call the service.
     */
    @Test
    public void testNodesLeaveMultiThreads() throws Exception {
        doTestClusterTopChangesWhileServiceCalling(3, false, true);
    }

    /**
     * Tests service topology is updated when there is a gap of service invocation between forced service redeployment.
     */
    @Test
    public void testForcedServiceRedeployWhileClientIsIdle() {
        try (IgniteClient client = startClient(0)) {
            ServicesTest.TestServiceInterface svc = client.services().serviceProxy(SRV_NAME, ServicesTest.TestServiceInterface.class);

            Set<UUID> srvcTopOnClient = new GridConcurrentHashSet<>();

            addClientLogLsnr(srvcTopOnClient::addAll);

            ((GridTestLog4jLogger)log).setLevel(Level.DEBUG);

            while (srvcTopOnClient.isEmpty())
                svc.testMethod();

            ((GridTestLog4jLogger)log).setLevel(Level.INFO);

            assertTrue(srvcTopOnClient.size() == FILTERED_NODES_CNT
                && srvcTopOnClient.contains(grid(1).localNode().id())
                && srvcTopOnClient.contains(grid(2).localNode().id()));

            grid(1).services().cancel(SRV_NAME);

            srvcTopOnClient.clear();

            grid(1).services().deploy(serviceCfg().setNodeFilter(null));

            ((GridTestLog4jLogger)log).setLevel(Level.DEBUG);

            while (srvcTopOnClient.isEmpty())
                svc.testMethod();

            assertEquals(GRIDS, srvcTopOnClient.size());

            for (Ignite ig : G.allGrids())
                assertTrue(srvcTopOnClient.contains(ig.cluster().localNode().id()));
        }
    }

    /** */
    private void doTestClusterTopChangesWhileServiceCalling(int nodesCnt, boolean addNodes, boolean multiThreaded) throws Exception {
        Set<UUID> newNodesUUIDs = new GridConcurrentHashSet<>();

        // Start additional nodes to stop them.
        if (!addNodes) {
            startGridsMultiThreaded(GRIDS, nodesCnt);

            for (int i = GRIDS; i < GRIDS + nodesCnt; ++i)
                newNodesUUIDs.add(grid(i).localNode().id());
        }

        // Service topology on the clients.
        Set<UUID> srvcTopOnClient = new GridConcurrentHashSet<>();

        addClientLogLsnr(srvcTopOnClient::addAll);

        AtomicBoolean changeClusterTop = new AtomicBoolean();
        AtomicBoolean stopFlag = new AtomicBoolean();

        try (IgniteClient client = startClient(0)) {
            ServicesTest.TestServiceInterface svc = client.services().serviceProxy(SRV_NAME, ServicesTest.TestServiceInterface.class);

            ((GridTestLog4jLogger)log).setLevel(Level.DEBUG);

            runMultiThreaded(() -> {
                do {
                    try {
                        svc.testMethod();
                    }
                    catch (ClientException e) {
                        String m = e.getMessage();

                        // Until the service topology is not updated yet, service invoke request can be redirected to
                        // node which has just left the cluster. This case raises a node-left exception in the service
                        // call response. Unfortunately, this exception is not processed by the client service proxy as
                        // a resend attempts.
                        if (addNodes || (!m.contains("Node has left grid") && !m.contains("Failed to send job due to node failure"))
                            || newNodesUUIDs.stream().noneMatch(nid -> m.contains(nid.toString())))
                            throw e;
                    }

                    // Wait until the initial topology is received.
                    if (srvcTopOnClient.size() == (addNodes ? FILTERED_NODES_CNT : FILTERED_NODES_CNT + nodesCnt)
                        && changeClusterTop.compareAndSet(false, true)) {
                        srvcTopOnClient.clear();

                        for (int i = 0; i < nodesCnt; ++i) {
                            int nodeIdx = GRIDS + i;

                            runAsync(() -> {
                                try {
                                    if (addNodes)
                                        newNodesUUIDs.add(startGrid(nodeIdx).localNode().id());
                                    else
                                        stopGrid(nodeIdx);
                                }
                                catch (Exception e) {
                                    log.error("Unable to start or stop test grid.", e);

                                    stopFlag.set(true);
                                }
                            });
                        }
                    }

                    // Stop if new excepted service topology received.
                    if (srvcTopOnClient.size() == (addNodes ? FILTERED_NODES_CNT + nodesCnt : FILTERED_NODES_CNT))
                        stopFlag.set(true);
                }
                while (!stopFlag.get());
            }, multiThreaded ? 4 : 1, "ServiceTestLoader");
        }

        // The initial nodes must always persist it the service topology.
        assertTrue(srvcTopOnClient.contains(grid(1).localNode().id())
            && srvcTopOnClient.contains(grid(2).localNode().id()));

        assertEquals(addNodes ? nodesCnt : 0, newNodesUUIDs.stream().filter(srvcTopOnClient::contains).count());
    }

    /**
     * Tests client uses service awareness when partitionAwareness is enabled.
     */
    @Test
    public void testServiceAwarenessEnabled() {
        AtomicInteger redirectCnt = new AtomicInteger();

        G.allGrids().forEach(g -> ((IgniteEx)g).context().io().addMessageListener(GridTopic.TOPIC_JOB, new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                if (msg instanceof GridJobExecuteRequest
                    && ((GridJobExecuteRequest)msg).getTaskClassName().contains(GridServiceProxy.class.getName()))
                    redirectCnt.incrementAndGet();
            }
        }));

        partitionAwareness = false;

        callServiceNTimesFromClient(SRV_NAME, null, () -> redirectCnt.get() >= 100);

        // Check no service awareness: continous redirections.
        assertEquals(100, redirectCnt.get());

        partitionAwareness = true;

        // Received service topology.
        AtomicReference<Collection<UUID>> top = new AtomicReference<>();

        AtomicInteger callCounter = new AtomicInteger();

        addClientLogLsnr(nodes -> {
            // First top update.
            if (top.get() == null) {
                redirectCnt.set(0);
                callCounter.set(0);
            }

            top.set(nodes);
        });

        callServiceNTimesFromClient(SRV_NAME, callCounter, () -> top.get() != null && callCounter.get() >= 1000);

        assertTrue(top.get().size() == FILTERED_NODES_CNT && top.get().contains(grid(1).localNode().id())
            && top.get().contains(grid(2).localNode().id()));

        assertTrue(redirectCnt.get() < 50);
    }

    /** */
    private void addClientLogLsnr(Consumer<Set<UUID>> srvTopConsumer) {
        clientLogLsnr.registerListener(s -> {
            if (s.contains("Topology of service '" + SRV_NAME + "' has been updated: ")) {
                String nodes = s.substring(s.indexOf(": [") + 3);

                nodes = nodes.substring(0, nodes.length() - 1);

                srvTopConsumer.accept(Arrays.stream(nodes.split(", ")).map(UUID::fromString).collect(Collectors.toSet()));
            }
        });
    }

    /** */
    private void callServiceNTimesFromClient(String srvcName, @Nullable AtomicInteger callCounter, Supplier<Boolean> stop) {
        ((GridTestLog4jLogger)log).setLevel(Level.DEBUG);

        try (IgniteClient client = startClient(0)) {
            ServicesTest.TestServiceInterface svc = client.services().serviceProxy(srvcName, ServicesTest.TestServiceInterface.class);

            while (!stop.get()) {
                svc.testMethod();

                if (callCounter != null)
                    callCounter.incrementAndGet();
            }
        }
        finally {
            ((GridTestLog4jLogger)log).setLevel(Level.INFO);
        }
    }

    /**
     * Accepts nodes with the name index equal to 1, 2 or >= GRIDS.
     */
    private static final class TestNodeFilter implements IgnitePredicate<ClusterNode> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            String nodeName = node.attribute("org.apache.ignite.ignite.name");

            if (F.isEmpty(nodeName))
                return false;

            int nodeIdx = -1;

            try {
                nodeIdx = Integer.parseInt(nodeName.substring(nodeName.length() - 1));
            }
            catch (Exception e) {
                // No-op.
            }

            return nodeIdx == 1 || nodeIdx == 2 || nodeIdx >= GRIDS;
        }
    }

    /** */
    private static final class TestBlockingDiscoverySpi extends TcpDiscoverySpi {
        /** */
        private final Set<Class<? extends DiscoveryCustomMessage>> toBlock = new HashSet<>();

        /** */
        private final List<CustomMessageWrapper> blocked = new CopyOnWriteArrayList<>();

        /** {@inheritDoc} */
        @Override public void sendCustomEvent(DiscoverySpiCustomMessage msg) throws IgniteException {
            if (msg instanceof CustomMessageWrapper
                && toBlock.stream().anyMatch(mt -> mt.isAssignableFrom(((CustomMessageWrapper)msg).delegate().getClass()))) {
                blocked.add((CustomMessageWrapper)msg);

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
}
