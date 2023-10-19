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

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import com.mchange.v2.c3p0.util.TestUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridJobExecuteRequest;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.service.GridServiceProxy;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.apache.logging.log4j.Level;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.util.IgniteUtils.TEST_FLAG;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.runMultiThreaded;

/**
 * Checks service invocation for thin client if service awareness is disabled or enabled.
 */
public class ServiceAwarenessTest extends AbstractThinClientTest {
    /** Node-filter service name. */
    private static final String SRV_NAME = "node_filtered_svc";

    /** */
    private static final int GRIDS = 4;

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

        stopAllGrids();

        ((GridTestLog4jLogger)log).setLevel(Level.INFO);

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
        TestBlockingDiscoverySpi testDisco =((TestBlockingDiscoverySpi)grid(0).configuration().getDiscoverySpi());

        TEST_FLAG = true;

        startGrid(GRIDS);

        //grid(0).context().

        Thread.sleep(5_000);
    }

    /**
     * Tests one node comes while one thread is used to call the service.
     */
    @Test
    public void testOneNodeComesOneThread() throws Exception {
        testClusterTopChanges(1, false);
    }

    /**
     * Tests several nodes come while one thread is used to call the service.
     */
    @Test
    public void testSeveralNodesComeOneThread() throws Exception {
        testClusterTopChanges(3, false);
    }

    /**
     * Tests one node comes while several threads are used to call the service.
     */
    @Test
    public void testOneNodeComesMultiThreads() throws Exception {
        testClusterTopChanges(1, true);
    }


    /**
     * Tests several nodes come while several threads are used to call the service.
     */
    @Test
    public void testSeveralNodesComeMultiThreads() throws Exception {
        testClusterTopChanges(3, true);
    }

    /**
     * Tests one node leaves while one thread is used to call the service.
     */
    @Test
    public void testOneNodeLeavesOneThread() throws Exception {
        testClusterTopChanges(-1, false);
    }

    /**
     * Tests several nodes leaves while one thread is used to call the service.
     */
    @Test
    public void testSeveralNodesLeaveOneThread() throws Exception {
        testClusterTopChanges(-3, false);
    }

    /**
     * Tests one node leaves while several threads are used to call the service.
     */
    @Test
    public void testOneNodeLeavesMultiThreads() throws Exception {
        testClusterTopChanges(-1, true);
    }

    /**
     * Tests several nodes leave while several threads are used to call the service.
     */
    @Test
    public void testSeveralNodesLeaveMultiThreads() throws Exception {
        testClusterTopChanges(-3, true);
    }

    /**
     * Tests service topology is updated when there is a gap of service invocation between forced service redeployment.
     */
    @Test
    public void testForcedRedeploy() throws InterruptedException {
        try (IgniteClient client = startClient(0)) {
            ServicesTest.TestServiceInterface svc = client.services().serviceProxy(SRV_NAME, ServicesTest.TestServiceInterface.class);

            AtomicReference<Set<UUID>> receivedSrvTop = new AtomicReference<>();

            addClientLogLsnr(receivedSrvTop::set);

            ((GridTestLog4jLogger)log).setLevel(Level.DEBUG);

            do {
                svc.testMethod();
            }
            while (receivedSrvTop.get() == null);

            ((GridTestLog4jLogger)log).setLevel(Level.INFO);

            assertTrue(receivedSrvTop.get().contains(grid(1).localNode().id())
                && receivedSrvTop.get().contains(grid(2).localNode().id()));

            grid(1).services().cancel(SRV_NAME);

            receivedSrvTop.set(null);

            Thread.sleep(3000);

            grid(1).services().deploy(serviceCfg().setNodeFilter(null));

            Thread.sleep(3000);

            ((GridTestLog4jLogger)log).setLevel(Level.DEBUG);

            do {
                svc.testMethod();
            }
            while (receivedSrvTop.get() == null);

            assertEquals(GRIDS, receivedSrvTop.get().size());

            for (Ignite ig : G.allGrids())
                assertTrue(receivedSrvTop.get().contains(ig.cluster().localNode().id()));
        }
    }

    /** */
    private void testClusterTopChanges(int newNodesCnt, boolean multiThreaded) throws Exception {
        assert newNodesCnt != 0;

        Set<UUID> nodesToStop = new HashSet<>();

        for (int i = 0; i < -newNodesCnt; ++i)
            nodesToStop.add(startGrid(GRIDS + i).localNode().id());

        // Service topology on the clients.
        AtomicReference<Collection<UUID>> receivedSrvTop = new AtomicReference<>();

        addClientLogLsnr(receivedSrvTop::set);

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
                        if (newNodesCnt > 0
                            || (!m.contains("Node has left grid") && !m.contains("Failed to send job due to node failure"))
                            || nodesToStop.stream().noneMatch(nid -> m.contains(nid.toString())))
                            throw e;
                    }

                    // Wait until the initial topology is received.
                    if (receivedSrvTop.get() != null && receivedSrvTop.get().size() == (newNodesCnt > 0 ? 2 : 2 + Math.abs(newNodesCnt))
                        && changeClusterTop.compareAndSet(false, true)) {

                        for (int i = 0; i < Math.abs(newNodesCnt); ++i) {
                            int nodeIdx = GRIDS + i;

                            runAsync(() -> {
                                try {
                                    if (newNodesCnt < 0)
                                        stopGrid(nodeIdx);
                                    else
                                        startGrid(nodeIdx);
                                }
                                catch (Exception e) {
                                    log.error("Unable to start or stop test grid.", e);

                                    stopFlag.set(true);
                                }
                            });
                        }
                    }

                    // Stoip if new excepted service topology received.
                    if (receivedSrvTop.get() != null && receivedSrvTop.get().size() == (newNodesCnt < 0 ? 2 : 2 + Math.abs(newNodesCnt)))
                        stopFlag.set(true);
                }
                while (!stopFlag.get());
            }, multiThreaded ? 4 : 1, "CallSrvLoader");
        }
        finally {
            ((GridTestLog4jLogger)log).setLevel(Level.INFO);
        }

        // The initial nodes must always persist it the service topology.
        assertTrue(receivedSrvTop.get().contains(grid(1).localNode().id())
            && receivedSrvTop.get().contains(grid(2).localNode().id()));

        for (int i = 0; i < newNodesCnt; ++i)
            assertTrue(receivedSrvTop.get().contains(grid(GRIDS + i).localNode().id()));
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

        assertTrue(top.get().size() == 2 && top.get().contains(grid(1).localNode().id())
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
        private final Set<Class<? extends DiscoveryCustomMessage>> blockMessages = new HashSet<>();

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, OutputStream out, TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            super.writeToSocket(sock, out, msg, timeout);
        }
    }
}
