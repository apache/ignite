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

package org.apache.ignite.internal;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests checks case when one node is unable to connect to next in a ring,
 * but those nodes are not experiencing any connectivity troubles between
 * each other.
 */
public class IgniteDiscoveryMassiveNodeFailTest extends GridCommonAbstractTest {
    /** */
    private static final int FAILURE_DETECTION_TIMEOUT = 5_000;

    /** */
    private Set<InetSocketAddress> failedAddrs = new GridConcurrentHashSet<>();

    /** */
    private volatile TcpDiscoveryNode compromisedNode;

    /** */
    private volatile boolean forceFailConnectivity;

    /** */
    private volatile boolean failNodes;

    /** */
    private long timeout;

    /** */
    private volatile Set<ClusterNode> failedNodes = Collections.emptySet();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        FailDiscoverySpi disco = new FailDiscoverySpi();

        disco.setIpFinder(LOCAL_IP_FINDER);

        cfg.setDiscoverySpi(disco);

        disco.setConnectionRecoveryTimeout(timeout);

        cfg.setFailureDetectionTimeout(FAILURE_DETECTION_TIMEOUT);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        System.setProperty(IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE, "false");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        System.setProperty(IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE, "true");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        timeout = 2_000;
        failNodes = false;
        forceFailConnectivity = false;
    }

    /**
     * Node fails 2 nodes when connection check is disabled.
     *
     * @throws Exception If failed.
     */
    public void testMassiveFailDisabledRecovery() throws Exception {
        timeout = 0; // Disable previous node check.

        doFailNodes(false);
    }

    /**
     *
     */
    private void doFailNodes(boolean simulateNodeFailure) throws Exception {
        startGrids(5);

        grid(0).events().enabledEvents();

        failedNodes = new HashSet<>(Arrays.asList(grid(3).cluster().localNode(), grid(4).cluster().localNode()));

        CountDownLatch latch = new CountDownLatch(failedNodes.size());

        grid(0).events().localListen(e -> {
            DiscoveryEvent evt = (DiscoveryEvent)e;

            if (failedNodes.contains(evt.eventNode()))
                latch.countDown();

            return true;
        }, EventType.EVT_NODE_FAILED);

        compromisedNode = (TcpDiscoveryNode)grid(2).localNode();

        for (int i = 3; i < 5; i++)
            failedAddrs.addAll(((TcpDiscoveryNode)grid(i).localNode()).socketAddresses());

        System.out.println(">> Start failing nodes");

        forceFailConnectivity = true;

        if (simulateNodeFailure) {
            for (int i = 3; i < 5; i++)
                ((TcpDiscoverySpi)grid(i).configuration().getDiscoverySpi()).simulateNodeFailure();
        }

        assert latch.await(waitTime(), TimeUnit.MILLISECONDS);

        assertEquals(3, grid(0).cluster().forServers().nodes().size());
    }

    /**
     *
     */
    private long waitTime() {
        return timeout + 5000;
    }

    /**
     * Node fail itself.
     *
     * @throws Exception If failed.
     */
    public void testMassiveFailSelfKill() throws Exception {
        startGrids(5);

        grid(0).events().enabledEvents();

        CountDownLatch latch = new CountDownLatch(1);

        grid(0).events().localListen((e) -> {
            DiscoveryEvent evt = (DiscoveryEvent)e;

            if (evt.eventNode().equals(compromisedNode))
                latch.countDown();

            return true;
        }, EventType.EVT_NODE_FAILED);

        compromisedNode = (TcpDiscoveryNode)grid(2).localNode();

        for (int i = 3; i < 5; i++)
            failedAddrs.addAll(((TcpDiscoveryNode)grid(i).localNode()).socketAddresses());

        System.out.println(">> Start failing nodes");

        forceFailConnectivity = true;

        assert latch.await(waitTime(), TimeUnit.MILLISECONDS);

        assertEquals(4, grid(0).cluster().forServers().nodes().size());
    }

    /**
     * When connectivity restored, no topology changes will be applied.
     *
     * @throws Exception If failed.
     */
    public void testMassiveFailAndRecovery() throws Exception {
        startGrids(5);

        grid(0).events().enabledEvents();

        CountDownLatch latch = new CountDownLatch(1);

        grid(0).events().localListen(e -> {
            DiscoveryEvent evt = (DiscoveryEvent)e;

            if (evt.eventNode().equals(compromisedNode))
                latch.countDown();

            return true;
        }, EventType.EVT_NODE_FAILED);

        compromisedNode = (TcpDiscoveryNode)grid(2).localNode();

        for (int i = 3; i < 5; i++)
            failedAddrs.addAll(((TcpDiscoveryNode)grid(i).localNode()).socketAddresses());

        System.out.println(">> Start failing nodes");

        forceFailConnectivity = true;

        doSleep(timeout / 4); // wait 1 try

        forceFailConnectivity = false;

        System.out.println(">> Stop failing nodes");

        assert !latch.await(waitTime(), TimeUnit.MILLISECONDS);

        // Topology is not changed
        assertEquals(5, grid(0).cluster().forServers().nodes().size());
        assertEquals(5, grid(0).cluster().topologyVersion());
    }

    /**
     * Regular nodes fail by timeout.
     *
     * @throws Exception If failed.
     */
    public void testMassiveFail() throws Exception {
        failNodes = true;

        // Must be greater than failureDetectionTimeout / 3 as it calculated into
        // connection check frequency.
        timeout = FAILURE_DETECTION_TIMEOUT;

        doFailNodes(false);
    }

    /**
     * Regular node fail by crash. Should be faster due to
     *
     *
     * @throws Exception If failed.
     */
    public void testMassiveFailForceNodeFail() throws Exception {
        failNodes = true;

        // Must be greater than failureDetectionTimeout / 3 as it calculated into
        // connection check frequency.
        timeout = FAILURE_DETECTION_TIMEOUT / 2;

        doFailNodes(true);
    }

    /**
     * Check that cluster recovers from temporal connection breakage.
     *
     * @throws Exception If failed.
     */
    public void testRecoveryOnDisconnect() throws Exception {
        startGrids(3);

        IgniteEx ignite1 = grid(1);
        IgniteEx ignite2 = grid(2);

        ((TcpDiscoverySpi)ignite1.configuration().getDiscoverySpi()).brakeConnection();
        ((TcpDiscoverySpi)ignite2.configuration().getDiscoverySpi()).brakeConnection();

        doSleep(FAILURE_DETECTION_TIMEOUT);

        assertEquals(3, grid(0).cluster().nodes().size());
        assertEquals(3, grid(1).cluster().nodes().size());
        assertEquals(3, grid(2).cluster().nodes().size());
    }

    /**
     *
     */
    private class FailDiscoverySpi extends TcpDiscoverySpi {
        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg, byte[] data,
            long timeout) throws IOException {
            assertNotFailedNode(sock);

            if (isDrop(msg))
                return;

            super.writeToSocket(sock, msg, data, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            assertNotFailedNode(sock);

            if (isDrop(msg))
                return;

            super.writeToSocket(sock, msg, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(ClusterNode node, Socket sock, OutputStream out,
            TcpDiscoveryAbstractMessage msg, long timeout) throws IOException, IgniteCheckedException {
            assertNotFailedNode(sock);

            if (isDrop(msg))
                return;

            super.writeToSocket(node, sock, out, msg, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, OutputStream out, TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            assertNotFailedNode(sock);

            if (isDrop(msg))
                return;

            super.writeToSocket(sock, out, msg, timeout);
        }

        /**
         *
         */
        private boolean isDrop(TcpDiscoveryAbstractMessage msg) {
            boolean drop = failNodes && forceFailConnectivity && failedNodes.contains(ignite.cluster().localNode());

            if (drop)
                ignite.log().info(">> Drop message " + msg);

            return drop;
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(TcpDiscoveryAbstractMessage msg, Socket sock, int res,
            long timeout) throws IOException {
            assertNotFailedNode(sock);

            if (isDrop(msg))
                return;

            super.writeToSocket(msg, sock, res, timeout);
        }

        /**
         * @param sock Socket.
         * @throws IOException To break connection.
         */
        @SuppressWarnings("SuspiciousMethodCalls")
        private void assertNotFailedNode(Socket sock) throws IOException {
            if (forceFailConnectivity && getLocalNode().equals(compromisedNode) && failedAddrs.contains(sock.getRemoteSocketAddress())) {
                log.info(">> Force fail connection " + sock.getRemoteSocketAddress());

                throw new IOException("Force fail connection " + sock.getRemoteSocketAddress());
            }
        }
    }
}
