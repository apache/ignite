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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.eclipse.jetty.util.ConcurrentHashSet;

/**
 * Tests checks case when one node is unable to connect to next in a ring,
 * but those nodes are not experiencing any connectivity troubles between
 * each other.
 */
public class IgniteDiscoveryMassiveNodeFailTest extends GridCommonAbstractTest {
    /** */
    private Set<InetSocketAddress> failedAddrs = new ConcurrentHashSet<>();

    /** */
    private volatile TcpDiscoveryNode compromisedNode;

    /** */
    private volatile boolean forceFail = true;

    /** */
    private long timeout;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        FailDiscoverySpi disco = new FailDiscoverySpi();

        disco.setIpFinder(LOCAL_IP_FINDER);

        cfg.setDiscoverySpi(disco);

        disco.setConnectionRecoveryTimeout(timeout);

        return cfg;
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
    }

    /**
     * Node fails 2 nodes when connection check is disabled.
     *
     * @throws Exception If failed.
     */
    public void testMassiveFail() throws Exception {
        timeout = 0; // Disable previous node check.

        startGrids(5);

        grid(0).events().enabledEvents();

        Set<ClusterNode> failed = new HashSet<>(Arrays.asList(grid(3).cluster().localNode(), grid(4).cluster().localNode()));

        CountDownLatch latch = new CountDownLatch(failed.size());

        grid(0).events().localListen((e) -> {
            DiscoveryEvent evt = (DiscoveryEvent)e;

            if (failed.contains(evt.eventNode()))
                latch.countDown();

            return true;
        }, EventType.EVT_NODE_FAILED);

        compromisedNode = (TcpDiscoveryNode)grid(2).localNode();

        for (int i = 3; i < 5; i++)
            failedAddrs.addAll(((TcpDiscoveryNode)grid(i).localNode()).socketAddresses());

        System.out.println(">> Start failing nodes");

        forceFail = true;

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

        forceFail = true;

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

        forceFail = true;

        doSleep(timeout / 4); // wait 1 try

        forceFail = false;

        System.out.println(">> Stop failing nodes");

        assert !latch.await(waitTime(), TimeUnit.MILLISECONDS);

        // Topology is not changed
        assertEquals(5, grid(0).cluster().forServers().nodes().size());
        assertEquals(5, grid(0).cluster().topologyVersion());
    }

    /**
     *
     */
    private class FailDiscoverySpi extends TcpDiscoverySpi {
        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg, byte[] data,
            long timeout) throws IOException {
            assertNotFailedNode(sock);

            super.writeToSocket(sock, msg, data, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            assertNotFailedNode(sock);

            super.writeToSocket(sock, msg, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(ClusterNode node, Socket sock, OutputStream out,
            TcpDiscoveryAbstractMessage msg, long timeout) throws IOException, IgniteCheckedException {
            assertNotFailedNode(sock);

            super.writeToSocket(node, sock, out, msg, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, OutputStream out, TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            assertNotFailedNode(sock);

            super.writeToSocket(sock, out, msg, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(TcpDiscoveryAbstractMessage msg, Socket sock, int res,
            long timeout) throws IOException {
            assertNotFailedNode(sock);

            super.writeToSocket(msg, sock, res, timeout);
        }

        /**
         * @param sock Socket.
         * @throws IOException To break connection.
         */
        @SuppressWarnings("SuspiciousMethodCalls")
        private void assertNotFailedNode(Socket sock) throws IOException {
            if (forceFail && getLocalNode().equals(compromisedNode) && failedAddrs.contains(sock.getRemoteSocketAddress())) {
                log.info(">> Force fail connection " + sock.getRemoteSocketAddress());

                throw new IOException("Force fail connection " + sock.getRemoteSocketAddress());
            }
        }
    }
}
